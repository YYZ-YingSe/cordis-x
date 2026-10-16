import type { Message } from '../../core/message.ts';
import { payload, controlOnly, type Payload } from '../../protocols/session/messages.ts';
import type { EventFlow } from '../../core/events.ts';
import { Refusal } from '../../core/error.ts';
import { RpcError } from './context.ts';
import type { Context, DependencyChannel } from './context.ts';
import type { Value } from '../../core/value.ts';

import { Continue, EventDispatchError } from '../../core/events.ts';
import type { EventCallback } from '../../core/events.ts';
export { Continue, Break, EventDispatchError } from '../../core/events.ts';
export type { EventCallback, EventFlow } from '../../core/events.ts';
export class Subscription {
  private channel: DependencyChannel;
  private listener: string;
  private grant: string;
  constructor(channel: DependencyChannel, listener: string, grant: string) {
    this.channel = channel;
    this.listener = listener;
    this.grant = grant;
  }
  async dispose(): Promise<void> {
    await this.channel.request('event_dispose', { listener: this.listener, grant: this.grant });
  }
}
export class Dispatch {
  private channel: DependencyChannel;
  private dispatch: string;
  private grant: string;
  constructor(channel: DependencyChannel, dispatch: string, grant: string) {
    this.channel = channel;
    this.dispatch = dispatch;
    this.grant = grant;
  }
  async join(): Promise<EventFlow[]> {
    const reply = await this.channel.requestPayload('event_join', payload({ dispatch: this.dispatch, grant: this.grant }));
    const result = reply.control;
    if (!result || typeof result !== 'object' || Array.isArray(result))
      throw new Refusal('protocol', 'invalid dispatch outcome');
    if (result.kind === 'success' && Array.isArray(result.flows)) {
      let index = 0;
      const flows = result.flows.map((tag): EventFlow => {
        if (tag === 'continue') return { kind: 'continue' };
        if (tag !== 'break' || index >= reply.messages.length) throw new Error('invalid event body');
        return { kind: 'break', value: reply.messages[index++] };
      });
      if (index !== reply.messages.length) throw new Error('unexpected event body');
      return flows;
    }
    if ((result.kind === 'rejected' || result.kind === 'refused') && result.error && typeof result.error === 'object' && !Array.isArray(result.error) && typeof result.error.code === 'string' && typeof result.error.message === 'string')
      throw result.kind === 'refused' ? new Refusal(result.error.code, result.error.message) : new RpcError(result.error.code, result.error.message);
    if (result.kind === 'unknown') throw new Error(`event outcome unknown: ${JSON.stringify(result.error)}`);
    if (result.kind === 'failure' && Array.isArray(result.errors))
      throw new EventDispatchError(result.errors);
    throw new Refusal('protocol', 'invalid dispatch outcome');
  }
  snapshot(): Promise<Value> {
    return this.channel.request('event_status', { dispatch: this.dispatch, grant: this.grant });
  }
}
type Entry = { callback?: EventCallback; running: number };
export class EventRegistry {
  private entries = new Map<string, Entry>();
  private seen = new Set<string>();
  private nextListener = 0;
  private nextDispatch = 0;
  private declarations: Record<string, { name: string; revision: string }>;
  constructor(declarations: Record<string, { name: string; revision: string }>) {
    this.declarations = declarations;
  }
  private check(event: string) {
    if (!Object.hasOwn(this.declarations, event)) throw new Refusal('undeclared_event', event);
  }
  private grant(reply: Value, key: string, id: string): string {
    if (
      !reply ||
      typeof reply !== 'object' ||
      Array.isArray(reply) ||
      reply[key] !== id ||
      typeof reply.grant !== 'string' ||
      !/^[0-9a-f]{32}$/.test(reply.grant)
    )
      throw new Refusal('protocol', 'invalid event acknowledgement');
    return reply.grant;
  }
  async register(
    channel: DependencyChannel,
    context: string,
    event: string,
    callback: EventCallback,
  ): Promise<Subscription> {
    this.check(event);
    if (this.nextListener >= 4096) throw new Refusal('events_exhausted', 'listener history full');
    const listener = `l${++this.nextListener}`;
    this.entries.set(listener, { callback, running: 0 });
    const reply = await channel.request('event_register', { context, event, listener });
    return new Subscription(channel, listener, this.grant(reply, 'listener', listener));
  }
  async emit(
    channel: DependencyChannel,
    context: string,
    event: string,
    args: Message,
    mode: 'parallel' | 'serial',
  ): Promise<Dispatch> {
    this.check(event);
    if (this.nextDispatch >= 4096) throw new Refusal('events_exhausted', 'dispatch history full');
    const dispatch = `d${++this.nextDispatch}`;
    const reply = controlOnly(await channel.requestPayload('event_emit', payload({ context, event, mode, dispatch }, args)));
    return new Dispatch(channel, dispatch, this.grant(reply, 'dispatch', dispatch));
  }
  async invoke(listener: string, call: string, args: Message, ctx: Context): Promise<Payload> {
    const entry = this.entries.get(listener);
    if (!entry?.callback || !/^v[0-9]{1,20}$/.test(call) || this.seen.has(call))
      throw new Refusal('closed', 'event callback unavailable');
    if (this.seen.size >= 4096)
      throw new Refusal('session_exhausted', 'event callback history full');
    this.seen.add(call);
    entry.running++;
    try {
      const flow = (await entry.callback(args, ctx)) ?? Continue();
      if (!flow || !['continue', 'break'].includes(flow.kind) || Object.keys(flow).length !== (flow.kind === 'continue' ? 1 : 2)) throw new Error('invalid event flow');
      if (flow.kind === 'continue') return payload({ kind: 'continue' });
      if (!(flow.value instanceof Uint8Array)) throw new Error('invalid break body');
      return payload({ kind: 'break' }, flow.value);
    } finally {
      await ctx.finish();
      entry.running--;
    }
  }
  release(listener: string) {
    const entry = this.entries.get(listener);
    if (!entry) throw new Refusal('closed', 'unknown listener');
    if (entry.running) throw new Refusal('listeners_running', 'listener has active callbacks');
    entry.callback = undefined;
  }
  get released(): boolean {
    return [...this.entries.values()].every((e) => !e.callback && e.running === 0);
  }
}
