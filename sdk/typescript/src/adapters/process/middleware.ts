import { Identities, makeRoom, Completion, registration } from './retention.ts';
import { OperationInbox } from '../../protocols/session/operations.ts';
import type { Message } from '../../core/message.ts';
import { payload, one, type Payload } from '../../protocols/session/messages.ts';
import { Refusal } from '../../core/error.ts';
import { RpcError } from './context.ts';
import type { Context, DependencyChannel } from './context.ts';
import type { Value } from '../../core/value.ts';

import type { MiddlewareCallback, TerminalCallback } from '../../core/handles.ts';
export type { MiddlewareCallback, TerminalCallback } from '../../core/handles.ts';
/** Single-use capability bound to one middleware invocation. */
export class Next {
  private open = true;
  private used = false;
  private pending?: Promise<Message>;
  private channel: DependencyChannel;
  private context: string;
  private call: string;
  private grant: string;
  constructor(channel: DependencyChannel, context: string, call: string, grant: string) {
    this.channel = channel;
    this.context = context;
    this.call = call;
    this.grant = grant;
  }
  run(args: Message): Promise<Message> {
    if (!this.open) return Promise.reject(new Refusal('expired_next', 'middleware frame ended'));
    if (this.used) return Promise.reject(new Refusal('duplicate_next', 'next already admitted'));
    this.used = true;
    this.pending = this.channel.requestPayload('middleware_next', payload({ context: this.context, call: this.call, grant: this.grant }, args)).then(one);
    // A caller may intentionally detach its wait; finish still drains it.
    this.pending.catch(() => {});
    return this.pending;
  }
  async finish() {
    this.open = false;
    if (this.pending) await Promise.allSettled([this.pending]);
  }
}
export class MiddlewareRegistration {
  private channel: DependencyChannel;
  private handler: string;
  private grant: string;
  constructor(channel: DependencyChannel, handler: string, grant: string) {
    this.channel = channel;
    this.handler = handler;
    this.grant = grant;
  }
  private disposeCompletion = new Completion<void>();
  dispose(): Promise<void> {
    return this.disposeCompletion.observe(async () => { await this.channel.request('middleware_dispose', { handler: this.handler, grant: this.grant }); });
  }
}
type Entry = { callback?: MiddlewareCallback; running: number };
type Terminal = { callback: TerminalCallback; used: boolean };
export class MiddlewareRegistry {
  private handlers = new Map<string, Entry>();
  private runs = new Map<string, Terminal>();
  private seen = new OperationInbox();
  private nextHandler = new Identities();
  private nextRun = new Identities();
  private declarations: Record<string, { name: string; revision: string }>;
  constructor(declarations: Record<string, { name: string; revision: string }>) {
    this.declarations = declarations;
  }
  private check(alias: string) {
    if (!Object.hasOwn(this.declarations, alias))
      throw new Refusal('undeclared_middleware', alias);
  }
  async register(
    channel: DependencyChannel,
    context: string,
    middleware: string,
    callback: MiddlewareCallback,
  ): Promise<MiddlewareRegistration> {
    this.check(middleware);
    makeRoom(this.handlers, 4096, (e) => !e.callback && !e.running);
    const handler = this.nextHandler.next('m');
    this.handlers.set(handler, { callback, running: 0 });
    const reply = await registration(channel.request('middleware_register', { context, middleware, handler }), () => this.handlers.delete(handler));
    if (
      !reply ||
      typeof reply !== 'object' ||
      Array.isArray(reply) ||
      reply.handler !== handler ||
      typeof reply.grant !== 'string' ||
      !/^[0-9a-f]{32}$/.test(reply.grant)
    )
      throw new Refusal('protocol', 'invalid middleware acknowledgement');
    return new MiddlewareRegistration(channel, handler, reply.grant);
  }
  async run(
    channel: DependencyChannel,
    context: string,
    middleware: string,
    args: Message,
    callback: TerminalCallback,
  ): Promise<Message> {
    this.check(middleware);
    makeRoom(this.runs, 4096, () => false);
    const run = this.nextRun.next('q');
    this.runs.set(run, { callback, used: false });
    try {
      const result=one(await channel.requestPayload('middleware_run', payload({ context, middleware, run }, args)));
      this.runs.delete(run);
      return result;
    } catch(error) {
      if(error instanceof RpcError) this.runs.delete(run);
      throw error;
    }
  }
  private admit(call: string): () => void {
    if (!call.startsWith('w')) throw new Refusal('closed', 'middleware callback unavailable');
    return this.seen.admit(call.slice(1));
  }

  private async outcome(callback: () => Message | Promise<Message>): Promise<Payload> {
    return payload(null, await callback());
  }
  async invoke(
    handler: string,
    call: string,
    args: Message,
    ctx: Context,
    next: Next,
  ): Promise<Payload> {
    const entry = this.handlers.get(handler);
    if (!entry?.callback) throw new Refusal('closed', 'middleware handler unavailable');
    const finish = this.admit(call);
    entry.running++;
    try {
      return await this.outcome(() => entry.callback!(args, ctx, next));
    } finally {
      // Close both admission windows before waiting for either to drain.
      const following = next.finish();
      await ctx.finish();
      await following;
      entry.running--;
      finish();
    }
  }
  async terminal(run: string, call: string, args: Message, ctx: Context): Promise<Payload> {
    const entry = this.runs.get(run);
    if (!entry || entry.used) throw new Refusal('closed', 'terminal unavailable');
    const finish = this.admit(call);
    entry.used = true;
    try {
      return await this.outcome(() => entry.callback(args, ctx));
    } finally {
      await ctx.finish();
      finish();
    }
  }
  release(handler: string) {
    const entry = this.handlers.get(handler);
    if (!entry) throw new Refusal('closed', 'unknown middleware handler');
    if (entry.running) throw new Refusal('middleware_running', 'middleware has active callbacks');
    entry.callback = undefined;
  }
  get released(): boolean {
    return (
      this.runs.size === 0 && [...this.handlers.values()].every((e) => !e.callback && !e.running)
    );
  }
}
