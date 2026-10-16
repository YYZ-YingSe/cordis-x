import { Refusal } from '../../core/error.ts';
import { RpcError } from './context.ts';
import type { Context, DependencyChannel } from './context.ts';
import { validateControl } from '../../protocols/session/binary.ts';
import type { Value } from '../../core/value.ts';

import type { ScopeCallback } from '../../core/handles.ts';
export type { ScopeCallback } from '../../core/handles.ts';
/** A child owner, not a saved Context. Every run creates fresh callback authority. */
export class Scope {
  private channel: DependencyChannel;
  private registry: ScopeRegistry;
  private scope: string;
  private grant: string;
  constructor(channel: DependencyChannel, registry: ScopeRegistry, scope: string, grant: string) {
    this.channel = channel;
    this.registry = registry;
    this.scope = scope;
    this.grant = grant;
  }
  private request(command: string) {
    return this.channel.request(command, { scope: this.scope, grant: this.grant });
  }
  async ready(): Promise<void> {
    await this.request('scope_ready');
  }
  async dispose(): Promise<void> {
    await this.request('scope_dispose');
  }
  snapshot(): Promise<Value> {
    return this.request('scope_status');
  }
  run(callback: ScopeCallback): Promise<Value> {
    return this.registry.run(this.channel, this.scope, this.grant, callback);
  }
}

type Run = { scope: string; callback?: ScopeCallback; started: boolean; running: boolean };
export class ScopeRegistry {
  private nextScope = 0;
  private nextRun = 0;
  private runs = new Map<string, Run>();
  async register(channel: DependencyChannel, context: string, label: string): Promise<Scope> {
    if (Buffer.byteLength(label) > 1024)
      throw new Refusal('invalid_argument', 'scope label too long');
    if (this.nextScope >= 256) throw new Refusal('scopes_exhausted', 'scope history full');
    const scope = `c${++this.nextScope}`;
    const reply = await channel.request('scope_register', { context, scope, label });
    if (
      !reply ||
      typeof reply !== 'object' ||
      Array.isArray(reply) ||
      reply.scope !== scope ||
      typeof reply.grant !== 'string' ||
      !/^[0-9a-f]{32}$/.test(reply.grant)
    )
      throw new Refusal('protocol', 'invalid scope registration');
    return new Scope(channel, this, scope, reply.grant);
  }
  async run(
    channel: DependencyChannel,
    scope: string,
    grant: string,
    callback: ScopeCallback,
  ): Promise<Value> {
    if (this.nextRun >= 4096) throw new Refusal('runs_exhausted', 'scope run history full');
    const run = `u${++this.nextRun}`;
    const entry: Run = { scope, callback, started: false, running: false };
    this.runs.set(run, entry);
    try {
      return await channel.request('scope_run', { scope, grant, run });
    } finally {
      entry.callback = undefined;
    }
  }
  async invoke(scope: string, run: string, ctx: Context): Promise<Value> {
    const entry = this.runs.get(run);
    if (!entry || entry.scope !== scope || entry.started || !entry.callback)
      throw new Refusal('closed', 'scope callback unavailable');
    entry.started = true;
    entry.running = true;
    const callback = entry.callback;
    try {
      try {
        const value = (await callback(ctx)) ?? null;
        validateControl(value);
        return { kind: 'success', value };
      } catch (error) {
        return {
          kind: error instanceof Refusal ? 'refused' : 'failure',
          error: {
            code: error instanceof RpcError ? error.code : 'scope_failed',
            message: error instanceof Error ? error.message : String(error),
          },
        };
      }
    } finally {
      await ctx.finish();
      entry.callback = undefined;
      entry.running = false;
    }
  }
  get idle(): boolean {
    return [...this.runs.values()].every((r) => !r.running);
  }
}
