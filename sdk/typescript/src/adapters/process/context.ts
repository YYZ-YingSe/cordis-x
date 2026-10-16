import type { Message } from '../../core/message.ts';
import type { EventFlow } from '../../core/events.ts';
import { payload as data, one, controlOnly } from '../../protocols/session/messages.ts';
import { operationOutcome } from '../../protocols/session/outcome.ts';
import { Refusal } from '../../core/error.ts';
import type {
  MiddlewareRegistry,
  MiddlewareCallback,
  MiddlewareRegistration,
  TerminalCallback,
} from './middleware.ts';
import type { EventRegistry, EventCallback, Subscription, Dispatch } from './events.ts';
import type { Task, TaskCallback, TaskRegistry, TaskCancellation } from './task.ts';
import type { EffectProducer, EffectSequence, SequenceRegistry } from './sequence.ts';
import type { Scope, ScopeRegistry } from './scope.ts';
import type { Value } from '../../core/value.ts';
import type { ResourceHandle } from '../../core/handles.ts';
import { acquireResource, bindResourceContext } from './dependency-resources.ts';
import { Child } from './composition.ts';
import type { Composition } from './composition.ts';

import { RpcError } from '../../core/error.ts';
export { RpcError } from '../../core/error.ts';
import { DependencyChannel } from '../../protocols/session/channel.ts';
export { DependencyChannel } from '../../protocols/session/channel.ts';
export type { Identity, Envelope } from '../../protocols/session/channel.ts';

/** Valid during one activate/invoke/acquire/close/deactivate callback. */
export class Context {
  get validation() { return this.channel.validation; }
  private open = true;
  private pending = new Set<Promise<unknown>>();
  private channel: DependencyChannel;
  private token: string;
  private dependencies: Record<string, string>;
  private effects?: EffectRegistry;
  private cleanup: boolean;
  private sequences?: SequenceRegistry;
  private scopes?: ScopeRegistry;
  private tasks?: TaskRegistry;
  private cancellation?: TaskCancellation;
  private events?: EventRegistry;
  private middlewares?: MiddlewareRegistry;
  private composition?: Composition;
  constructor(
    channel: DependencyChannel,
    token: string,
    dependencies: Record<string, string>,
    effects?: EffectRegistry,
    cleanup = false,
    sequences?: SequenceRegistry,
    scopes?: ScopeRegistry,
    tasks?: TaskRegistry,
    cancellation?: TaskCancellation,
    events?: EventRegistry,
    middlewares?: MiddlewareRegistry,
    composition?: Composition,
  ) {
    this.channel = channel;
    this.token = token;
    this.dependencies = dependencies;
    this.effects = effects;
    this.cleanup = cleanup;
    this.sequences = sequences;
    this.scopes = scopes;
    this.tasks = tasks;
    this.cancellation = cancellation;
    this.events = events;
    this.middlewares = middlewares;
    this.composition = composition;
    bindResourceContext(this, channel, (dispose, resource, grant, method, args) =>
      this.resourceRequest(dispose, resource, grant, method, args));
  }
  acquire(dependency: string, kind: string, args: Message): Promise<ResourceHandle> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'acquire requires a live ordinary Context'));
    if (!Object.hasOwn(this.dependencies, dependency))
      return Promise.reject(new Refusal('undeclared_dependency', dependency));
    return this.track(acquireResource(this.channel, this.token, dependency, kind, args));
  }
  private resourceRequest(
    dispose: boolean, resource: string, grant: string, method: string, args: Message | undefined,
  ): Promise<Message | undefined> {
    if (!this.open || !this.token || (dispose && this.cleanup))
      return Promise.reject(new Refusal('closed', 'resource operation requires a live Context'));
    const payload: Record<string, Value> = { context: this.token, resource, grant };
    if (!dispose) Object.assign(payload, { method });
    const command = dispose ? 'dependency_resource_dispose'
      : this.cleanup ? 'cleanup_resource_call' : 'dependency_resource_call';
    return this.track(this.channel.requestPayload(command, data(payload, args)).then((reply) => {
      const value = operationOutcome(reply.control);
      if (dispose) { if (value !== null || reply.messages.length) throw new Error('invalid resource disposal acknowledgement'); return; }
      return one(reply);
    }));
  }
  /** Configuration revision and layers fixed for the current callback. */
  metadata(name: string, revision: string): Promise<{ revision: string | null; layers: Value[] }> {
    if (!this.open || !this.token) return Promise.reject(new Refusal('closed', 'context expired'));
    return this.track(
      this.channel.metadata(this.token, name, revision, this.cleanup).then((value) => {
        if (
          !value ||
          typeof value !== 'object' ||
          Array.isArray(value) ||
          !(
            value.revision === null ||
            (typeof value.revision === 'string' && /^(0|[1-9][0-9]*)$/.test(value.revision))
          ) ||
          !Array.isArray(value.layers)
        )
          throw new Refusal('protocol', 'invalid metadata snapshot');
        return { revision: value.revision, layers: value.layers };
      }),
    );
  }
  install(factory: string, config: Message): Promise<Child> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'context cannot install'));
    if (!this.composition)
      return Promise.reject(new Refusal('unsupported_composition', 'composition not negotiated'));
    if (!this.composition.factories.includes(factory))
      return Promise.reject(new Refusal('undeclared_factory', factory));
    return this.track(
      this.channel
        .requestPayload('child_install', data({ context: this.token, factory }, config))
        .then((reply) => new Child(this.channel, operationOutcome(controlOnly(reply)))),
    );
  }
  provide(service: string, value: Message): Promise<void> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'context cannot provide'));
    if (!this.composition)
      return Promise.reject(new Refusal('unsupported_composition', 'composition not negotiated'));
    if (!Object.hasOwn(this.composition.provisions, service))
      return Promise.reject(new Refusal('undeclared_provision', service));
    return this.track(
      this.channel.requestPayload('value_provide', data({ context: this.token, service }, value)).then(() => {}),
    );
  }
  invoke(dependency: string, method: string, args: Message): Promise<Message> {
    if (!this.open || !this.token)
      return Promise.reject(new Refusal('closed', 'invocation context expired'));
    if (!Object.hasOwn(this.dependencies, dependency))
      return Promise.reject(new Refusal('undeclared_dependency', dependency));
    return this.track(this.channel.invoke(this.token, dependency, method, args, this.cleanup));
  }
  /** The returned handle outlives this callback; ownership stays with its caller. */
  defer(label: string, cleanup: (ctx: Context) => void | Promise<void>): Promise<Effect> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'context cannot register effects'));
    if (!this.effects)
      return Promise.reject(
        new Refusal('unsupported_effects', 'effects capability not negotiated'),
      );
    return this.track(this.effects.register(this.channel, this.token, label, cleanup));
  }
  effect(label: string, producer: EffectProducer): Promise<EffectSequence> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'context cannot start sequences'));
    if (!this.sequences)
      return Promise.reject(
        new Refusal('unsupported_sequences', 'sequence capability not negotiated'),
      );
    return this.track(this.sequences.register(this.channel, this.token, label, producer));
  }
  scope(label: string): Promise<Scope> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'context cannot create scopes'));
    if (!this.scopes)
      return Promise.reject(new Refusal('unsupported_scopes', 'scope capability not negotiated'));
    return this.track(this.scopes.register(this.channel, this.token, label));
  }
  spawn(label: string, callback: TaskCallback): Promise<Task> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'context cannot spawn tasks'));
    if (!this.tasks)
      return Promise.reject(new Refusal('unsupported_tasks', 'task capability not negotiated'));
    return this.track(this.tasks.register(this.channel, this.token, label, callback));
  }
  /** Resolves when this managed task's owner or implementation starts closing. */
  cancelled(): Promise<void> {
    if (!this.open || !this.cancellation)
      return Promise.reject(new Refusal('closed', 'cancellation requires a live task context'));
    return this.cancellation.promise;
  }
  middleware(alias: string, callback: MiddlewareCallback): Promise<MiddlewareRegistration> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'context cannot register middleware'));
    if (!this.middlewares)
      return Promise.reject(
        new Refusal('unsupported_middleware', 'middleware capability not negotiated'),
      );
    return this.track(this.middlewares.register(this.channel, this.token, alias, callback));
  }
  waterfall(
    alias: string,
    args: Message,
    terminal: TerminalCallback = (value) => value,
  ): Promise<Message> {
    if (!this.open || !this.token)
      return Promise.reject(new Refusal('closed', 'invocation context expired'));
    if (!this.middlewares)
      return Promise.reject(
        new Refusal('unsupported_middleware', 'middleware capability not negotiated'),
      );
    return this.track(this.middlewares.run(this.channel, this.token, alias, args, terminal));
  }
  on(event: string, callback: EventCallback): Promise<Subscription> {
    if (!this.open || !this.token || this.cleanup)
      return Promise.reject(new Refusal('closed', 'context cannot subscribe'));
    if (!this.events)
      return Promise.reject(new Refusal('unsupported_events', 'events capability not negotiated'));
    return this.track(this.events.register(this.channel, this.token, event, callback));
  }
  emit(event: string, args: Message): Promise<Dispatch> {
    return this.dispatch(event, args, 'parallel');
  }
  parallel(event: string, args: Message): Promise<EventFlow[]> {
    return this.track(this.emit(event, args).then((d) => d.join()));
  }
  serial(event: string, args: Message): Promise<EventFlow> {
    return this.track(this.dispatch(event, args, 'serial').then(async d => { const flows = await d.join(); if (flows.length !== 1) throw new Error('invalid serial flow count'); return flows[0]; }));
  }
  private dispatch(event: string, args: Message, mode: 'parallel' | 'serial'): Promise<Dispatch> {
    if (!this.open || !this.token)
      return Promise.reject(new Refusal('closed', 'invocation context expired'));
    if (!this.events)
      return Promise.reject(new Refusal('unsupported_events', 'events capability not negotiated'));
    return this.track(this.events.emit(this.channel, this.token, event, args, mode));
  }
  private track<T>(task: Promise<T>): Promise<T> {
    this.pending.add(task);
    task.then(
      () => this.pending.delete(task),
      () => this.pending.delete(task),
    );
    return task;
  }
  async finish() {
    this.open = false;
    await Promise.allSettled(this.pending);
  }
}

/** An independent, session-bound disposal capability. Dropping it does not dispose. */
export class Effect {
  private channel: DependencyChannel;
  private effect: string;
  private grant: string;
  constructor(channel: DependencyChannel, effect: string, grant: string) {
    this.channel = channel;
    this.effect = effect;
    this.grant = grant;
  }
  async dispose(): Promise<void> {
    await this.channel.request('effect_dispose', { effect: this.effect, grant: this.grant });
  }
}

type Cleanup = (ctx: Context) => void | Promise<void>;
type EffectRecord = { cleanup?: Cleanup; result?: Promise<void>; released: boolean };

/** Host-side callback storage, separate from the Runtime's ownership decisions. */
export class EffectRegistry {
  private entries = new Map<string, EffectRecord>();
  private next = 0;
  async register(
    channel: DependencyChannel,
    context: string,
    label: string,
    cleanup: Cleanup,
  ): Promise<Effect> {
    if (Buffer.byteLength(label, 'utf8') > 1024)
      throw new Refusal('invalid_argument', 'effect label too long');
    if (this.next >= 4096) throw new Refusal('effects_exhausted', 'effect history full');
    const effect = `e${++this.next}`;
    // Store before sending: cleanup can arrive even if the acknowledgement is lost.
    this.entries.set(effect, { cleanup, released: false });
    const reply = await channel.request('effect_register', { context, effect, label });
    if (
      !reply ||
      typeof reply !== 'object' ||
      Array.isArray(reply) ||
      reply.effect !== effect ||
      typeof reply.grant !== 'string' ||
      !/^[0-9a-f]{32}$/.test(reply.grant)
    )
      throw new Refusal('protocol', 'invalid effect registration acknowledgement');
    return new Effect(channel, effect, reply.grant);
  }
  async release(effect: string, ctx: Context): Promise<void> {
    const entry = this.entries.get(effect);
    if (!entry) throw new Refusal('unknown_effect', 'effect not in this activation');
    if (!entry.result) {
      entry.result = (async () => {
        try {
          await entry.cleanup!(ctx);
        } finally {
          await ctx.finish();
        }
        entry.released = true;
        entry.cleanup = undefined;
      })();
    }
    await entry.result;
  }
  get released(): boolean {
    return [...this.entries.values()].every((entry) => entry.released);
  }
}
