import type { Message } from './message.ts';
import type { Value } from './value.ts';
import type { EffectProducer } from './effects.ts';
import type { EventCallback } from './events.ts';
import type {
  ResourceHandle,
  Effect,
  EffectSequence,
  Scope,
  Task,
  TaskCallback,
  Subscription,
  Dispatch,
  MiddlewareCallback,
  MiddlewareRegistration,
  TerminalCallback,
  Child,
} from './handles.ts';

/** Authority for one callback. Keeping this object does not extend its lifetime. */
export interface Context {
  exportReference(descriptor: import('./contract.ts').Interface, object: import('./reference.ts').ReferenceObject): Promise<string>;
  invokeReference(key: string, digest: string, method: string, args: Message): Promise<Message>;
  disposeReference(key: string): Promise<void>;
  readonly validation: import('./validation.ts').Validation;
  acquire(dependency: string, kind: string, args: Message): Promise<ResourceHandle>;
  invoke(dependency: string, method: string, args: Message): Promise<Message>;
  metadata(name: string, revision: string, signal?: AbortSignal): Promise<{ revision: string | null; layers: Value[] }>;
  defer(label: string, cleanup: (ctx: Context) => void | Promise<void>): Promise<Effect>;
  effect(label: string, producer: EffectProducer): Promise<EffectSequence>;
  scope(label: string): Promise<Scope>;
  spawn(label: string, callback: TaskCallback): Promise<Task>;
  cancelled(signal?: AbortSignal): Promise<void>;
  on(event: string, callback: EventCallback): Promise<Subscription>;
  emit(event: string, args: Message): Promise<Dispatch>;
  parallel(event: string, args: Message): Promise<import('./events.ts').EventFlow[]>;
  serial(event: string, args: Message): Promise<import('./events.ts').EventFlow>;
  middleware(alias: string, callback: MiddlewareCallback): Promise<MiddlewareRegistration>;
  waterfall(alias: string, args: Message, terminal?: TerminalCallback): Promise<Message>;
  install(factory: string, config: Message): Promise<Child>;
  provide(service: string, value: Message): Promise<void>;
}
