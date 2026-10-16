// The business API deliberately has no dependency on Node, a host or a codec.
export type { Message } from './core/message.ts';
export type { Value } from './core/value.ts';
export { RpcError, Refusal } from './core/error.ts';
export type { Context } from './core/context.ts';
export type { Service, Resource } from './core/service.ts';
export type { Composition } from './core/composition.ts';
export { Inverse, iterate } from './core/effects.ts';
export type { EffectStep, EffectProducer, EffectCompletion } from './core/effects.ts';
export { Continue, Break, EventDispatchError } from './core/events.ts';
export type { EventCallback, EventFlow } from './core/events.ts';
export type {
  ResourceHandle,
  Effect,
  EffectSequence,
  Scope,
  ScopeCallback,
  Task,
  TaskCallback,
  Subscription,
  Dispatch,
  Next,
  MiddlewareCallback,
  MiddlewareRegistration,
  TerminalCallback,
  Child,
} from './core/handles.ts';

export { valueInterface } from './core/contract.ts';
export type { Interface, MessageContract, MethodContract, ResourceContract } from './core/contract.ts';
export * as typed from './core/typed.ts';

export type { Validation } from './core/validation.ts';
