import type { Message } from './message.ts';
import type { Context } from './context.ts';
import type { Value } from './value.ts';
import type { EffectCompletion } from './effects.ts';

/** Handles describe authority, never Session IDs or transport connections. */
export interface ResourceHandle {
  invoke(ctx: Context, method: string, args: Message): Promise<Message>;
  dispose(ctx: Context): Promise<void>;
}
export interface Effect {
  dispose(): Promise<void>;
}
export interface EffectSequence extends Effect {
  ready(): Promise<EffectCompletion>;
  snapshot(): Promise<Value>;
}
export type ScopeCallback = (ctx: Context) => Value | void | Promise<Value | void>;
export interface Scope extends Effect {
  ready(): Promise<void>;
  snapshot(): Promise<Value>;
  run(callback: ScopeCallback): Promise<Value>;
}
export type TaskCallback = (ctx: Context) => void | Promise<void>;
export interface Task {
  join(): Promise<void>;
  snapshot(): Promise<Value>;
}
export interface Subscription extends Effect {}
export interface Dispatch {
  join(): Promise<import('./events.ts').EventFlow[]>;
  snapshot(): Promise<Value>;
}
export interface Next {
  run(args: Message): Promise<Message>;
}
export interface MiddlewareRegistration extends Effect {}
export type MiddlewareCallback = (args: Message, ctx: Context, next: Next) => Message | Promise<Message>;
export type TerminalCallback = (args: Message, ctx: Context) => Message | Promise<Message>;
export interface Child extends Effect {
  ready(): Promise<Value>;
  settled(): Promise<Value>;
  status(): Promise<Value>;
}
