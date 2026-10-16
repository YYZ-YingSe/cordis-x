import type { Message } from './message.ts';
import type { Interface } from './contract.ts';
import type { Context } from './context.ts';
import type { Composition } from './composition.ts';
import type { Value } from './value.ts';
export interface Resource {
  invoke(method: string, args: Message, ctx?: Context): Message | Promise<Message>;
  close(ctx?: Context): void | Promise<void>;
}
export interface Service {
  /** Generated dispatch owns decode and validation; raw services use adapter checks. */
  readonly typedDispatch?: true;
  interface: Interface;
  dependencyInterfaces?: Record<string, Interface>;
  contract: { name: string; revision: string };
  dependencies?: Record<string, string>;
  events?: Record<string, { name: string; revision: string }>;
  middleware?: Record<string, { name: string; revision: string }>;
  composition?: Composition;
  activate(ctx: Context): void | Promise<void>;
  invoke(method: string, args: Message, ctx: Context): Message | Promise<Message>;
  acquire?(kind: string, args: Message, ctx: Context): Resource | Promise<Resource>;
  deactivate(ctx: Context): void | Promise<void>;
}
