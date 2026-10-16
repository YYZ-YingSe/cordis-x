import type { Message } from './message.ts';
import { RpcError } from './error.ts';
import type { Context } from './context.ts';
import type { Value } from './value.ts';
export type EventFlow = { kind: 'continue' } | { kind: 'break'; value: Message };
export const Continue = (): EventFlow => ({ kind: 'continue' });
export const Break = (value: Message): EventFlow => ({ kind: 'break', value });
export type EventCallback = (
  args: Message,
  ctx: Context,
) => EventFlow | void | Promise<EventFlow | void>;
export class EventDispatchError extends RpcError {
  errors: Value[];
  constructor(errors: Value[]) {
    super('event_failed', JSON.stringify(errors));
    this.errors = errors;
  }
}
