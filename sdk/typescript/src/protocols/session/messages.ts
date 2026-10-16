import type { Value } from '../../core/value.ts';
import type { Message } from '../../core/message.ts';
export type Identity = { incarnation: string; fiber: string; epoch: string };
export type Envelope = { session: string; activation: Identity; operation: string };
export type Payload = { control: Value; messages: Message[] };
export type Request = Envelope & { command: string; payload: Payload };
export type Outcome = { kind: 'success'; value: Payload } |
  { kind: 'failure' | 'refused' | 'unknown'; error: { code: string; message: string } };
export type Response = Envelope & { outcome: Outcome };
export const payload = (control: Value = null, message?: Message): Payload =>
  ({ control, messages: message === undefined ? [] : [message] });
export function one(value: Payload): Message {
  if (value.messages.length !== 1) throw new Error('expected one business message');
  return value.messages[0];
}
export function controlOnly(value: Payload): Value {
  if (value.messages.length) throw new Error('unexpected business message');
  return value.control;
}
