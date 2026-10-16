import { create } from '@bufbuild/protobuf';
import { UInt64ValueSchema } from '@bufbuild/protobuf/wkt';
import { typed } from '../../src/index.ts';
import type { Value } from '../../src/index.ts';
import { SequenceStatusSchema, TaskStatusSchema, CompletionSchema, ScopeStatusSchema } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/managed_status';
export const count = (n: number) => create(UInt64ValueSchema, { value: BigInt(n) });
function record(value: Value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid managed diagnostic');
  return value;
}
// Projection of runtime diagnostics; it creates no capability or cleanup state.
function outcome(value: Value, sequence: boolean): Value {
  if (value === null) return { pending: {} };
  const raw = record(value);
  if (Object.keys(raw).length !== 2) throw new Error('invalid managed outcome');
  if (raw.kind === 'success' && Object.hasOwn(raw, 'value')) {
    if (!sequence && raw.value !== null) throw new Error('task success must have no payload');
    return { succeeded: sequence ? raw.value : {} };
  }
  if ((raw.kind === 'failure' || raw.kind === 'refused') && Object.hasOwn(raw, 'error'))
    return { [raw.kind === 'failure' ? 'failed' : 'refused']: raw.error };
  throw new Error('invalid managed outcome kind');
}
export function sequenceStatus(value: Value) {
  const raw = record(value);
  return typed.protobuf(SequenceStatusSchema).fromJson({ ...raw, forward: outcome(raw.forward, true) });
}
export function taskStatus(value: Value) {
  const raw = record(value);
  return typed.protobuf(TaskStatusSchema).fromJson({ ...raw, outcome: outcome(raw.outcome, false) });
}
export const completion = (value: string) => typed.protobuf(CompletionSchema).fromJson({ completion: value });
export const scopeStatus = typed.protobuf(ScopeStatusSchema);
