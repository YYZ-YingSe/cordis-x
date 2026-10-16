import type { Validation } from '../../core/validation.ts';
import { auditBinary } from '../../core/protobuf-wire.ts';
import { create, fromBinary, fromJson, toBinary, toJson } from '@bufbuild/protobuf';
import { ValueSchema } from '@bufbuild/protobuf/wkt';
import { reflect, isReflectMessage, isReflectList, isReflectMap, type ReflectMessage } from '@bufbuild/protobuf/reflect';
import { FrameSchema, PayloadSchema, type Payload as ProtoPayload } from '#cordis-generated/transport';
import { MAX_MESSAGE, type Value } from '../../core/value.ts';
import type { Payload, Request, Response, Outcome } from './messages.ts';
export const CONTENT_TYPE = 'application/protobuf';
function checked(value: ReflectMessage, budget = { nodes: 65536 }): void {
  if (--budget.nodes < 0) throw new Error('control node limit exceeded');
  if(value.desc.typeName==='google.protobuf.Value') {
    const fields=value.fields.filter(field=>value.isSet(field));
    if(fields.length!==1) throw new Error('missing control value branch');
    if(fields[0].name==='null_value' && value.get(fields[0])!==0) throw new Error('invalid null enum');
  }
  const child = (v: unknown) => {
    if (isReflectMessage(v)) checked(v, budget);
    else if (isReflectList(v)) for (const item of v) child(item);
    else if (isReflectMap(v)) for (const [, item] of v) child(item);
    else if (--budget.nodes < 0) throw new Error('control node limit exceeded');
  };
  for (const field of value.fields) if (value.isSet(field)) child(value.get(field));
}
export function validateControl(value: Value): void {
  let nodes=8192, bytes=MAX_MESSAGE;
  const walk=(value: Value,depth: number): void => {
    if(depth>16) throw new Error('control nesting too deep');
    if(--nodes<0) throw new Error('control node limit exceeded');
    bytes-=2;
    if(typeof value==='number' && (!Number.isFinite(value) || (Number.isInteger(value) && !Number.isSafeInteger(value))))
      throw new Error('control integers outside safe range require a decimal string');
    if(typeof value==='string') {
      if(!value.isWellFormed()) throw new Error('invalid control Unicode');
      bytes-=Buffer.byteLength(value);
    } else if(Array.isArray(value)) for(const child of value) walk(child,depth+1);
    else if(value!==null && typeof value==='object') {
      if(Object.getPrototypeOf(value)!==null && Object.getPrototypeOf(value)!==Object.prototype) throw new Error('expected control object');
      for(const [key,child] of Object.entries(value)) {
        if(!key.isWellFormed()) throw new Error('invalid control key');
        bytes-=Buffer.byteLength(key);walk(child,depth+1);
      }
    } else if(!['boolean','number'].includes(typeof value) && value!==null) throw new Error('invalid control value');
    if(bytes<0) throw new Error('control byte limit exceeded');
  };
  walk(value,0);
}
function encodePayload(value: Payload): ProtoPayload {
  validateControl(value.control);
  if (value.messages.length > 4096 || value.messages.reduce((n, b) => n + b.byteLength, 0) > MAX_MESSAGE)
    throw new Error('message too large');
  return create(PayloadSchema, { control: fromJson(ValueSchema, value.control), messages: value.messages });
}
function decodePayload(value: ProtoPayload | undefined): Payload {
  if (!value?.control || value.control.kind.case === undefined) throw new Error('missing control value');
  if(value.messages.length>4096) throw new Error('too many business messages');
  const control = toJson(ValueSchema, value.control) as Value;
  validateControl(control);
  return { control, messages: value.messages };
}
export function encodeFrame(value: Request | Response): Uint8Array {
  const frame = create(FrameSchema, { session: value.session, activation: value.activation, operation: value.operation });
  if ('command' in value) frame.direction = { case: 'request', value: { $typeName: 'cordis.transport.Request', command: value.command, payload: encodePayload(value.payload) } };
  else {
    const outcome = value.outcome;
    frame.direction = { case: 'response', value: { $typeName: 'cordis.transport.Response', outcome:
      outcome.kind === 'success' ? { case: 'success', value: encodePayload(outcome.value) } : {
        case: outcome.kind === 'failure' ? 'rejected' : outcome.kind,
        value: { $typeName: 'cordis.transport.Error', ...outcome.error },
      },
    } };
  }
  const bytes = toBinary(FrameSchema, frame);
  if (bytes.byteLength > MAX_MESSAGE) throw new Error('message too large');
  return bytes;
}
export function decodeFrame(bytes: Uint8Array, policy: Validation = 'checked'): Request | Response {
  if (bytes.byteLength > MAX_MESSAGE) throw new Error('message too large');
  if (policy === 'audit') auditBinary(FrameSchema, bytes);
  const frame = fromBinary(FrameSchema, bytes, { recursionLimit: 100 });
  checked(reflect(FrameSchema, frame), { nodes: 65536 });
  if (!frame.activation) throw new Error('missing activation');
  const base = { session: frame.session, operation: frame.operation, activation: {
    incarnation: frame.activation.incarnation, fiber: frame.activation.fiber, epoch: frame.activation.epoch,
  } };
  if (frame.direction.case === 'request') return { ...base, command: frame.direction.value.command, payload: decodePayload(frame.direction.value.payload) };
  if (frame.direction.case !== 'response') throw new Error('missing frame direction');
  const outcome = frame.direction.value.outcome;
  if (outcome.case === undefined) throw new Error('missing outcome');
  if (outcome.case === 'success') return { ...base, outcome: { kind: 'success', value: decodePayload(outcome.value) } };
  const result: Outcome = { kind: outcome.case === 'rejected' ? 'failure' : outcome.case, error: { code: outcome.value.code, message: outcome.value.message } };
  return { ...base, outcome: result };
}
