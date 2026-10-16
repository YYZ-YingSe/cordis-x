/** Completed operation result, including acquisition certainty. */
import { Refusal, RpcError } from '../../core/error.ts';
import type { Value } from '../../core/value.ts';
function object(value: Value): value is Record<string, Value> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
export function operationOutcome(reply: Value): Value {
  if (object(reply) && Object.keys(reply).length === 2) {
    if (reply.status === 'ok' && Object.hasOwn(reply, 'value')) return reply.value;
    if (
      (reply.status === 'rejected' || reply.status === 'refused') &&
      object(reply.error) &&
      Object.keys(reply.error).length === 2 &&
      typeof reply.error.code === 'string' &&
      typeof reply.error.message === 'string'
    )
      throw reply.status === 'refused' ? new Refusal(reply.error.code, reply.error.message) : new RpcError(reply.error.code, reply.error.message);
  }
  // Unknown and malformed acknowledgements never become known business rejections.
  throw new Error('unknown operation outcome');
}

