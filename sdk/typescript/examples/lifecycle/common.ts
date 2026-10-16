import { create } from '@bufbuild/protobuf';
import { EmptySchema, Int64ValueSchema, StringValueSchema } from '@bufbuild/protobuf/wkt';
import { RpcError, typed } from '../../src/index.ts';
import type { Context } from '../../src/index.ts';
import { LifecycleLogClient } from '@cordis-x/example-contracts/lifecycle';
import { ChildStatusSchema } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/common';
export const unit = () => create(EmptySchema);
export const number = (value: bigint) => create(Int64ValueSchema, { value });
export function add(a: bigint, b: bigint): bigint {
  const result = a + b;
  if (result < -(1n << 63n) || result >= (1n << 63n))
    throw new RpcError('overflow', 'int64 arithmetic overflow');
  return result;
}
export async function log(client: LifecycleLogClient, ctx: Context, value: string): Promise<void> {
  await client.write(ctx, create(StringValueSchema, { value }));
}
export const childStatus = typed.protobuf(ChildStatusSchema);
