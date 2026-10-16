import { create } from '@bufbuild/protobuf';
import { EmptySchema, Int64ValueSchema } from '@bufbuild/protobuf/wkt';
import { CounterHolderExport, CounterHolderDependencies, CountersExport, CountersDependencies } from '@cordis-x/example-contracts/counters';
import type { CountersCounterClient } from '@cordis-x/example-contracts/counters';
import { CounterClosedSchema } from '@cordis-x/example-contracts/counters/messages/fixtures/counters';
import { RpcError, typed } from '../../src/index.ts';

export function counters() {
  return CountersExport(() => ({
    async counter(_ctx, args) {
      let value = args.value;
      return {
        async get() { return create(Int64ValueSchema, { value }); },
        async add(_ctx, args) {
          const next = value + args.value;
          if (next < -(1n << 63n) || next >= (1n << 63n))
            throw new RpcError('overflow', 'counter overflow');
          value = next;
          return create(Int64ValueSchema, { value });
        },
        async close(ctx) {
          await CountersDependencies.logger().write(ctx, create(CounterClosedSchema, { counterClosed: value }));
        },
      };
    },
  }), {});
}

export function client() {
  let resource: CountersCounterClient | undefined;
  const saved = () => {
    if (!resource) throw new RpcError('empty', 'acquire a counter first');
    return resource;
  };
  return CounterHolderExport(() => ({
    async acquire(ctx, args) {
      resource = await CounterHolderDependencies.counters().counter(ctx, args);
      return create(EmptySchema);
    },
    async get(ctx, args) { return saved().get(ctx, args); },
    async add(ctx, args) { return saved().add(ctx, args); },
    async release(ctx) { await saved().dispose(ctx); return create(EmptySchema); },
    async scoped(ctx, args) {
      const counter = await CounterHolderDependencies.counters().counter(ctx, args);
      const scope = await ctx.scope('counter scope');
      const result = await scope.run(async inner => {
        await inner.defer('read before close', async cleanup => {
          await counter.get(cleanup, create(EmptySchema));
        });
        return typed.protobuf(Int64ValueSchema).toJson(await counter.get(inner, create(EmptySchema)));
      });
      await scope.dispose();
      await counter.dispose(ctx);
      return typed.protobuf(Int64ValueSchema).fromJson(result);
    },
  }), {});
}
