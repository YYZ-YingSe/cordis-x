import { create } from '@bufbuild/protobuf';
import { WorkflowExport, WorkflowControls as controls, type WorkflowServer } from '@cordis-x/example-contracts/workflows';
import { DecisionSchema, DecisionsSchema, SettingsSchema, TickSchema } from '@cordis-x/example-contracts/workflows/messages/workflows';
import { Int64Schema, UnitSchema } from '@cordis-x/example-contracts/workflows/messages/cordis/types';
import { RpcError, typed } from '../src/index.ts';
import { run } from '../src/adapters/process.ts';
const number = (value: bigint) => create(Int64Schema, { value });
function add(a: bigint, b: bigint): bigint {
  const n = a + b;
  if (n < -(1n << 63n) || n >= (1n << 63n)) throw new RpcError('overflow', 'int64 overflow');
  return n;
}
function decision(flow: typed.Flow<ReturnType<typeof number>>) {
  return create(DecisionSchema, { state: flow.kind === 'break'
    ? { case: 'stopped', value: flow.value }
    : { case: 'continued', value: create(UnitSchema) } });
}
function business(): WorkflowServer {
  return {
    async activate(ctx) {
      await controls.tick().on(ctx, async (_, args) => args.value! < 0n
        ? typed.Continue() : typed.Break(number(add(args.value!, 1n))));
      await controls.request().register(ctx, async (_, args, next) => {
        if (args.value === 0n) return number(100n);
        const result = await next.run(create(TickSchema, { value: add(args.value!, 1n) }));
        return number(add(result.value!, 3n));
      });
      await controls.settings().provide(ctx, create(SettingsSchema, { answer: 42n }));
    },
    async run(ctx, args) {
      return controls.request().run(ctx, args, async (_, args) => number(add(args.value!, args.value!)));
    },
    async serial(ctx, args) { return decision(await controls.tick().serial(ctx, args)); },
    async parallel(ctx, args) {
      return create(DecisionsSchema, { values: (await controls.tick().parallel(ctx, args)).map(decision) });
    },
    async spawn_worker(ctx, config) {
      const child = await controls.worker().install(ctx, config);
      await child.ready();
      return create(UnitSchema);
    },
  };
}
await run(WorkflowExport(business, {}), { http: !process.argv.includes('--stdio-only') });
