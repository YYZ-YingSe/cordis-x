import { create } from '@bufbuild/protobuf';
import { Int64ValueSchema, StringValueSchema } from '@bufbuild/protobuf/wkt';
import { ServiceExport } from '@cordis-x/example-contracts/calculator_process';
import { MetadataResultSchema, MetadataForResultSchema, CounterMetadataResultSchema } from '@cordis-x/example-contracts/calculator_process/messages/fixtures/calculator_process';
import { run } from '../src/adapters/process.ts';
import { RpcError, Refusal, typed } from '../src/index.ts';
import { codeVersion } from './calculator-version.ts';

function add(left: bigint, right: bigint): bigint {
  const value = left + right;
  if (value < -(1n << 63n) || value >= (1n << 63n))
    throw new RpcError('overflow', 'int64 addition overflow');
  return value;
}
await run(ServiceExport(() => {
  let open = false;
  const ready = () => { if (!open) throw new Refusal('closed', 'calculator closed'); };
  return {
    async activate() { open = true; },
    async deactivate() { open = false; },
    async code_version() { ready(); return create(StringValueSchema, { value: codeVersion }); },
    async echo(_ctx, args) { ready(); return args; },
    async add(_ctx, args) {
      ready();
      return create(Int64ValueSchema, { value: add(args.left!, args.right!) });
    },
    async metadata(ctx) {
      ready();
      return typed.protobuf(MetadataResultSchema).fromJson(await ctx.metadata('example.calculator', '1'));
    },
    async metadata_for(ctx, args) {
      ready();
      return typed.protobuf(MetadataForResultSchema).fromJson(await ctx.metadata(args.name!, args.revision!));
    },
    async counter(_ctx, args) {
      ready();
      let value = args.initial ?? 0n;
      let closed = false;
      const available = () => { ready(); if (closed) throw new Refusal('closed', 'counter closed'); };
      return {
        async get() { available(); return create(Int64ValueSchema, { value }); },
        async add(_ctx, args) {
          available(); value = add(value, args.value);
          return create(Int64ValueSchema, { value });
        },
        async code_version() { available(); return create(StringValueSchema, { value: codeVersion }); },
        async metadata(ctx) {
          available();
          return typed.protobuf(CounterMetadataResultSchema).fromJson(await ctx.metadata('example.calculator', '1'));
        },
        async close() {
          if (!open) throw new Error('provider closed before resource');
          closed = true;
        },
      };
    },
  };
}, {}), { http: !process.argv.includes('--stdio-only') });
