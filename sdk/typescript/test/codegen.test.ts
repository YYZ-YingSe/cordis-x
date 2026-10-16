import test from 'node:test';
import assert from 'node:assert/strict';
import { create, toBinary } from '@bufbuild/protobuf';
import * as sdk from '../src/index.ts';
import { AdderClient, AdderExport, AdderAddError, CounterClient, CounterExport, type CounterCounterServer } from '@cordis-x/example-contracts/examples';
import { AddArgsSchema, CounterArgsSchema } from '@cordis-x/example-contracts/examples/messages/examples';
import { Int64Schema, UnitSchema } from '@cordis-x/example-contracts/examples/messages/cordis/types';
import { DataSchema, OptionOptionBoolSchema } from '@cordis-x/test-contracts/profile/messages/profile';
const number = (value: bigint) => create(Int64Schema, { value });
test('typed dispatch validates before business and preserves int64 and failures', async () => {
    let entries = 0;
    assert.throws(() => AdderExport(() => { throw new Error('factory must not run'); }, true), sdk.Refusal);
    const service = AdderExport(() => ({ async add(_ctx, args) { entries++; const value = args.left! + args.right!; if (value < -(1n << 63n) || value >= (1n << 63n))
            throw new AdderAddError('overflow', 'int64 overflow'); return number(value); } }), {});
    const ctx = { validation: 'checked', async invoke(dep, method, args) { assert.equal(dep, 'example.adder'); return service.invoke(method, args, ctx); } } as sdk.Context;
    await service.activate(ctx);
    assert.equal((await new AdderClient().add(ctx, create(AddArgsSchema, { left: 20n, right: 22n }))).value, 42n);
    await assert.rejects(async () => service.invoke('add', toBinary(AddArgsSchema,create(AddArgsSchema,{left:20n})), ctx), sdk.Refusal);
    assert.equal(entries, 1);
    await assert.rejects(new AdderClient().add(ctx, create(AddArgsSchema, { left: (1n << 63n) - 1n, right: 1n })), AdderAddError);
    const bad = AdderExport(() => ({ async add() { throw new sdk.RpcError('undeclared', 'fault'); } }), {});
    await bad.activate(ctx);
    await assert.rejects(async () => bad.invoke('add',toBinary(AddArgsSchema,create(AddArgsSchema,{left:1n,right:1n})),ctx), (e: unknown) => e instanceof Error && !(e instanceof sdk.RpcError));
});
test('nested option, bytes, full integers, floats, recursion and exact array size', () => {
    const codec = sdk.typed.protobuf(OptionOptionBoolSchema);
    for (const state of [{ none: 'NULL_VALUE' }, { some: { none: 'NULL_VALUE' } }, { some: { some: false } }, { some: { some: true } }] as sdk.Value[]) {
        assert.deepEqual(codec.toJson(codec.fromJson(state)), state);
    }
    for (const bad of [{}, { some: {} }, { some: false }, { none: 'NULL_VALUE', some: { none: 'NULL_VALUE' } }] as sdk.Value[])
        assert.throws(() => codec.fromJson(bad));
    const data = sdk.typed.protobuf(DataSchema);
    const native = data.fromJson({ int32Value: 0, option: { some: { some: false } }, int64Value: '-9223372036854775808', uint64Value: '18446744073709551615', float32Value: 1.25, float64Value: -1.5, binary: 'AAH/', position: { values: [1, 2, 3] }, tree: { children: [{}] } });
    assert.equal(native.uint64Value, (1n << 64n) - 1n);
    assert.deepEqual(data.decode(data.encode(native)), native);
    native.position!.values.pop();
    assert.throws(() => data.encode(native, 'checked'), /size/);
    const cycle = create(DataSchema, { int32Value: 0, option: create(OptionOptionBoolSchema, { state: { case: 'none', value: 0 } }) });
    // Invalid JavaScript object graphs are rejected before the library recursively serializes them.
    const tree: any = { $typeName: 'profile.Tree', label: '', children: [] };
    tree.children.push(tree);
    cycle.tree = tree;
    assert.throws(() => data.encode(cycle, 'checked'), /cyclic/);
});
test('resource clients forward current Context and retain exactly-once cleanup', async () => {
    let closed = 0, value = 10n;
    const contexts: sdk.Context[] = [];
    const object: CounterCounterServer = { async get() { return number(value); }, async add(_ctx, n) { value += n.value!; return number(value); }, async close() { closed++; } };
    const server = CounterExport(() => ({ async counter() { return object; } }), {});
    const ctx = { validation: 'checked',} as sdk.Context, later = {} as sdk.Context;
    await server.activate(ctx);
    const resource = await server.acquire!('counter',toBinary(CounterArgsSchema,create(CounterArgsSchema,{initial:10n})),ctx);
    const handle: sdk.ResourceHandle = { async invoke(current, method, args) { contexts.push(current); return resource.invoke(method, args, current); }, async dispose(current) { contexts.push(current); await resource.close(current); } };
    const client = await new CounterClient().counter({ validation: 'checked', async acquire() { return handle; } } as unknown as sdk.Context, create(CounterArgsSchema, { initial: 10n }));
    assert.equal((await client.add(later, number(2n))).value, 12n);
    await client.dispose(later);
    assert.deepEqual(contexts, [later, later]);
    assert.equal(closed, 1);
});
