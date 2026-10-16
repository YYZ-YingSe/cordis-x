import test from 'node:test';
import assert from 'node:assert/strict';
import { create, toBinary } from '@bufbuild/protobuf';
import { WorkflowControls as controls, WorkflowExport } from '@cordis-x/example-contracts/workflows';
import { TickSchema, SettingsSchema, WorkerConfigSchema } from '@cordis-x/example-contracts/workflows/messages/workflows';
import { Int64Schema } from '@cordis-x/example-contracts/workflows/messages/cordis/types';
import { Refusal, RpcError, typed } from '../src/index.ts';
import type { Context, EventCallback, MiddlewareCallback, Value, Message } from '../src/index.ts';
const tick = (value: bigint) => create(TickSchema, { value });
const number = (value: bigint) => create(Int64Schema, { value });

test('generated bindings declare controls separately from incoming methods', () => {
  const exported = WorkflowExport(() => { throw new Error('business must remain deferred'); }, {});
  assert.equal(exported.events?.tick.name, 'example.tick');
  assert.equal(exported.middleware?.request.name, 'example.request');
  assert.deepEqual(exported.composition?.factories, ['worker']);
  assert.equal(exported.composition?.provisions.settings.name, 'example.settings');
  assert.equal(exported.interface.methods.mode, 'named');
  if (exported.interface.methods.mode === 'named') assert.equal(exported.interface.methods.value.tick, undefined);
});

test('typed event and middleware callbacks reject input before business and retain result failures', async () => {
  let listener: EventCallback | undefined;
  let handler: MiddlewareCallback | undefined;
  let entered = 0;
  const ctx = { validation: 'checked',
    async on(_alias: string, callback: EventCallback) { listener = callback; return { async dispose() {} }; },
    async middleware(_alias: string, callback: MiddlewareCallback) { handler = callback; return { async dispose() {} }; },
  } as unknown as Context;
  await controls.tick().on(ctx, async (current, args) => {
    assert.equal(current, ctx); entered++;
    if (args.value === 1n) return typed.Break(create(Int64Schema));
    if (args.value === 2n) throw new RpcError('undeclared', 'fault');
    if (args.value === 3n) throw new Refusal('overflow', 'authority');
    return typed.Continue();
  });
  await assert.rejects(async () => listener!(new Uint8Array(), ctx), Refusal);
  assert.equal(entered, 0);
  assert.deepEqual(await listener!(toBinary(TickSchema,tick(0n)),ctx), { kind: 'continue' });
  for (const value of ['1', '2'])
    await assert.rejects(async () => listener!(toBinary(TickSchema,tick(BigInt(value))),ctx), e => e instanceof Error && !(e instanceof RpcError));
  await assert.rejects(async () => listener!(toBinary(TickSchema,tick(3n)),ctx), Refusal);
  let nextCalls = 0;
  await controls.request().register(ctx, async (current, args, next) => {
    assert.equal(current, ctx);
    await assert.rejects(next.run(create(TickSchema)), Refusal);
    return next.run(args);
  });
  const next = { async run(value: Message) { nextCalls++; return value; } };
  assert.deepEqual(await handler!(toBinary(TickSchema,tick(7n)),ctx,next),toBinary(Int64Schema,number(7n)));
  assert.equal(nextCalls, 1);
});

test('factory config, provision output and dispatch outcomes are checked', async () => {
  let entered = 0;
  const factory = controls.worker();
  for (const bad of [new Uint8Array(),new Uint8Array([0x18,1]),new Uint8Array([0x0a,1,255])])
    await assert.rejects(factory.build(bad, () => { entered++; }), Refusal);
  assert.equal(entered, 0);
  assert.equal(await factory.build(toBinary(WorkerConfigSchema,create(WorkerConfigSchema,{label:'x'})), value => value.label), 'x');
  await assert.rejects(factory.build(toBinary(WorkerConfigSchema,create(WorkerConfigSchema,{label:'x'})), () => { throw new RpcError('undeclared', 'fault'); }), e => e instanceof Error && !(e instanceof RpcError));
  const operations: unknown[] = [];
  const ctx = { validation: 'checked',
    async install(alias: string, config: Message) { operations.push({ alias, config }); return { async ready() { return null; } }; },
    async provide(alias: string, value: Message) { operations.push({ alias, value }); },
    async emit() { return { async join() { return [{ kind: 'continue' }, { kind: 'break', value:toBinary(Int64Schema,number(9n)) }]; } }; },
    async serial() { return { kind: 'break', value:new Uint8Array() }; },
  } as unknown as Context;
  await factory.install(ctx, create(WorkerConfigSchema, { label: 'x' }));
  await controls.settings().provide(ctx, create(SettingsSchema, { answer: 42n }));
  assert.deepEqual(operations, [{ alias: 'worker', config:toBinary(WorkerConfigSchema,create(WorkerConfigSchema,{label:'x'})) }, { alias: 'settings', value:toBinary(SettingsSchema,create(SettingsSchema,{answer:42n})) }]);
  assert.deepEqual(await controls.tick().parallel(ctx, tick(1n)), [typed.Continue(), typed.Break(number(9n))]);
  await assert.rejects(controls.tick().serial(ctx, tick(1n)), e => e instanceof Error && !(e instanceof Refusal));
  assert.throws(() => controls.settings().decode(new Uint8Array()), e => e instanceof Error && !(e instanceof Refusal));
});
