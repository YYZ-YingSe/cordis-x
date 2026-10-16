import { request, answer, body, jsonBody, payload, type Request, type FixtureOutcome } from './binary-support.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { Context, DependencyChannel, RpcError } from '../src/adapters/process/context.ts';
import type { Context as PublicContext, ResourceHandle, Value } from '../src/index.ts';

function setup() {
  const sent: Request[] = [];
  const channel = new DependencyChannel();
  channel.configure('a'.repeat(32), { incarnation: '1', fiber: '2', epoch: '3' }, async (value) => {
    sent.push(request(value));
  });
  channel.enableResources(true);
  const context = (cleanup = false) => new Context(channel, cleanup ? 'cleanup' : `ctx${sent.length}`, {
    counters: '1',
  }, undefined, cleanup);
  const reply = (index: number, value: Value, failure = false) => {
    const { session, activation, operation } = sent[index];
    channel.receive({ session, activation, operation, outcome: failure
      ? { kind: 'unknown', error: value as {code:string;message:string} } : { kind: 'success', value:answer(sent[index],value) } });
  };
  const ok = (index: number, value: Value = null) => reply(index, { status: 'ok', value });
  async function acquire(ctx = context()): Promise<ResourceHandle> {
    const index = sent.length;
    const pending = ctx.acquire('counters', 'counter', body(40));
    const { resource } = sent[index].payload.control as Record<string, Value>;
    ok(index, { resource, grant: 'b'.repeat(32) });
    const handle = await pending;
    await ctx.finish();
    return handle;
  }
  return { channel, sent, context, reply, ok, acquire };
}

test('resource handles outlive acquisition, use each current Context, and reject foreign authority', async () => {
  const s = setup(), old = s.context();
  const handle = await s.acquire(old);
  assert.deepEqual(Object.keys(handle).sort(), ['dispose', 'invoke']);
  await assert.rejects(handle.invoke(old, 'get', body(null)), { code: 'closed' });
  await assert.rejects(handle.invoke(setup().context(), 'get', body(null)), { code: 'invalid_handle' });
  await assert.rejects(handle.invoke({ ...s.context() } as unknown as PublicContext, 'get', body(null)), { code: 'invalid_handle' });
  const current = s.context();
  const call = handle.invoke(current, 'add', body(2));
  assert.equal((s.sent[1].payload.control as Record<string, Value>).context, 'ctx1');
  s.ok(1, 42);
  assert.deepEqual(await call, body(42));
  const release = handle.dispose(current);
  s.ok(2);
  await release;
  await current.finish();
  await assert.rejects(handle.dispose(current), { code: 'closed' });
});

test('acquisition checks declarations, cleanup authority and host support before sending', async () => {
  const s = setup(), ctx = s.context(), cleanup = s.context(true);
  await assert.rejects(ctx.acquire('missing', 'counter', body(null)), { code: 'undeclared_dependency' });
  await assert.rejects(cleanup.acquire('counters', 'counter', body(null)), { code: 'closed' });
  s.channel.enableResources(false);
  await assert.rejects(ctx.acquire('counters', 'counter', body(null)), { code: 'unsupported_resources' });
  assert.equal(s.sent.length, 0);
  await ctx.finish();
  await cleanup.finish();
});

test('callback finish drains detached acquisition and resource operations', async () => {
  const s = setup(), ctx = s.context();
  const acquire = ctx.acquire('counters', 'counter', body(null));
  let done = false;
  const finishing = ctx.finish().then(() => { done = true; });
  await Promise.resolve();
  assert.equal(done, false);
  s.ok(0, { resource: 'r1', grant: 'b'.repeat(32) });
  const handle = await acquire;
  await finishing;
  for (const dispose of [false, true]) {
    const current = s.context(), index = s.sent.length;
    const task = dispose ? handle.dispose(current) : handle.invoke(current, 'get', body(null));
    done = false;
    const finish = current.finish().then(() => { done = true; });
    await Promise.resolve();
    assert.equal(done, false);
    s.ok(index);
    await task;
    await finish;
  }
});

test('known rejection preserves its code; unknown, malformed and lost acquisition results remain uncertain', async () => {
  for (const [value, failure] of [
    [{ status: 'rejected', error: { code: 'factory_rejected', message: 'no resource' } }, false],
    [{ status: 'unknown', error: { code: 'uncertain', message: 'partial work' } }, false],
    [{ status: 'ok', value: { resource: 'r2', grant: 'b'.repeat(32) } }, false],
    [{ status: 'ok', value: { resource: 'r1', grant: 'wrong' } }, false],
    [{ status: 'ok', value: null }, false],
    [{ code: 'internal', message: 'no certainty' }, true],
  ] as [Value, boolean][]) {
    const s = setup(), ctx = s.context();
    const pending = ctx.acquire('counters', 'counter', body(null));
    const known = !failure && (value as Record<string, Value>).status === 'rejected';
    const rejected = assert.rejects(pending, (e: unknown) => known
      ? e instanceof RpcError && e.code === 'factory_rejected'
      : e instanceof Error && !(e instanceof RpcError));
    s.reply(0, value, failure);
    await rejected;
    await ctx.finish();
  }
  const s = setup(), ctx = s.context();
  const pending = assert.rejects(ctx.acquire('counters', 'counter', body(null)), (e: unknown) =>
    e instanceof Error && !(e instanceof RpcError));
  s.channel.close();
  await pending;
  await ctx.finish();
});

test('resource disposal validates acknowledgement and cleanup cannot start disposal', async () => {
  const s = setup(), handle = await s.acquire(), cleanup = s.context(true);
  await assert.rejects(handle.dispose(cleanup), { code: 'closed' });
  const ctx = s.context(), pending = handle.dispose(ctx);
  s.ok(1, {});
  await assert.rejects(pending, (e: unknown) => e instanceof Error && !(e instanceof RpcError));
  await ctx.finish();
});

test('resource calls, cleanup calls and disposal retain separate bounded capacity', async () => {
  const s = setup(), handle = await s.acquire(), ctx = s.context(), cleanup = s.context(true);
  const calls = Array.from({ length: 16 }, () => handle.invoke(ctx, 'get', body(null)));
  await assert.rejects(handle.invoke(ctx, 'get', body(null)), { code: 'overloaded' });
  const ordinary = Array.from({ length: 16 }, () => ctx.invoke('counters', 'busy', body(null)));
  await assert.rejects(ctx.invoke('counters', 'busy', body(null)), { code: 'overloaded' });
  const read = handle.invoke(cleanup, 'get', body(null)), release = handle.dispose(ctx);
  assert.equal(s.sent[33].command, 'cleanup_resource_call');
  assert.equal(s.sent[34].command, 'dependency_resource_dispose');
  s.ok(33, 42);
  s.ok(34);
  assert.deepEqual(await read, body(42));
  await release;
  for (let i = 1; i <= 16; i++) s.ok(i);
  for (let i = 17; i <= 32; i++) s.reply(i, null);
  await Promise.all([...calls, ...ordinary]);
  await ctx.finish();
  await cleanup.finish();
});

test('resource capability history is bounded and never reuses an ID', async () => {
  const s = setup(), ctx = s.context();
  const ids = new Set<Value>();
  for (let i = 0; i < 4096; i++) {
    const pending = ctx.acquire('counters', 'counter', body(null));
    const id = (s.sent[i].payload.control as Record<string, Value>).resource;
    assert.equal(ids.has(id), false);
    ids.add(id);
    s.ok(i, { resource: id, grant: 'b'.repeat(32) });
    await pending;
  }
  await assert.rejects(ctx.acquire('counters', 'counter', body(null)), { code: 'resources_exhausted' });
  assert.equal(s.sent.length, 4096);
  await ctx.finish();
});
