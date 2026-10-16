import { request, answer, body, jsonBody, payload, type Request, type FixtureOutcome } from './binary-support.ts';
import type { Context as PublicContext } from '../src/index.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { Context, DependencyChannel, RpcError } from '../src/adapters/process/context.ts';
import { MiddlewareRegistry, Next } from '../src/adapters/process/middleware.ts';
import type { Value } from '../src/core/value.ts';
function setup() {
  const sent: Request[] = [];
  const channel = new DependencyChannel();
  channel.configure('a'.repeat(32), { incarnation: '1', fiber: '2', epoch: '3' }, async (v) => {
    sent.push(request(v));
  });
  const registry = new MiddlewareRegistry({ request: { name: 'example.request', revision: '1' } });
  const ctx = (cleanup = false) =>
    new Context(
      channel,
      'b'.repeat(32),
      { logger: '1' },
      undefined,
      cleanup,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      registry,
    );
  const next = () => new Next(channel, 'b'.repeat(32), 'w0', 'c'.repeat(32));
  const reply = (i: number, value: Value) => {
    const { session, activation, operation } = sent[i];
    channel.receive({
      session,
      activation,
      operation,
      outcome: { kind: 'success', value: answer(sent[i] ,value) },
    });
  };
  return { channel, registry, ctx, next, reply, sent };
}
test('next is single-use, expires, and detached next waits drain before callback release', async () => {
  const { registry, ctx, next, reply, sent } = setup();
  const original = ctx();
  const registration = original.middleware('request', (args, _ctx, next) => {
    void next.run(args);
    return body(5);
  });
  const continuation = next();
  const running = registry.invoke('m1', 'w0', body(7), ctx(), continuation);
  await assert.rejects(continuation.run(body(8)), { code: 'duplicate_next' });
  assert.throws(() => registry.release('m1'), { code: 'middleware_running' });
  await new Promise<void>((resolve) => setImmediate(resolve));
  await assert.rejects(continuation.run(body(8)), { code: 'expired_next' });
  let done = false;
  running.then(() => {
    done = true;
  });
  assert.equal(done, false);
  reply(1, 14);
  assert.deepEqual(await running, payload(null,body(5)));
  reply(0, { handler: 'm1', grant: 'd'.repeat(32) });
  const handle = await registration;
  await original.finish();
  const disposal = handle.dispose();
  assert.equal(sent[2].command, 'middleware_dispose');
  registry.release('m1');
  registry.release('m1');
  reply(2, {});
  await disposal;
  assert.equal(registry.released, true);
});
test('business failures reach the response outcome; terminal runs once and short circuits release unused callbacks', async () => {
  const { registry, ctx, next, reply } = setup();
  const registration = ctx().middleware('request', () => {
    throw new RpcError('closed', 'business');
  });
  reply(0, { handler: 'm1', grant: 'd'.repeat(32) });
  await registration;
  await assert.rejects(registry.invoke('m1','w0',body(null),ctx(),next()),{code:'closed',message:'business'});
  await assert.rejects(registry.invoke('m1', 'w0', body(null), ctx(), next()), { code: 'closed' });
  let saved: PublicContext | undefined;
  const running = ctx().waterfall('request', body(2), (args, ctx) => {
    saved = ctx;
    return args;
  });
  assert.deepEqual(await registry.terminal('q1', 'w1', body(3), ctx()), payload(null,body(3)));
  await assert.rejects(registry.terminal('q1', 'w2', body(3), ctx()), { code: 'closed' });
  await assert.rejects(saved!.invoke('logger', 'write', body(null)), { code: 'closed' });
  reply(1, 3);
  assert.deepEqual(await running,body(3));
  const short = ctx().waterfall('request', body(2), () => {
    throw new Error('must not execute');
  });
  reply(2, 4);
  assert.deepEqual(await short,body(4));
  registry.release('m1');
  assert.equal(registry.released, true);
});
test('middleware capability, context lifetime, cleanup and aliases are checked locally', async () => {
  const { ctx, sent } = setup();
  await assert.rejects(ctx().waterfall('unknown', body(null)), { code: 'undeclared_middleware' });
  await assert.rejects(
    ctx(true).middleware('request', () => body(null)),
    { code: 'closed' },
  );
  const expired = ctx();
  await expired.finish();
  await assert.rejects(expired.waterfall('request', body(null)), { code: 'closed' });
  assert.equal(sent.length, 0);
});
test('full runs and next lanes retain dependency and cleanup capacity', async () => {
  const { channel, reply } = setup();
  const runs = Array.from({ length: 16 }, () => channel.request('middleware_run', {}));
  const nexts = Array.from({ length: 16 }, () => channel.request('middleware_next', {}));
  await assert.rejects(channel.request('middleware_run', {}), { code: 'overloaded' });
  await assert.rejects(channel.request('middleware_next', {}), { code: 'overloaded' });
  const dep = channel.request('dependency_call', {}),
    cleanup = channel.request('cleanup_call', {});
  reply(32, null);
  reply(33, null);
  await dep;
  await cleanup;
  [...runs, ...nexts].forEach((_, i) => reply(i, null));
  await Promise.all([...runs, ...nexts]);
});
test('unknown middleware runs retain terminal ownership; known refusals release it',async()=>{
  for(const kind of ['unknown','refused'] as const){
    const {channel,registry,ctx,sent}=setup();
    const pending=ctx().waterfall('request',body(1),value=>value);
    const req=sent[0];
    channel.receive({session:req.session,activation:req.activation,operation:req.operation,outcome:{kind,error:{code:'lost',message:'lost'}}});
    await assert.rejects(pending);
    assert.equal(registry.released,kind==='refused');
    channel.close();
  }
});
