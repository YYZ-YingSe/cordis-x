import { request, answer, body, jsonBody, payload, type Request, type FixtureOutcome } from './binary-support.ts';
import type { Context as PublicContext } from '../src/index.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { Context, DependencyChannel, RpcError } from '../src/adapters/process/context.ts';
import {
  EventRegistry,
  Continue,
  Break,
  EventDispatchError,
} from '../src/adapters/process/events.ts';
import type { Value } from '../src/core/value.ts';
function setup() {
  const sent: Request[] = [];
  const channel = new DependencyChannel();
  channel.configure('a'.repeat(32), { incarnation: '1', fiber: '2', epoch: '3' }, async (v) => {
    sent.push(request(v));
  });
  const registry = new EventRegistry({ tick: { name: 'example.tick', revision: '1' } });
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
      registry,
    );
  const reply = (i: number, value: Value) => {
    const { session, activation, operation } = sent[i];
    channel.receive({
      session,
      activation,
      operation,
      outcome: { kind: 'success', value: answer(sent[i] ,value) },
    });
  };
  return { channel, registry, ctx, reply, sent };
}
test('listeners execute before registration acknowledgement, use fresh contexts, and release only after drain', async () => {
  const { registry, ctx, reply, sent } = setup();
  let saved: PublicContext | undefined;
  const original = ctx();
  const registration = original.on('tick', async (args, child) => {
    saved = child;
    await child.invoke('logger', 'write', args);
    return Break(args);
  });
  const running = registry.invoke('l1', 'v1', body(7), ctx());
  assert.throws(() => registry.release('l1'), { code: 'listeners_running' });
  reply(1, null);
  assert.deepEqual(await running, payload({kind:'break'},body(7)));
  reply(0, { listener: 'l1', grant: 'f'.repeat(32) });
  const subscription = await registration;
  await original.finish();
  await assert.rejects(saved!.invoke('logger', 'write', body(null)), { code: 'closed' });
  await assert.rejects(registry.invoke('l1', 'v1', body(7), ctx()), { code: 'closed' });
  const disposing = subscription.dispose();
  assert.deepEqual(sent[2].payload.control, { listener: 'l1', grant: 'f'.repeat(32) });
  registry.release('l1');
  registry.release('l1');
  reply(2, {});
  await disposing;
  assert.equal(registry.released, true);
  await assert.rejects(registry.invoke('l1', 'v2', body(7), ctx()), { code: 'closed' });
});
test('business errors stay inside callback outcomes; dispatch handles survive their creating context', async () => {
  const { registry, ctx, reply } = setup();
  const original = ctx();
  const registration = original.on('tick', () => {
    throw new RpcError('closed', 'business failure');
  });
  reply(0, { listener: 'l1', grant: 'f'.repeat(32) });
  await registration;
  await assert.rejects(registry.invoke('l1','v0',body(null),ctx()),{code:'closed',message:'business failure'});
  const emitting = original.emit('tick', body(null));
  reply(1, { dispatch: 'd1', grant: 'e'.repeat(32) });
  const dispatch = await emitting;
  await original.finish();
  const joining = assert.rejects(
    dispatch.join(),
    (e: unknown) => e instanceof EventDispatchError && e.errors.length === 2,
  );
  reply(2, {
    kind: 'failure',
    errors: [
      { code: 'listener_failed', message: 'a' },
      { code: 'listener_failed', message: 'b' },
    ],
  });
  await joining;
});
test('unknown events, expired contexts and cleanup registration reject locally; void means Continue', async () => {
  const { registry, ctx, reply, sent } = setup();
  await assert.rejects(
    ctx().on('unknown', () => {}),
    { code: 'undeclared_event' },
  );
  await assert.rejects(
    ctx(true).on('tick', () => {}),
    { code: 'closed' },
  );
  const original = ctx();
  await original.finish();
  await assert.rejects(original.emit('tick', body(null)), { code: 'closed' });
  assert.equal(sent.length, 0);
  const registration = ctx().on('tick', () => {});
  reply(0, { listener: 'l1', grant: 'f'.repeat(32) });
  await registration;
  assert.deepEqual(await registry.invoke('l1', 'v1', body(null), ctx()), payload({kind:'continue'}));
});
test('event joins and emissions retain independent capacity for callback dependencies', async () => {
  const { channel, reply } = setup();
  const joins = Array.from({ length: 16 }, () => channel.request('event_join', {}));
  await assert.rejects(channel.request('event_join', {}), { code: 'overloaded' });
  const emission = channel.request('event_emit', {}),
    dependency = channel.invoke('context','logger','write',body(null));
  reply(16, {});
  reply(17, null);
  await emission;
  await dependency;
  joins.forEach((_, i) => reply(i, null));
  await Promise.all(joins);
});
