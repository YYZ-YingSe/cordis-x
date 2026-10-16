import { request, answer, body, jsonBody, payload, type Request, type FixtureOutcome } from './binary-support.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { Context, DependencyChannel } from '../src/adapters/process/context.ts';
import { Child, checkComposition } from '../src/adapters/process/composition.ts';
import type { Value } from '../src/core/value.ts';
const manifest = {
  factories: ['worker'],
  provisions: { settings: { name: 'settings', revision: '1' } },
};
function setup(cleanup = false) {
  const channel = new DependencyChannel(),
    sent: Request[] = [];
  channel.configure('a'.repeat(32), { incarnation: '1', fiber: '2', epoch: '3' }, async (v) => {
    sent.push(request(v));
  });
  const ctx = new Context(
    channel,
    'b'.repeat(32),
    {},
    undefined,
    cleanup,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    undefined,
    manifest,
  );
  const reply = (n: number, value: Value) => {
    const { session, activation, operation } = sent[n];
    channel.receive({
      session,
      activation,
      operation,
      outcome: { kind: 'success', value: answer(sent[n] ,value) },
    });
  };
  return { ctx, channel, sent, reply };
}
test('composition registration drains before context expires, child handles survive callback', async () => {
  const { ctx, reply, sent } = setup();
  const pending = ctx.install('worker', body(null));
  let closed = false;
  const closing = ctx.finish().then(() => {
    closed = true;
  });
  await assert.rejects(ctx.provide('settings', body(1)), { code: 'closed' });
  assert.equal(closed, false);
  reply(0, { status: 'ok', value: { child: 'c'.repeat(32), grant: 'd'.repeat(32) } });
  const child = await pending;
  await closing;
  const status = child.status();
  assert.equal(sent[1].command, 'child_status');
  reply(1, { state: 'active' });
  assert.deepEqual(await status, { state: 'active' });
});
test('cleanup, unknown aliases and forged acknowledgement are rejected', async () => {
  const { ctx, channel, sent } = setup();
  await assert.rejects(ctx.install('unknown', body(null)), { code: 'undeclared_factory' });
  await assert.rejects(ctx.provide('unknown', body(1)), { code: 'undeclared_provision' });
  await assert.rejects(setup(true).ctx.install('worker', body(null)), { code: 'closed' });
  assert.equal(sent.length, 0);
  assert.throws(() => new Child(channel, { child: 'x', grant: 'y' }), { code: 'protocol' });
  const pending = ctx.install('worker', body(null));
  const rejected = assert.rejects(pending, { code: 'closed' });
  channel.close();
  await rejected;
  await ctx.finish();
});
test('manifest order is irrelevant; duplicates and excess fields cannot widen authority', () => {
  assert.deepEqual(
    checkComposition({
      provisions: { settings: { revision: '1', name: 'settings' } },
      factories: ['worker'],
    }),
    manifest,
  );
  assert.throws(() => checkComposition({ ...manifest, factories: ['worker', 'worker'] }));
  assert.throws(() => checkComposition({ ...manifest, extra: true } as typeof manifest));
});
