import { request, answer, body, jsonBody, payload, type Request, type FixtureOutcome } from './binary-support.ts';
import type { Context as PublicContext } from '../src/index.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { Context, DependencyChannel, RpcError } from '../src/adapters/process/context.ts';
import { Inverse, iterate, SequenceRegistry } from '../src/adapters/process/sequence.ts';
import type { Value } from '../src/core/value.ts';

function setup() {
  const sent: Request[] = [];
  const channel = new DependencyChannel();
  channel.configure(
    'a'.repeat(32),
    { incarnation: '1', fiber: '2', epoch: '3' },
    async (bytes) => {
      sent.push(request(bytes));
    },
  );
  const registry = new SequenceRegistry();
  const ctx = () =>
    new Context(channel, 'b'.repeat(32), { logger: '1' }, undefined, false, registry);
  const reply = (index: number, value: Value) => {
    const { session, activation, operation } = sent[index];
    channel.receive({
      session,
      activation,
      operation,
      outcome: { kind: 'success', value: answer(sent[index] ,value) },
    });
  };
  return { channel, registry, ctx, sent, reply };
}
test('generator gets fresh step contexts and its final return inverse is retained', async () => {
  const { registry, channel, ctx, reply, sent } = setup();
  const events: string[] = [];
  let old: PublicContext;
  const original = ctx();
  const registering = original.effect(
    'generator',
    iterate(async function* (step) {
      old = step;
      step = yield new Inverse('first', () => {
        events.push('first');
      });
      assert.notEqual(step, old);
      await assert.rejects(old.invoke('logger', 'write', body(null)), { code: 'closed' });
      return new Inverse('last', () => {
        events.push('last');
      });
    }),
  );
  reply(0, { sequence: 's1', grant: 'f'.repeat(32) });
  const sequence = await registering;
  await original.finish();
  await assert.rejects(
    original.effect('late', () => ({ done: true })),
    { code: 'closed' },
  );
  assert.deepEqual(await registry.next('s1', '1', ctx()), {
    done: false,
    inverse: 'first',
    error: null,
  });
  assert.deepEqual(await registry.next('s1', '2', ctx()), {
    done: true,
    inverse: 'last',
    error: null,
  });
  assert.throws(() => registry.release('s1'), { code: 'effects_remaining' });
  await registry.undo('s1', '2', ctx());
  await registry.undo('s1', '2', ctx());
  await registry.undo('s1', '1', ctx());
  registry.release('s1');
  registry.release('s1');
  assert.deepEqual(events, ['last', 'first']);
  assert.equal(registry.released, true);
  const ready = sequence.ready();
  assert.equal(sent[1].command, 'sequence_ready');
  reply(1, { completion: 'completed' });
  assert.equal(await ready, 'completed');
  const disposing = sequence.dispose();
  assert.equal(sent[2].command, 'sequence_dispose');
  reply(2, {});
  await disposing;
  channel.close();
});
test('failed step can return an inverse; failed cleanup is cached and prevents producer release', async () => {
  const { registry, ctx, reply } = setup();
  let calls = 0;
  const registering = ctx().effect('failure', () => ({
    done: true,
    error: new RpcError('forward', 'failed'),
    inverse: new Inverse('partial', () => {
      calls++;
      throw new RpcError('cleanup', 'failed');
    }),
  }));
  reply(0, { sequence: 's1', grant: 'f'.repeat(32) });
  await registering;
  const step = await registry.next('s1', '1', ctx());
  assert.deepEqual(step, {
    done: true,
    inverse: 'partial',
    error: { code: 'forward', message: 'failed' },
  });
  for (let i = 0; i < 2; i++)
    await assert.rejects(registry.undo('s1', '1', ctx()), { code: 'cleanup' });
  assert.equal(calls, 1);
  assert.throws(() => registry.release('s1'), { code: 'effects_remaining' });
});
test('registration loss retains producer and a never-started sequence can be released', async () => {
  const { registry, channel, ctx } = setup();
  let steps = 0;
  const registering = assert.rejects(
    ctx().effect('unknown', () => {
      steps++;
      return { done: true };
    }),
    { code: 'closed' },
  );
  channel.close();
  await registering;
  assert.equal(registry.released, false);
  registry.release('s1');
  assert.equal(steps, 0);
  assert.equal(registry.released, true);
});
