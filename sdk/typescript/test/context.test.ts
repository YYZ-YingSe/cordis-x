import { request, answer, body, jsonBody, payload, type Request, type FixtureOutcome } from './binary-support.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { Context, DependencyChannel, RpcError } from '../src/adapters/process/context.ts';
import { Refusal } from '../src/index.ts';
import type { Value } from '../src/core/value.ts';

function setup() {
  const sent: Request[] = [];
  const channel = new DependencyChannel();
  const identity = { incarnation: '1', fiber: '2', epoch: '3' };
  channel.configure('a'.repeat(32), identity, async (value) => {
    sent.push(request(value));
  });
  const context = new Context(channel, 'b'.repeat(32), { logger: '1' });
  const response = (index: number, outcome: FixtureOutcome) => {
    const { session, activation, operation } = sent[index];
    return { session, activation, operation, outcome: outcome.kind === 'success' ? {kind:'success' as const,value:answer(sent[index],outcome.value)} : outcome };
  };
  return { sent, channel, context, response };
}

test('metadata requires negotiation, drains the callback and expires with its Context', async () => {
  const { sent, channel, context, response } = setup();
  await assert.rejects(context.metadata('logger', '1'), { code: 'unsupported_metadata' });
  assert.equal(sent.length, 0);
  channel.enableMetadata(true);
  const read = context.metadata('logger', '1');
  assert.equal(sent[0].command, 'context_metadata');
  assert.deepEqual(sent[0].payload.control, { context: 'b'.repeat(32), name: 'logger', revision: '1' });
  let done = false;
  const finish = context.finish().then(() => {
    done = true;
  });
  await Promise.resolve();
  assert.equal(done, false);
  const value = { revision: '18446744073709551615', layers: [{ timeout: 3000 }] };
  channel.receive(response(0, { kind: 'success', value }));
  assert.deepEqual(await read, value);
  await finish;
  await assert.rejects(context.metadata('logger', '1'), { code: 'closed' });
});

test('metadata cleanup retains its reserved lane when ordinary reverse calls are full', async () => {
  const { sent, channel, context, response } = setup();
  channel.enableMetadata(true);
  const pending = Array.from({ length: 16 }, () => context.metadata('logger', '1'));
  await assert.rejects(context.metadata('logger', '1'), { code: 'overloaded' });
  const cleanup = new Context(channel, 'c'.repeat(32), {}, undefined, true);
  const read = cleanup.metadata('logger', '1');
  assert.equal(sent[16].command, 'cleanup_metadata');
  channel.receive(response(16, { kind: 'success', value: { revision: '1', layers: [] } }));
  await read;
  await cleanup.finish();
  const failures = pending.map((p) => assert.rejects(p, { code: 'closed' }));
  channel.close();
  await Promise.all(failures);
  await context.finish();
});

test('dependency replies correlate independently; context finish drains accepted work and rejects new work', async () => {
  const { sent, channel, context, response } = setup();
  const first = context.invoke('logger', 'write', body('first'));
  const second = context.invoke('logger', 'write', body('second'));
  assert.equal(sent.length, 2);
  let finished = false;
  const draining = context.finish().then(() => {
    finished = true;
  });
  await assert.rejects(context.invoke('logger', 'write', body('late')), { code: 'closed' });
  channel.receive(response(1, { kind: 'success', value: 2 }));
  assert.deepEqual(await second, body(2));
  assert.equal(finished, false);
  channel.receive(response(0, { kind: 'success', value: 1 }));
  assert.deepEqual(await first, body(1));
  await draining;
  assert.equal(finished, true);
});

test('unknown dependencies stay local and remote business errors retain their code', async () => {
  const { sent, channel, context, response } = setup();
  await assert.rejects(context.invoke('missing', 'write', body(null)), { code: 'undeclared_dependency' });
  assert.equal(sent.length, 0);
  const call = context.invoke('logger', 'write', body(null));
  const rejected = assert.rejects(call, { code: 'write_failed', message: 'disk full' });
  channel.receive(
    response(0, { kind: 'failure', error: { code: 'write_failed', message: 'disk full' } }),
  );
  await rejected;
  await context.finish();
});

test('invalid or duplicate replies are rejected and channel loss wakes all pending calls', async () => {
  const { channel, context, response } = setup();
  const call = context.invoke('logger', 'write', body(null));
  const valid = response(0, { kind: 'success', value: null });
  assert.throws(() => channel.receive({ ...valid, session: 'c'.repeat(32) }), /identity mismatch/);
  channel.receive(valid);
  await call;
  assert.throws(() => channel.receive(valid), /unsolicited/);
  const pending = context.invoke('logger', 'write', body(null));
  const rejected = assert.rejects(
    pending,
    (error: unknown) => error instanceof Error && !(error instanceof RpcError),
  );
  channel.close();
  await rejected;
  await context.finish();
});

test('resource release waits for dependency calls started by a resource method', async () => {
  const { ResourceRecord } = await import('../src/adapters/process/resources.ts');
  const { channel, context, response } = setup();
  let closed = false;
  const resource = new ResourceRecord('owner', {
    invoke() {
      void context.invoke('logger', 'write', body('detached'));
      return body(null);
    },
    close() {
      closed = true;
    },
  });
  const invoking = resource.invoke('write',body(null),context);
  const releasing = resource.close();
  await Promise.resolve();
  assert.equal(closed, false);
  channel.receive(response(0, { kind: 'success', value: null }));
  await invoking;
  await releasing;
  assert.equal(closed, true);
});

test('defer drains registration, survives acknowledgement races, and yields an independent handle', async () => {
  const { EffectRegistry } = await import('../src/adapters/process/context.ts');
  const { channel, sent, response } = setup();
  const effects = new EffectRegistry();
  const ctx = new Context(channel, 'c'.repeat(32), {}, effects);
  let releases = 0;
  const registration = ctx.defer('subscription', () => {
    releases++;
  });
  assert.equal(sent[0].command, 'effect_register');
  let finished = false;
  const closing = ctx.finish().then(() => {
    finished = true;
  });
  await assert.rejects(
    ctx.defer('late', () => {}),
    { code: 'closed' },
  );
  const cleanup = new Context(channel, 'd'.repeat(32), {}, effects, true);
  await assert.rejects(
    cleanup.defer('nested', () => {}),
    { code: 'closed' },
  );
  // Release may reach the host before its registration reply does.
  await effects.release('e1', cleanup);
  assert.equal(finished, false);
  channel.receive(response(0, { kind: 'success', value: { effect: 'e1', grant: 'f'.repeat(32) } }));
  const effect = await registration;
  await closing;
  const disposing = effect.dispose();
  assert.equal(sent[1].command, 'effect_dispose');
  assert.deepEqual(sent[1].payload.control, { effect: 'e1', grant: 'f'.repeat(32) });
  await effects.release('e1', cleanup);
  channel.receive(response(1, { kind: 'success', value: {} }));
  await disposing;
  assert.equal(releases, 1);
  assert.equal(effects.released, true);
});

test('effect cleanup drains dependency calls and caches a failed inverse without retrying', async () => {
  const { EffectRegistry } = await import('../src/adapters/process/context.ts');
  const { channel, response } = setup();
  const effects = new EffectRegistry();
  const ctx = new Context(channel, 'c'.repeat(32), {}, effects);
  let releases = 0;
  const registration = ctx.defer('failed', (cleanup) => {
    releases++;
    void cleanup.invoke('logger', 'write', body('cleanup'));
    throw new RpcError('cleanup_failed', 'failed inverse');
  });
  channel.receive(response(0, { kind: 'success', value: { effect: 'e1', grant: 'f'.repeat(32) } }));
  await registration;
  await ctx.finish();
  const cleanup = new Context(channel, 'd'.repeat(32), { logger: '1' }, effects, true);
  let settled = false;
  const failed = assert
    .rejects(effects.release('e1', cleanup), { code: 'cleanup_failed' })
    .then(() => {
      settled = true;
    });
  const repeated = assert.rejects(effects.release('e1', cleanup), { code: 'cleanup_failed' });
  await Promise.resolve();
  assert.equal(settled, false);
  channel.receive(response(1, { kind: 'success', value: null }));
  await Promise.all([failed, repeated]);
  assert.equal(releases, 1);
  assert.equal(effects.released, false);
});

test('unconfirmed registrations retain callbacks and negotiation is required', async () => {
  const { EffectRegistry } = await import('../src/adapters/process/context.ts');
  const { channel, context } = setup();
  await assert.rejects(
    context.defer('unsupported', () => {}),
    { code: 'unsupported_effects' },
  );
  const effects = new EffectRegistry();
  const ctx = new Context(channel, 'c'.repeat(32), {}, effects);
  const registration = assert.rejects(
    ctx.defer('unknown', () => {}),
    { code: 'closed' },
  );
  channel.close();
  await registration;
  await ctx.finish();
  assert.equal(effects.released, false);
});

test('business and runtime errors with identical codes keep distinct categories', async () => {
  const { channel, context, response } = setup();
  for (const kind of ['failure', 'refused'] as const) {
    const pending = context.invoke('logger', 'write', body(null));
    const assertion = assert.rejects(pending, (error: unknown) =>
      error instanceof RpcError && error.code === 'closed' &&
      (error instanceof Refusal) === (kind === 'refused'));
    channel.receive(response(kind === 'failure' ? 0 : 1, {kind, error: {code: 'closed', message: 'same code'}}));
    await assertion;
  }
  await context.finish();
});
