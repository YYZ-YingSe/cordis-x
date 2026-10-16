import { request, answer, body, jsonBody, payload, type Request, type FixtureOutcome } from './binary-support.ts';
import type { Context as PublicContext } from '../src/index.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { Context, DependencyChannel, RpcError } from '../src/adapters/process/context.ts';
import { TaskRegistry } from '../src/adapters/process/task.ts';
import type { Value } from '../src/core/value.ts';

function setup() {
  const sent: Request[] = [];
  const channel = new DependencyChannel();
  channel.configure('a'.repeat(32), { incarnation: '1', fiber: '2', epoch: '3' }, async (value) => {
    sent.push(request(value));
  });
  const registry = new TaskRegistry();
  const ctx = (task?: string) =>
    new Context(
      channel,
      'b'.repeat(32),
      { logger: '1' },
      undefined,
      false,
      undefined,
      undefined,
      registry,
      task ? registry.cancellation(task) : undefined,
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
  return { sent, channel, registry, ctx, reply };
}
test('cancellation before task dispatch is sticky; task executes before registration reply and only once', async () => {
  const { registry, ctx, reply, sent } = setup();
  const creating = ctx();
  let count = 0,
    saved: PublicContext | undefined;
  const registration = creating.spawn('worker', async (worker) => {
    saved = worker;
    await worker.cancelled();
    count++;
  });
  registry.cancel('t1');
  registry.cancel('t1');
  assert.deepEqual(await registry.invoke('t1', ctx('t1')), { kind: 'success', value: null });
  reply(0, { task: 't1', grant: 'f'.repeat(32) });
  const task = await registration;
  await creating.finish();
  await assert.rejects(saved!.invoke('logger', 'write', body(null)), { code: 'closed' });
  await assert.rejects(registry.invoke('t1', ctx('t1')), { code: 'closed' });
  const join = task.join();
  assert.deepEqual(sent[1].payload.control, { task: 't1', grant: 'f'.repeat(32) });
  reply(1, null);
  await join;
  assert.equal(count, 1);
});
test('task failure remains an inner outcome and task completion drains admitted work', async () => {
  const { registry, ctx, reply } = setup();
  const registration = ctx().spawn('worker', (worker) => {
    void worker.invoke('logger', 'write', body('pending'));
    throw new RpcError('closed', 'business error');
  });
  reply(0, { task: 't1', grant: 'f'.repeat(32) });
  await registration;
  let finished = false;
  const running = registry.invoke('t1', ctx('t1')).then((outcome) => {
    finished = true;
    return outcome;
  });
  await Promise.resolve();
  assert.equal(finished, false);
  assert.equal(registry.idle, false);
  reply(1, null);
  assert.deepEqual(await running, {
    kind: 'failure',
    error: { code: 'closed', message: 'business error' },
  });
  assert.equal(registry.idle, true);
});
test('unsupported, expired and cleanup contexts reject spawn; cancellation is limited to task callbacks', async () => {
  const { channel, registry, ctx } = setup();
  await assert.rejects(
    new Context(channel, 'b'.repeat(32), {}).spawn('x', () => {}),
    { code: 'unsupported_tasks' },
  );
  await assert.rejects(
    new Context(channel, 'b'.repeat(32), {}, undefined, true, undefined, undefined, registry).spawn(
      'x',
      () => {},
    ),
    { code: 'closed' },
  );
  const context = ctx();
  await assert.rejects(context.cancelled(), { code: 'closed' });
  await context.finish();
  await assert.rejects(
    context.spawn('x', () => {}),
    { code: 'closed' },
  );
});
test('sixteen join waiters do not consume registration or dependency capacity', async () => {
  const { channel, sent, reply } = setup();
  const waits = Array.from({ length: 16 }, () => channel.request('task_join', {}));
  await assert.rejects(channel.request('task_join', {}), { code: 'overloaded' });
  const dependency = channel.invoke('context','logger','write',body(null));
  reply(16, null);
  await dependency;
  assert.equal(sent.length, 17);
  waits.forEach((_, i) => reply(i, null));
  await Promise.all(waits);
});
