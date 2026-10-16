import { request, answer, body, jsonBody, payload, type Request, type FixtureOutcome } from './binary-support.ts';
import type { Context as PublicContext } from '../src/index.ts';
import test from 'node:test';
import assert from 'node:assert/strict';
import { Context, DependencyChannel, RpcError } from '../src/adapters/process/context.ts';
import { ScopeRegistry } from '../src/adapters/process/scope.ts';
import type { Value } from '../src/core/value.ts';

function setup() {
  const sent: Request[] = [];
  const channel = new DependencyChannel();
  channel.configure('a'.repeat(32), { incarnation: '1', fiber: '2', epoch: '3' }, async (value) => {
    sent.push(request(value));
  });
  const registry = new ScopeRegistry();
  const ctx = () =>
    new Context(channel, 'b'.repeat(32), { logger: '1' }, undefined, false, undefined, registry);
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

test('scope handles outlive the creating context but callback contexts expire and callbacks execute once', async () => {
  const { registry, ctx, reply, sent } = setup();
  const original = ctx();
  const creating = original.scope('connection');
  reply(0, { scope: 'c1', grant: 'f'.repeat(32) });
  const scope = await creating;
  await original.finish();
  await assert.rejects(original.scope('late'), { code: 'closed' });
  let saved: PublicContext | undefined;
  const running = scope.run((child) => {
    saved = child;
    return 42;
  });
  assert.equal(sent[1].command, 'scope_run');
  await assert.rejects(registry.invoke('c2', 'u1', ctx()), { code: 'closed' });
  assert.deepEqual(await registry.invoke('c1', 'u1', ctx()), { kind: 'success', value: 42 });
  await assert.rejects(saved!.invoke('logger', 'write', body(null)), { code: 'closed' });
  await assert.rejects(registry.invoke('c1', 'u1', ctx()), { code: 'closed' });
  reply(1, 42);
  assert.equal(await running, 42);
  const disposing = scope.dispose();
  assert.deepEqual(sent[2].payload.control, { scope: 'c1', grant: 'f'.repeat(32) });
  reply(2, {});
  await disposing;
});

test('scope callback failure is completed work and drains its admitted context work', async () => {
  const { registry, ctx, reply } = setup();
  const creating = ctx().scope('connection');
  reply(0, { scope: 'c1', grant: 'f'.repeat(32) });
  const scope = await creating;
  const running = scope.run((child) => {
    void child.invoke('logger', 'write', body('pending'));
    throw new RpcError('closed', 'business error');
  });
  let settled = false;
  const callback = registry.invoke('c1', 'u1', ctx()).then((outcome) => {
    settled = true;
    return outcome;
  });
  await Promise.resolve();
  assert.equal(settled, false);
  assert.equal(registry.idle, false);
  reply(2, null);
  assert.deepEqual(await callback, {
    kind: 'failure',
    error: { code: 'closed', message: 'business error' },
  });
  assert.equal(registry.idle, true);
  reply(1, null);
  await running;
});

test('unsupported and cleanup contexts cannot create scopes; lost run acknowledgements do not replay callbacks', async () => {
  const { registry, channel, ctx, reply } = setup();
  await assert.rejects(new Context(channel, 'b'.repeat(32), {}).scope('unsupported'), {
    code: 'unsupported_scopes',
  });
  await assert.rejects(
    new Context(channel, 'b'.repeat(32), {}, undefined, true, undefined, registry).scope('cleanup'),
    { code: 'closed' },
  );
  const creating = ctx().scope('connection');
  reply(0, { scope: 'c1', grant: 'f'.repeat(32) });
  const scope = await creating;
  let count = 0;
  const running = assert.rejects(
    scope.run(() => {
      count++;
      return null;
    }),
    { code: 'closed' },
  );
  await registry.invoke('c1', 'u1', ctx());
  channel.close();
  await running;
  await assert.rejects(registry.invoke('c1', 'u1', ctx()), { code: 'closed' });
  assert.equal(count, 1);
});
