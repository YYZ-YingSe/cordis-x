import test from 'node:test';
import assert from 'node:assert/strict';
import { DependencyChannel } from '../src/protocols/session/channel.ts';
import { decodeFrame } from '../src/protocols/session/binary.ts';
import { Task } from '../src/adapters/session/task.ts';
import type { Request, Response, CancelObservation } from '../src/protocols/session/messages.ts';

function setup() {
  const sent: (Request | Response | CancelObservation)[] = [];
  const channel = new DependencyChannel();
  channel.configure('a'.repeat(32), { incarnation: '1', fiber: '2', epoch: '3' }, async bytes => {
    sent.push(decodeFrame(bytes));
  });
  const requests = () => sent.filter((f): f is Request => 'command' in f);
  const reply = (request: Request, refused = false) => channel.receive({
    session: request.session, activation: request.activation, operation: request.operation,
    outcome: refused
      ? { kind: 'refused', error: { code: 'observation_cancelled', message: 'withdrawn' } }
      : { kind: 'success', value: { control: null, messages: [] } },
  });
  return { channel, sent, requests, reply };
}

test('one cancelled join does not cancel a sibling or poison the terminal cache', async () => {
  const { channel, sent, requests, reply } = setup();
  const task = new Task(channel, 't1', 'b'.repeat(32));
  const stop = new AbortController();
  const first = task.join(stop.signal);
  const second = task.join();
  assert.equal(requests().length, 2);
  stop.abort();
  const cancellation = sent.at(-1)!;
  assert.ok('cancelObservation' in cancellation);
  assert.equal(cancellation.operation, requests()[0].operation);
  let finished = false;
  first.catch(() => { finished = true; });
  await Promise.resolve();
  assert.equal(finished, false, 'withdrawal must await original acknowledgement');
  reply(requests()[0], true);
  await assert.rejects(first, { code: 'observation_cancelled' });
  reply(requests()[1]);
  await second;
  await task.join();
  assert.equal(requests().length, 2, 'delivered completion can be cached');
});

test('withdrawal works with every join slot occupied and reuses no operation identity', async () => {
  const { channel, sent, requests, reply } = setup();
  const stops = Array.from({ length: 16 }, () => new AbortController());
  const pending = stops.map(stop => channel.request('task_join', { task: 't1', grant: 'b'.repeat(32) }, stop.signal));
  const settled = Promise.allSettled(pending);
  await assert.rejects(channel.request('task_join', {}), { code: 'overloaded' });
  stops.forEach(stop => stop.abort());
  assert.equal(sent.filter(frame => 'cancelObservation' in frame).length, 16);
  requests().forEach(request => reply(request, true));
  assert.ok((await settled).every(result => result.status === 'rejected'));
  const again = channel.request('task_join', { task: 't1', grant: 'b'.repeat(32) });
  const request = requests().at(-1)!;
  assert.notEqual(request.operation, requests()[0].operation);
  reply(request);
  await again;
});

test('pre-cancelled queries are not submitted and business operations cannot request withdrawal', async () => {
  const { channel, sent } = setup();
  const stop = new AbortController();
  stop.abort();
  await assert.rejects(channel.request('child_ready', {}, stop.signal), { code: 'observation_cancelled' });
  await assert.rejects(channel.request('task_register', {}, new AbortController().signal), { code: 'invalid_observation' });
  assert.equal(sent.length, 0);
});
