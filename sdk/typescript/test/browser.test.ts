import test from 'node:test';
import assert from 'node:assert/strict';
import { MessageChannel } from 'node:worker_threads';
import { portConnection, type BinaryPort } from '../src/adapters/browser.ts';
import { encodeFrame } from '../src/protocols/session/binary.ts';
import { payload } from '../src/protocols/session/messages.ts';

test('browser carrier enforces complete-frame admission without Node byte helpers', async () => {
  const channel = new MessageChannel();
  const connection = portConnection(channel.port1 as unknown as BinaryPort, 4096);
  const identity = { incarnation: '1', fiber: '1', epoch: '1' };
  const frame = { session: '0'.repeat(32), activation: identity, operation: '0:1', command: 'hello', payload: payload({}) };
  const receive = connection.receive(() => 'contract', () => 1024)[Symbol.asyncIterator]();
  channel.port2.postMessage(encodeFrame(frame, 1024));
  const next = await receive.next();
  assert.deepEqual(next.value, frame);
  connection.close(); channel.port2.close();
  assert.equal((await receive.next()).done, true);
});

test('a malformed browser message fails the carrier rather than inventing a business refusal', async () => {
  const channel = new MessageChannel();
  const connection = portConnection(channel.port1 as unknown as BinaryPort, 4096);
  const receive = connection.receive(() => 'checked', () => 1024)[Symbol.asyncIterator]();
  channel.port2.postMessage({ forged: true });
  await assert.rejects(receive.next(), /invalid Session carrier/);
  channel.port2.close();
});
