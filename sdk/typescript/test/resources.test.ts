import { body, jsonBody } from './binary-support.ts';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { ResourceRecord } from '../src/adapters/process/resources.ts';

test('resource close waits for admitted calls and repeated close shares one result', async () => {
  const gate = Promise.withResolvers<Uint8Array>();
  let closed = 0;
  const entry = new ResourceRecord('owner', {
    invoke: () => gate.promise,
    close: () => {
      closed++;
    },
  });
  const call = entry.invoke('hold', body(null));
  const first = entry.close();
  const second = entry.close();
  assert.equal(first, second);
  assert.equal(closed, 0);
  await assert.rejects(entry.invoke('late', body(null)));
  gate.resolve(body(42));
  assert.deepEqual(await call, body(42));
  await first;
  assert.equal(closed, 1);
  assert.equal(entry.released, true);
});
test('a partially failed close is retained and never retried automatically', async () => {
  let attempts = 0;
  const entry = new ResourceRecord('owner', {
    invoke: () => body(42),
    close: () => {
      attempts++;
      throw new Error('partial inverse');
    },
  });
  await assert.rejects(entry.close());
  await assert.rejects(entry.close());
  assert.equal(attempts, 1);
  assert.equal(entry.released, false);
});
