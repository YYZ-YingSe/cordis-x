import test from 'node:test';
import assert from 'node:assert/strict';
import { OperationInbox, OperationOutbox, parseOperation } from '../src/protocols/session/operations.ts';

test('a suspended request does not prevent another slot from turning over indefinitely', () => {
  const sender = new OperationOutbox(2), receiver = new OperationInbox(2);
  const slow = sender.allocate();
  receiver.admit(slow);
  for (let n = 0; n < 100_000; ++n) {
    const fast = sender.allocate(), finish = receiver.admit(fast);
    assert.throws(() => sender.allocate());
    assert.throws(() => receiver.admit(fast));
    finish();
    assert(sender.complete(fast));
    assert.throws(() => receiver.admit(fast));
    assert(!sender.complete(fast));
  }
});
test('out of order deliveries and stale completions cannot alias a new generation', () => {
  const sender = new OperationOutbox(2), receiver = new OperationInbox(2);
  const a = sender.allocate(), b = sender.allocate();
  receiver.admit(b)(); receiver.admit(a)();
  assert(sender.complete(a));
  const c = sender.allocate(), finish = receiver.admit(c);
  assert(!sender.complete(a));
  assert.throws(() => receiver.admit(a));
  assert.throws(() => receiver.admit('2:1'));
  finish();
  for (const raw of ['1', '01:1', '0:0', '0:01', '-1:1', '0:18446744073709551616'])
    assert.equal(parseOperation(raw), undefined);
  assert.deepEqual(parseOperation('0:18446744073709551615'), { slot: 0, generation: (1n << 64n) - 1n });
});
