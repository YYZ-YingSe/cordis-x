import test from 'node:test';
import assert from 'node:assert/strict';
import { OperationInbox, OperationOutbox, parseOperation, operationLayout } from '../src/protocols/session/operations.ts';

test('host capacity sizes lanes without erasing the admitted hello generation', () => {
  const layout = operationLayout(257);
  const sender = new OperationOutbox(layout.forward), receiver = new OperationInbox(1);
  const hello = sender.allocate(0), finish = receiver.admit(hello);
  receiver.grow(layout.incoming);
  assert.throws(() => receiver.admit(hello));
  finish();
  assert.throws(() => receiver.admit(hello));
  sender.complete(hello);
  const ordinary = Array.from({ length: 257 }, () => sender.allocate(0));
  assert.throws(() => sender.allocate(0));
  const cleanup = sender.allocate(1);
  assert.equal(parseOperation(cleanup)?.slot, 257);
  receiver.admit(cleanup)();
  assert(sender.complete(cleanup));
  for (const operation of ordinary) assert(sender.complete(operation));
  for (const capacity of [0, -1, NaN, 0xffffffff]) assert.throws(() => operationLayout(capacity));
});

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
