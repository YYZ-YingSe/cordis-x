import assert from 'node:assert/strict';
import test from 'node:test';
import { checkValue, compareInterfaces, normalizeInterface, operation, valueInterface } from '../src/core/contract.ts';
import { runtimeSchema } from '#cordis-generated/schema';
import { counterInterface } from '../examples/resources/contract.ts';
test('structural comparison normalizes sets and locates semantic drift', () => {
    const a = valueInterface(true);
    a.capabilities = ['clock', 'log'];
    const b = structuredClone(a);
    b.capabilities.reverse();
    compareInterfaces(a, b);
    operation(a.methods, 'echo').errors.push('ordinary_only');
    assert.deepEqual(operation(operation(a.resources, 'counter').methods, 'get').errors, ['*']);
    const changed = structuredClone(counterInterface);
    operation(operation(changed.resources, 'counter').methods, 'add').args = { message: 'google.protobuf.StringValue', descriptor: runtimeSchema };
    assert.throws(() => compareInterfaces(counterInterface, changed), /resources.*counter.*add.*args/);
    assert.throws(() => compareInterfaces(counterInterface, valueInterface(true)), /interface/);
});
test('missing, duplicate, unknown and unbounded descriptors fail closed', () => {
    assert.throws(() => normalizeInterface(undefined));
    assert.throws(() => normalizeInterface({ ...counterInterface, legacy: true }));
    assert.throws(() => normalizeInterface({ ...counterInterface, capabilities: ['clock', 'clock'] }), /duplicate/);
    assert.throws(() => normalizeInterface({ ...counterInterface, capabilities: Array.from({ length: 4097 }, (_, i) => `c${i}`) }), /limit/);
    const invalid = structuredClone(counterInterface) as any;
    invalid.resources.value.counter.release = 'never';
    assert.throws(() => normalizeInterface(invalid), /lifecycle/);
    assert.throws(() => operation(counterInterface.methods, 'undeclared'), /undeclared/);
});
test('message contracts use the standard protobuf parser', () => {
    const integer = { message: 'google.protobuf.Int64Value', descriptor: runtimeSchema };
    checkValue(integer, '9223372036854775807', 'args');
    assert.throws(() => checkValue(integer, '9223372036854775808', 'args'));
    assert.throws(() => checkValue(integer, 1.5, 'args'));
    assert.throws(() => checkValue({ ...integer, message: 'missing.Type' }, 1, 'args'));
});
