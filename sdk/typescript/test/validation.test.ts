import test from 'node:test';
import assert from 'node:assert/strict';
import { clone, create, createFileRegistry, setExtension, toBinary } from '@bufbuild/protobuf';
import { AddArgsSchema } from '@cordis-x/example-contracts/examples/messages/examples';
import { FieldOptionsSchema, FileDescriptorProtoSchema, ValueSchema } from '@bufbuild/protobuf/wkt';
import { runtimeSchema } from '#cordis-generated/schema';
import { parseBinary, protobuf, validateMessage } from '../src/core/protobuf.ts';
import { validation } from '../src/core/validation.ts';
import { ConfigSchema, DataSchema, LinkASchema, file_profile } from '@cordis-x/test-contracts/profile/messages/profile';
import { exact_size, maximum } from '@cordis-x/test-contracts/profile/messages/cordis/options';

test('field plans isolate same-name descriptor identities and mutable values', () => {
  const file = clone(FileDescriptorProtoSchema, file_profile.proto);
  const field = file.messageType.find(message => message.name === 'Config')!.field[0];
  setExtension(field.options!, maximum, 5n);
  const other = createFileRegistry(file, name => file_profile.dependencies.find(dependency => dependency.proto.name === name)).getMessage('profile.Config')!;
  const ordinary = create(ConfigSchema, {limit: 42});
  validateMessage(ConfigSchema, ordinary, 'checked');
  assert.throws(() => validateMessage(other, create(other, {limit: 42}), 'checked'), /range/);
  validateMessage(other, create(other, {limit: 3}), 'checked');
  validateMessage(ConfigSchema, ordinary, 'checked');
  ordinary.limit = 0;
  assert.throws(() => validateMessage(ConfigSchema, ordinary, 'checked'), /range/);
  assert.doesNotThrow(() => validateMessage(ConfigSchema, ordinary, 'contract'));
});

test('compiled recursive plans preserve byte, node and depth budgets', () => {
  const links = create(LinkASchema, {next: {next: {}}});
  validateMessage(LinkASchema, links, 'audit');
  const data = create(DataSchema, {int32Value: 0, option: {state: {case:'none', value:0}}, binary: new Uint8Array(1024*1024)});
  validateMessage(DataSchema, data, 'checked');
  data.text = 'x';
  assert.throws(() => validateMessage(DataSchema, data, 'checked'), /byte limit/);
  data.binary = new Uint8Array(); data.text = '';
  data.flags = Array(65536).fill(false);
  assert.throws(() => validateMessage(DataSchema, data, 'checked'), /node limit/);
  data.flags = [];
  data.tree = {$typeName:'profile.Tree', label:'', children:[]};
  let tree = data.tree;
  for (let i=0;i<64;i++) { const child = {$typeName:'profile.Tree' as const, label:'', children:[]}; tree.children.push(child); tree = child; }
  assert.throws(() => validateMessage(DataSchema, data, 'checked'), /depth\/node limit/);
});

test('fixed strings count Unicode code points and still reject malformed strings', () => {
  const file = clone(FileDescriptorProtoSchema, file_profile.proto);
  const field = file.messageType.find(message => message.name === 'Data')!.field.find(field => field.name === 'text')!;
  field.options ??= create(FieldOptionsSchema);
  setExtension(field.options, exact_size, 2);
  const schema = createFileRegistry(file, name => file_profile.dependencies.find(dependency => dependency.proto.name === name)).getMessage('profile.Data')!;
  const message = (text: string) => create(schema, {int32Value:0, option:{state:{case:'none',value:0}}, text});
  for (const text of ['a🦀', 'a\u0301']) validateMessage(schema, message(text), 'checked');
  for (const text of ['🦀', 'abc']) assert.throws(() => validateMessage(schema, message(text), 'checked'), /size/);
  assert.throws(() => validateMessage(schema, message('a\ud800'), 'checked'), /Unicode/);
});

test('contract promises, checked fields, audit unknowns and mutable JS aliases', () => {
  const codec = protobuf(AddArgsSchema);
  const missing = create(AddArgsSchema, {left: 1n});
  assert.doesNotThrow(() => codec.encode(missing, 'contract'));
  for (const policy of ['checked', 'audit'] as const)
    assert.throws(() => codec.encode(missing, policy), /required/);
  const valid = create(AddArgsSchema, {left: 1n, right: 2n});
  const bytes = toBinary(AddArgsSchema, valid);
  const unknown = Uint8Array.from([...bytes, 0x78, 1]);
  for (const policy of ['contract', 'checked'] as const)
    assert.equal(codec.decode(unknown, policy).right, 2n);
  assert.throws(() => codec.decode(unknown, 'audit'), /undeclared/);
  for (const policy of ['contract', 'checked', 'audit'] as const)
    assert.throws(() => codec.decode(Uint8Array.from([0x08, 0x80]), policy));
  assert.doesNotThrow(() => codec.encode(valid, 'checked'));
  valid.right = undefined;
  assert.throws(() => codec.encode(valid, 'checked'), /required/, 'mutable objects never retain stale validation proofs');
  const extra = {...create(AddArgsSchema, {left: 1n, right: 2n}), extra: true};
  assert.doesNotThrow(() => codec.encode(extra, 'checked'));
  assert.throws(() => codec.encode(extra, 'audit'), /undeclared native/);
  assert.throws(() => validation('peer-validated'));
});

test('audit sees original map entries and overwritten oneofs in typed and opaque business bytes', () => {
  const codec = protobuf(ValueSchema);
  const shape = { message: ValueSchema.typeName, descriptor: runtimeSchema };
  const bodies = [
    [42, 11, 10, 9, 10, 1, 107, 18, 2, 8, 0, 120, 120],
    [42, 2, 120, 120, 8, 0],
  ];
  for (const body of bodies) {
    const bytes = Uint8Array.from(body);
    for (const policy of ['contract', 'checked'] as const) {
      assert.doesNotThrow(() => codec.decode(bytes, policy));
      assert.doesNotThrow(() => parseBinary(shape, bytes, policy));
    }
    assert.throws(() => codec.decode(bytes, 'audit'), /undeclared protobuf field/);
    assert.throws(() => parseBinary(shape, bytes, 'audit'), /undeclared protobuf field/);
  }
  assert.doesNotThrow(() => codec.decode(Uint8Array.from([42, 9, 10, 7, 10, 1, 107, 18, 2, 8, 0]), 'audit'));
});
