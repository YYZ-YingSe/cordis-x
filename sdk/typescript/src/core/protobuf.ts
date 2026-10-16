import { auditBinary } from './protobuf-wire.ts';
import type { Validation } from './validation.ts';
/** Standard protobuf messages plus bounded Cordis business validation. */
import { fromBinary, fromJson, toJson, toBinary, createFileRegistry, getOption, hasOption, ScalarType, type DescMessage, type DescFile, type DescExtension, type AnyDesc, type Message, type MessageShape, type DescField, type DescOneof } from '@bufbuild/protobuf';
import * as wkt from '@bufbuild/protobuf/wkt';
const { FileDescriptorSetSchema } = wkt;
const standardFiles = new Map(Object.values(wkt).filter((v): v is DescFile => typeof v === 'object' && v !== null && 'kind' in v && v.kind === 'file').map(f => [f.proto.name, f]));
import { reflect, isReflectMessage, type ReflectMessage, type ReflectList, type ReflectMap } from '@bufbuild/protobuf/reflect';
import type { MessageContract } from './contract.ts';
import type { Value } from './value.ts';
const MAX_BYTES = 1024 * 1024, MAX_NODES = 65536, MAX_DEPTH = 64;
const resolved = new WeakMap<MessageContract, {
    name: string;
    descriptor: string;
    message: DescMessage;
}>();
const extensions = new WeakMap<DescFile, Map<string, DescExtension>>();
function extension(file: DescFile, name: string): DescExtension | undefined {
    let all = extensions.get(file);
    if (!all) {
        all = new Map();
        const seen = new Set<DescFile>();
        const visit = (f: DescFile) => { if (seen.has(f))
            return; seen.add(f); for (const e of f.extensions)
            all!.set(e.typeName, e); for (const dep of f.dependencies)
            visit(dep); };
        visit(file);
        extensions.set(file, all);
    }
    return all.get('cordis.schema.' + name);
}
function annotation(desc: AnyDesc, file: DescFile, name: string): unknown {
    const option = extension(file, name);
    return option && hasOption(desc, option) ? getOption(desc, option) : undefined;
}
interface FieldPlan {
    readonly field: DescField;
    readonly required: boolean;
    readonly explicit: boolean;
    readonly exact: number | undefined;
    readonly minimum: bigint | undefined;
    readonly maximum: bigint | undefined;
    readonly enumValues: ReadonlySet<number> | undefined;
    readonly child: number | undefined;
}
interface MessagePlan {
    readonly fields: readonly FieldPlan[];
    readonly selected: readonly DescOneof[];
    readonly names: ReadonlySet<string>;
}
// Only immutable schema metadata is cached. DTOs and Uint8Array aliases are
// checked on every requested delivery, and are never keys in this cache.
const plans = new WeakMap<DescMessage, readonly MessagePlan[]>();
function messagePlan(root: DescMessage): readonly MessagePlan[] {
    const existing = plans.get(root);
    if (existing) return existing;
    const pending = [root], indexes = new Map<DescMessage, number>([[root, 0]]);
    const nodes: MessagePlan[] = [];
    // Iteration terminates on recursive schemas: each descriptor is queued once.
    for (let i = 0; i < pending.length; i++) {
        const schema = pending[i], sem = annotation(schema, schema.file, 'semantics');
        const selected = schema.oneofs.filter(oneof => sem === 1 || sem === 2 || sem === 3 ||
            schema.typeName === 'google.protobuf.Value' || annotation(oneof, schema.file, 'selected') === true);
        const fields = schema.fields.map(field => {
            let child: number | undefined;
            if (field.message) {
                child = indexes.get(field.message);
                if (child === undefined) {
                    child = pending.length;
                    indexes.set(field.message, child); pending.push(field.message);
                }
            }
            const lo = annotation(field, schema.file, 'minimum'), hi = annotation(field, schema.file, 'maximum');
            return { field, child, required: annotation(field, schema.file, 'required') === true,
                explicit: field.presence !== 2,
                exact: annotation(field, schema.file, 'exact_size') as number | undefined,
                minimum: lo === undefined ? undefined : BigInt(lo as bigint),
                maximum: hi === undefined ? undefined : BigInt(hi as bigint),
                enumValues: field.enum ? new Set(field.enum.values.map(value => value.number)) : undefined };
        });
        nodes.push({ fields, selected, names: new Set(['$typeName', '$unknown', ...schema.members.map(m => m.localName)]) });
    }
    plans.set(root, nodes);
    return nodes;
}
export function messageType(contract: MessageContract): DescMessage {
    const cached = resolved.get(contract);
    if (cached?.name === contract.message && cached.descriptor === contract.descriptor)
        return cached.message;
    if (contract.descriptor.length > 1398104)
        throw new Error('schema byte limit exceeded');
    const bytes = Buffer.from(contract.descriptor, 'base64');
    if (bytes.length > MAX_BYTES || bytes.toString('base64') !== contract.descriptor)
        throw new Error('invalid schema descriptor');
    const files = fromBinary(FileDescriptorSetSchema, bytes).file;
    let registry = createFileRegistry();
    for (const file of files) {
        const next = createFileRegistry(file, name => registry.getFile(name) ?? standardFiles.get(name));
        registry = createFileRegistry(registry, next);
    }
    const message = registry.getMessage(contract.message);
    if (!message || ['cordis.data.Lease', 'cordis.data.Child'].includes(message.typeName))
        throw new Error('unknown or non-data message type');
    resolved.set(contract, { name: contract.message, descriptor: contract.descriptor, message });
    return message;
}
export function validateMessage<Desc extends DescMessage>(schema: Desc, value: MessageShape<Desc>, policy: Validation = 'audit', decoded = false): void {
    if (policy === 'contract') return;
    const plan = messagePlan(schema);
    let budget = MAX_NODES, remainingBytes = MAX_BYTES;
    const active = new Set<Message>();
    const take = (depth: number) => { if (depth > MAX_DEPTH || --budget < 0)
        throw new Error('message depth/node limit exceeded'); };
    const leaf = (step: FieldPlan, value: unknown, depth: number) => {
        const field = step.field;
        if (isReflectMessage(value)) {
            visit(value, depth, step.child!);
            return;
        }
        take(depth);
        if (typeof value === 'string')
            remainingBytes -= Buffer.byteLength(value);
        else if (value instanceof Uint8Array)
            remainingBytes -= value.byteLength;
        if (remainingBytes < 0)
            throw new Error('message byte limit exceeded');
        if (step.enumValues && !step.enumValues.has(value as number))
            throw new Error('undeclared enum value');
        if (field.scalar === ScalarType.FLOAT || field.scalar === ScalarType.DOUBLE) {
            if (typeof value !== 'number' || !Number.isFinite(value) || field.scalar === ScalarType.FLOAT && !Number.isFinite(Math.fround(value)))
                throw new Error('non-finite float');
        }
        if (!decoded && field.scalar === ScalarType.STRING && (typeof value !== 'string' || !value.isWellFormed()))
            throw new Error('invalid Unicode string');
    };
    const visit = (message: ReflectMessage, depth: number, index: number) => {
        const node = plan[index];
        take(depth);
        if (active.has(message.message))
            throw new Error('cyclic object graph');
        active.add(message.message);
        if (policy === 'audit' && message.getUnknown()?.length)
            throw new Error('undeclared protobuf field');
        if (policy === 'audit') {
            for (const name of Object.keys(message.message))
                if (!node.names.has(name)) throw new Error('undeclared native message field: ' + name);
        }
        for (const oneof of node.selected)
            if (!message.oneofCase(oneof))
                throw new Error('no branch selected');
        for (const step of node.fields) {
            const field = step.field;
            if (step.required && !message.isSet(field))
                throw new Error('required field missing: ' + field.name);
            if (step.explicit && !message.isSet(field))
                continue;
            const value = message.get(field);
            if (step.exact !== undefined) {
                let size: number;
                if (typeof value === 'string') {
                    size = 0;
                    for (const _ of value) size++;
                } else size = value instanceof Uint8Array ? value.length : (value as ReflectList | ReflectMap).size;
                if (size !== step.exact)
                    throw new Error('fixed size mismatch: ' + field.name);
            }
            const lo = step.minimum, hi = step.maximum;
            if (lo !== undefined || hi !== undefined) {
                const n = BigInt(value as number | bigint);
                if (lo !== undefined && n < lo || hi !== undefined && n > hi)
                    throw new Error('integer outside declared range');
            }
            if (field.fieldKind === 'list') {
                for (const child of value as ReflectList)
                    leaf(step, child, depth + 1);
            }
            else if (field.fieldKind === 'map') {
                for (const [key, child] of value as ReflectMap) {
                    take(depth + 1);
                    take(depth + 2);
                    if (!decoded && typeof key === 'string' && !key.isWellFormed())
                        throw new Error('invalid map key');
                    if (typeof key === 'string') {
                        remainingBytes -= Buffer.byteLength(key);
                        if (remainingBytes < 0)
                            throw new Error('message byte limit exceeded');
                    }
                    leaf(step, child, depth + 2);
                }
            }
            else
                leaf(step, value, depth + 1);
        }
        active.delete(message.message);
    };
    visit(reflect(schema, value), 0, 0);
}
function jsonBudget(value: unknown, depth = 0, budget = { nodes: MAX_NODES }) {
    if (depth > MAX_DEPTH || --budget.nodes < 0)
        throw new Error('JSON depth/node limit exceeded');
    if (value && typeof value === 'object')
        for (const child of Object.values(value))
            jsonBudget(child, depth + 1, budget);
}
export function parseMessage(contract: MessageContract, value: Value): Message {
    return parse(messageType(contract), value);
}
function jsonPrecision(schema: DescMessage, value: Value, depth = 0): void {
    if (depth > MAX_DEPTH)
        throw new Error('message nesting limit exceeded');
    const scalar = (type: ScalarType | undefined, value: Value) => { if (type !== undefined && [ScalarType.INT64, ScalarType.SINT64, ScalarType.SFIXED64, ScalarType.UINT64, ScalarType.FIXED64].includes(type) && typeof value === 'number' && Math.abs(value) > Number.MAX_SAFE_INTEGER)
        throw new Error('int64/uint64 outside the JSON safe range must use a decimal string'); };
    if (schema.typeName.startsWith('google.protobuf.')) {
        if (schema.file.proto.name === 'google/protobuf/wrappers.proto') {
            if (value === null)
                throw new Error('scalar message requires a value');
            scalar(schema.fields[0].scalar, value);
        }
        return;
    }
    if (!value || typeof value !== 'object' || Array.isArray(value))
        return;
    for (const field of schema.fields) {
        const input = value[field.jsonName] ?? value[field.name];
        if (input === undefined || input === null)
            continue;
        const values = field.fieldKind === 'map' ? typeof input === 'object' && !Array.isArray(input) ? Object.values(input) : [] : field.fieldKind === 'list' ? Array.isArray(input) ? input : [] : [input];
        for (const item of values) {
            scalar(field.scalar, item);
            if (field.message)
                jsonPrecision(field.message, item, depth + 1);
        }
    }
}
function parse<Desc extends DescMessage>(schema: Desc, value: Value): MessageShape<Desc> {
    jsonBudget(value);
    jsonPrecision(schema, value);
    const message = fromJson(schema, value);
    validateMessage(schema, message);
    // Float32 and other representation details are normalized by the official codec.
    return fromBinary(schema, toBinary(schema, message), { recursionLimit: 100 });
}
export function parseBinary(contract: MessageContract, bytes: Uint8Array, policy: Validation = 'checked'): Message {
    if (!(bytes instanceof Uint8Array) || bytes.byteLength > MAX_BYTES) throw new Error('invalid protobuf body');
    const schema = messageType(contract);
    if (policy === 'audit') auditBinary(schema, bytes);
    const message = fromBinary(schema, bytes, { recursionLimit: 100 });
    validateMessage(schema, message, policy, true);
    return message;
}
export interface MessageCodec<T> {
    message: string;
    encode(value: T, policy?: Validation): Uint8Array;
    decode(value: Uint8Array, policy?: Validation): T;
    fromJson(value: Value): T;
    toJson(value: T): Value;
}
const codecs = new WeakMap<DescMessage, MessageCodec<Message>>();
export function protobuf<Desc extends DescMessage>(schema: Desc): MessageCodec<MessageShape<Desc>> {
    const existing = codecs.get(schema);
    if (existing) return existing as MessageCodec<MessageShape<Desc>>;
    const codec: MessageCodec<MessageShape<Desc>> = { message: schema.typeName,
        encode(value, policy = 'contract') {
            validateMessage(schema, value, policy);
            const bytes = toBinary(schema, value);
            if (bytes.byteLength > MAX_BYTES) throw new Error('encoded message byte limit exceeded');
            return bytes;
        },
        decode(bytes, policy = 'contract') {
            if (!(bytes instanceof Uint8Array) || bytes.byteLength > MAX_BYTES) throw new Error('invalid protobuf body');
            if (policy === 'audit') auditBinary(schema, bytes);
            const value = fromBinary(schema, bytes, { recursionLimit: 100 });
            validateMessage(schema, value, policy, true);
            return value;
        },
        fromJson(value) { return parse(schema, value); },
        toJson(value) { validateMessage(schema, value); return toJson(schema, value) as Value; },
    };
    codecs.set(schema, codec);
    return codec;
}

export function checkBinary(contract: MessageContract, bytes: Uint8Array, policy: Validation): void {
    if (!(bytes instanceof Uint8Array) || bytes.byteLength > MAX_BYTES) throw new Error('invalid protobuf body');
    if (policy !== 'contract') parseBinary(contract, bytes, policy);
}
