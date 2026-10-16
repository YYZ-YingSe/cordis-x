import { validate } from './value.ts';
import type { Value } from './value.ts';
import { runtimeSchema } from '#cordis-generated/schema';
import { parseMessage, messageType } from './protobuf.ts';
export interface MessageContract {
    message: string;
    descriptor: string;
}
export type Dispatch<T> = {
    mode: 'named';
    value: Record<string, T>;
} | {
    mode: 'dynamic';
    value: T;
};
export interface MethodContract {
    args: MessageContract;
    result: MessageContract;
    errors: string[];
}
export interface ResourceContract {
    args: MessageContract;
    methods: Dispatch<MethodContract>;
    errors: string[];
    ownership: 'caller';
    release: 'drain_once';
}
export interface Interface {
    methods: Dispatch<MethodContract>;
    resources: Dispatch<ResourceContract>;
    capabilities: string[];
    controls: Record<string, ControlContract>;
}
export type Endpoint = { name: string; revision: string };
export type ControlContract =
    | { kind: 'event'; contract: Endpoint; listener: MethodContract }
    | { kind: 'middleware'; contract: Endpoint; handler: MethodContract }
    | { kind: 'factory'; config: MessageContract; errors: string[] }
    | { kind: 'provision'; contract: Endpoint; value: MessageContract };
/** An explicit declaration of a method-name/Value dispatcher, never a default. */
export function valueInterface(resources: boolean): Interface {
    const method = (): MethodContract => ({ args: { message: 'google.protobuf.Value', descriptor: runtimeSchema }, result: { message: 'google.protobuf.Value', descriptor: runtimeSchema }, errors: ['*'] });
    return {
        methods: { mode: 'dynamic', value: method() },
        resources: resources ? { mode: 'dynamic', value: {
                args: { message: 'google.protobuf.Value', descriptor: runtimeSchema }, methods: { mode: 'dynamic', value: method() }, errors: ['*'],
                ownership: 'caller', release: 'drain_once',
            } } : { mode: 'named', value: {} },
        capabilities: [],
        controls: {},
    };
}
function object(value: unknown, path: string): Record<string, any> {
    if (!value || typeof value !== 'object' || Array.isArray(value))
        throw new Error(`${path}: expected object`);
    return value as Record<string, any>;
}
function exact(value: unknown, keys: string[], path: string) {
    const record = object(value, path);
    if (JSON.stringify(Object.keys(record).sort()) !== JSON.stringify(keys.sort()))
        throw new Error(`${path}: invalid fields`);
    return record;
}
function identifier(value: unknown, path: string): string {
    if (typeof value !== 'string' || !value.length || new TextEncoder().encode(value).length > 1024)
        throw new Error(`${path}: invalid identifier`);
    return value;
}
/** Validate bounded structure and normalize sets/maps, independently of the selected codec. */
export function normalizeInterface(input: unknown): Interface {
    validate(input as Value);
    let budget = 4096;
    const take = (path: string, depth = 0) => {
        if (--budget < 0 || depth > 32)
            throw new Error(`${path}: descriptor exceeds node/depth limit`);
    };
    const names = (input: unknown, path: string): string[] => {
        if (!Array.isArray(input))
            throw new Error(`${path}: expected identifier set`);
        const values = input.map((v) => { take(path); return identifier(v, path); });
        if (new Set(values).size !== values.length)
            throw new Error(`${path}: duplicate identifier`);
        return values.sort();
    };
    const message = (input: unknown, path: string): MessageContract => {
        take(path);
        const item = exact(input, ['message', 'descriptor'], path);
        identifier(item.message, path);
        if (typeof item.descriptor !== 'string' || item.descriptor.length > 1398104)
            throw new Error(`${path}: invalid message descriptor`);
        const result = { message: item.message, descriptor: item.descriptor };
        messageType(result);
        return result;
    };
    const dispatch = <T>(input: unknown, path: string, parse: (v: unknown, p: string) => T): Dispatch<T> => {
        take(path);
        const entry = exact(input, ['mode', 'value'], path);
        if (entry.mode === 'dynamic')
            return { mode: 'dynamic', value: parse(entry.value, path) };
        if (entry.mode !== 'named')
            throw new Error(`${path}: invalid dispatch`);
        const entries = object(entry.value, path);
        return { mode: 'named', value: Object.fromEntries(Object.keys(entries).sort().map((key) => {
                identifier(key, path);
                return [key, parse(entries[key], `${path}.${key}`)];
            })) };
    };
    const method = (input: unknown, path: string): MethodContract => {
        take(path);
        const entry = exact(input, ['args', 'result', 'errors'], path);
        return { args: message(entry.args, `${path}.args`), result: message(entry.result, `${path}.result`), errors: names(entry.errors, `${path}.errors`) };
    };
    const control = (input: unknown, path: string): ControlContract => {
        take(path);
        const item = object(input, path);
        const endpoint = (value: unknown): Endpoint => {
            const c = exact(value, ['name', 'revision'], path + '.contract');
            return { name: identifier(c.name, path), revision: identifier(c.revision, path) };
        };
        switch (item.kind) {
            case 'event': exact(item, ['kind', 'contract', 'listener'], path); return { kind: 'event', contract: endpoint(item.contract), listener: method(item.listener, path + '.listener') };
            case 'middleware': exact(item, ['kind', 'contract', 'handler'], path); return { kind: 'middleware', contract: endpoint(item.contract), handler: method(item.handler, path + '.handler') };
            case 'factory': exact(item, ['kind', 'config', 'errors'], path); return { kind: 'factory', config: message(item.config, path + '.config'), errors: names(item.errors, path + '.errors') };
            case 'provision': exact(item, ['kind', 'contract', 'value'], path); return { kind: 'provision', contract: endpoint(item.contract), value: message(item.value, path + '.value') };
            default: throw new Error(path + ': unknown control kind');
        }
    };
    const entry = exact(input, ['methods', 'resources', 'controls', 'capabilities'], 'interface');
    return {
        methods: dispatch(entry.methods, 'methods', method),
        resources: dispatch(entry.resources, 'resources', (input, path) => {
            const resource = exact(input, ['args', 'methods', 'errors', 'ownership', 'release'], path);
            if (resource.ownership !== 'caller' || resource.release !== 'drain_once')
                throw new Error(`${path}: unsupported lifecycle`);
            return { args: message(resource.args, `${path}.args`), methods: dispatch(resource.methods, `${path}.methods`, method), errors: names(resource.errors, `${path}.errors`), ownership: 'caller', release: 'drain_once' };
        }),
        capabilities: names(entry.capabilities, 'capabilities'),
        controls: Object.fromEntries(Object.entries(object(entry.controls, 'controls')).sort(([a],[b]) => a < b ? -1 : a > b ? 1 : 0).map(([key, value]) => {
            identifier(key, 'controls');
            return [key, control(value, 'controls.' + key)];
        })),
    };
}
export function compareInterfaces(expected: Interface, actual: unknown) {
    const compare = (a: any, b: any, path: string) => {
        if (JSON.stringify(a) === JSON.stringify(b))
            return;
        if (a && b && typeof a === 'object' && typeof b === 'object' && !Array.isArray(a) && !Array.isArray(b)) {
            if (JSON.stringify(Object.keys(a).sort()) !== JSON.stringify(Object.keys(b).sort()))
                throw new Error(`${path}: declared identities differ`);
            for (const key of Object.keys(a))
                compare(a[key], b[key], `${path}.${key}`);
            return;
        }
        if (path.endsWith('.descriptor'))
            throw new Error(`${path}: message definitions differ`);
        throw new Error(`${path}: expected ${JSON.stringify(a)}, received ${JSON.stringify(b)}`);
    };
    compare(normalizeInterface(expected), normalizeInterface(actual), 'interface');
}
export function operation<T>(dispatch: Dispatch<T>, name: string): T {
    if (dispatch.mode === 'dynamic')
        return dispatch.value;
    if (!Object.hasOwn(dispatch.value, name))
        throw new Error(`${name}: undeclared operation`);
    return dispatch.value[name];
}
export function checkValue(contract: MessageContract, data: Value, path: string): void {
    try {
        parseMessage(contract, data);
    }
    catch (error) {
        throw new Error(`${path}: ${error}`);
    }
}
