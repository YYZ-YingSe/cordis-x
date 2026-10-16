/** Shared generated-binding support. Authority and lifecycle stay in the adapter. */
import type { MessageContract } from './contract.ts';
import type { Validation } from './validation.ts';
import type { Message } from './message.ts';
import { Refusal, RpcError } from './error.ts';
import type { Value } from './value.ts';
export { protobuf } from './protobuf.ts';
import type { MessageCodec as Codec } from './protobuf.ts';
function identity<T>(shape: MessageContract, codec: Codec<T>) { if (shape.message !== codec.message)
    throw new Error('codec/contract message mismatch'); }
export function input<T>(value: Message, shape: MessageContract, codec: Codec<T>, policy: Validation = 'contract'): T {
    try {
        identity(shape, codec);
        return codec.decode(value, policy);
    }
    catch (error) {
        throw new Refusal('invalid_argument', String(error));
    }
}
export function result<T>(value: Message, shape: MessageContract, codec: Codec<T>, policy: Validation = 'contract'): T {
    identity(shape, codec);
    return codec.decode(value, policy);
}
export function argument<T>(value: T, shape: MessageContract, codec: Codec<T>, policy: Validation = 'contract'): Message {
    try {
        identity(shape, codec);
        const encoded = codec.encode(value, policy);
        return encoded;
    }
    catch (error) {
        throw new Refusal('invalid_argument', String(error));
    }
}
export function output<T>(value: T, shape: MessageContract, codec: Codec<T>, policy: Validation = 'contract'): Message {
    identity(shape, codec);
    const encoded = codec.encode(value, policy);
    return encoded;
}
export async function checked<T>(errors: readonly string[], call: () => Promise<T>): Promise<T> {
    try {
        return await call();
    }
    catch (error) {
        if (error instanceof RpcError && !(error instanceof Refusal) &&
            (!error.code || (!errors.includes(error.code) && !errors.includes('*'))))
            throw new Error('undeclared business error: ' + error.code);
        throw error;
    }
}

/** Configuration and explicitly external JSON entry points only. */
export function jsonInput<T>(value: Value, shape: MessageContract, codec: Codec<T>): T {
    try { identity(shape, codec); return codec.fromJson(value); }
    catch (error) { throw new Refusal('invalid_argument', String(error)); }
}
