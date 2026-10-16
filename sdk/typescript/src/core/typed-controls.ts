import type { Validation } from './validation.ts';
import type { Message } from './message.ts';
/** Typed views of runtime-owned capabilities. Wrappers never extend authority. */
import type { Context } from './context.ts';
import type { Child, Dispatch, MiddlewareRegistration, Next as RawNext, Subscription } from './handles.ts';
import type { MessageContract, MethodContract } from './contract.ts';
import type { MessageCodec } from './protobuf.ts';
import type { Value } from './value.ts';
import { argument, checked, input, output, result } from './typed-codec.ts';

export type Flow<T> = { kind: 'continue' } | { kind: 'break'; value: T };
export const Continue = <T>(): Flow<T> => ({ kind: 'continue' });
export const Break = <T>(value: T): Flow<T> => ({ kind: 'break', value });

function flow<T>(value: unknown, convert: (value: unknown) => T): Flow<T> {
  if (!value || typeof value !== 'object' || Array.isArray(value))
    throw new Error('invalid event flow');
  const item = value as Record<string, unknown>;
  if (item.kind === 'continue' && Object.keys(item).length === 1) return Continue();
  if (item.kind === 'break' && Object.keys(item).length === 2 && Object.hasOwn(item, 'value'))
    return Break(convert(item.value));
  throw new Error('invalid event flow');
}

export class Event<A, R> {
  readonly alias: string;
  private readonly signature: MethodContract;
  private readonly argsCodec: MessageCodec<A>;
  private readonly resultCodec: MessageCodec<R>;
  constructor(alias: string, signature: MethodContract, argsCodec: MessageCodec<A>, resultCodec: MessageCodec<R>) {
    this.alias = alias;
    this.signature = signature;
    this.argsCodec = argsCodec;
    this.resultCodec = resultCodec;
  }
  async on(ctx: Context, handler: (ctx: Context, args: A) => Promise<Flow<R>>): Promise<Subscription> {
    return ctx.on(this.alias, async (value, current) => {
      const args = input(value, this.signature.args, this.argsCodec, ctx.validation);
      const response = await checked(this.signature.errors, () => handler(current, args));
      return flow(response, value => output(value as R, this.signature.result, this.resultCodec, current.validation));
    });
  }
  private decode(value: unknown, policy: Validation): Flow<R> {
    return flow(value, value => result(value as Message, this.signature.result, this.resultCodec, policy));
  }
  async emit(ctx: Context, args: A): Promise<TypedDispatch<R>> {
    const dispatch = await ctx.emit(this.alias, argument(args, this.signature.args, this.argsCodec, ctx.validation));
    return new TypedDispatch(dispatch, value => this.decode(value, ctx.validation), this.signature.errors);
  }
  async parallel(ctx: Context, args: A): Promise<Flow<R>[]> {
    return (await this.emit(ctx, args)).join();
  }
  async serial(ctx: Context, args: A): Promise<Flow<R>> {
    const encoded = argument(args, this.signature.args, this.argsCodec, ctx.validation);
    return this.decode(await checked(this.signature.errors, () => ctx.serial(this.alias, encoded)), ctx.validation);
  }
}
export class TypedDispatch<R> {
  private readonly dispatch: Dispatch;
  private readonly decode: (value: unknown) => Flow<R>;
  private readonly errors: string[];
  constructor(dispatch: Dispatch, decode: (value: unknown) => Flow<R>, errors: string[]) {
    this.errors = errors;
    this.dispatch = dispatch;
    this.decode = decode;
  }
  async join(): Promise<Flow<R>[]> {
    const values = await checked(this.errors, () => this.dispatch.join());
    if (!Array.isArray(values)) throw new Error('expected event result array');
    return values.map(value => this.decode(value));
  }
  snapshot(): Promise<Value> { return this.dispatch.snapshot(); }
}
export class Next<A, R> {
  private readonly validation: Validation;
  private readonly next: RawNext;
  private readonly signature: MethodContract;
  private readonly argsCodec: MessageCodec<A>;
  private readonly resultCodec: MessageCodec<R>;
  constructor(next: RawNext, signature: MethodContract, argsCodec: MessageCodec<A>, resultCodec: MessageCodec<R>, validation: Validation) {
    this.validation = validation;
    this.next = next;
    this.signature = signature;
    this.argsCodec = argsCodec;
    this.resultCodec = resultCodec;
  }
  async run(args: A): Promise<R> {
    const encoded = argument(args, this.signature.args, this.argsCodec, this.validation);
    const value = await checked(this.signature.errors, () => this.next.run(encoded));
    return result(value, this.signature.result, this.resultCodec, this.validation);
  }
}
export class Middleware<A, R> {
  readonly alias: string;
  private readonly signature: MethodContract;
  private readonly argsCodec: MessageCodec<A>;
  private readonly resultCodec: MessageCodec<R>;
  constructor(alias: string, signature: MethodContract, argsCodec: MessageCodec<A>, resultCodec: MessageCodec<R>) {
    this.alias = alias;
    this.signature = signature;
    this.argsCodec = argsCodec;
    this.resultCodec = resultCodec;
  }
  async register(ctx: Context, handler: (ctx: Context, args: A, next: Next<A, R>) => Promise<R>): Promise<MiddlewareRegistration> {
    return ctx.middleware(this.alias, async (value, current, next) => {
      const args = input(value, this.signature.args, this.argsCodec, ctx.validation);
      const typed = new Next(next, this.signature, this.argsCodec, this.resultCodec, ctx.validation);
      const response = await checked(this.signature.errors, () => handler(current, args, typed));
      return output(response, this.signature.result, this.resultCodec, ctx.validation);
    });
  }
  async run(ctx: Context, args: A, terminal: (ctx: Context, args: A) => Promise<R>): Promise<R> {
    const encoded = argument(args, this.signature.args, this.argsCodec, ctx.validation);
    const value = await checked(this.signature.errors, () => ctx.waterfall(this.alias, encoded, async (value, current) => {
      const args = input(value, this.signature.args, this.argsCodec, ctx.validation);
      const response = await checked(this.signature.errors, () => terminal(current, args));
      return output(response, this.signature.result, this.resultCodec, ctx.validation);
    }));
    return result(value, this.signature.result, this.resultCodec, ctx.validation);
  }
}
export class Factory<C> {
  readonly alias: string;
  private readonly config: MessageContract;
  private readonly errors: string[];
  private readonly codec: MessageCodec<C>;
  constructor(alias: string, config: MessageContract, errors: string[], codec: MessageCodec<C>) {
    this.alias = alias;
    this.config = config;
    this.errors = errors;
    this.codec = codec;
  }
  async install(ctx: Context, config: C): Promise<Child> {
    const value = argument(config, this.config, this.codec, ctx.validation);
    return checked(this.errors, () => ctx.install(this.alias, value));
  }
  async build<T>(config: Message, factory: (config: C) => T | Promise<T>, policy: Validation = 'checked'): Promise<T> {
    const value = input(config, this.config, this.codec, policy);
    return checked(this.errors, async () => factory(value));
  }
}
export class Provision<T> {
  readonly alias: string;
  private readonly shape: MessageContract;
  private readonly codec: MessageCodec<T>;
  constructor(alias: string, shape: MessageContract, codec: MessageCodec<T>) {
    this.alias = alias;
    this.shape = shape;
    this.codec = codec;
  }
  async provide(ctx: Context, value: T): Promise<void> {
    await ctx.provide(this.alias, argument(value, this.shape, this.codec, ctx.validation));
  }
  decode(value: Message, policy: Validation = 'checked'): T { return result(value, this.shape, this.codec, policy); }
}
