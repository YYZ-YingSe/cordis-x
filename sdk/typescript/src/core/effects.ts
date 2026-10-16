import type { Context } from './context.ts';
import type { RpcError } from './error.ts';
export class Inverse {
  label: string;
  cleanup: (ctx: Context) => void | Promise<void>;
  constructor(label: string, cleanup: (ctx: Context) => void | Promise<void>) {
    this.label = label;
    this.cleanup = cleanup;
  }
}
export type EffectStep = { done: boolean; inverse?: Inverse; error?: RpcError };
export type EffectProducer = (ctx: Context) => EffectStep | Promise<EffectStep>;
export type EffectCompletion = 'completed' | 'stopped';

/** The first Context starts the generator; each subsequent next supplies a fresh Context. */
export function iterate(
  factory: (
    ctx: Context,
  ) =>
    | Generator<Inverse | void, Inverse | void, Context>
    | AsyncGenerator<Inverse | void, Inverse | void, Context>,
): EffectProducer {
  let iterator: ReturnType<typeof factory> | undefined;
  return async (ctx) => {
    let result: IteratorResult<Inverse | void, Inverse | void>;
    if (iterator) result = await iterator.next(ctx);
    else {
      iterator = factory(ctx);
      result = await iterator.next();
    }
    return { done: result.done ?? false, inverse: result.value || undefined };
  };
}
