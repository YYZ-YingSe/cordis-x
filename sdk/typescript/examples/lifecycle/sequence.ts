import { Inverse, iterate, RpcError } from '../../src/index.ts';
import type { EffectSequence } from '../../src/index.ts';
import { SequencesDependencies, type SequencesServer } from '@cordis-x/example-contracts/lifecycle';
import { log, unit } from './common.ts';
import { completion, count, sequenceStatus } from './managed.ts';
export function sequences(): SequencesServer {
  const logger = SequencesDependencies.logger();
  const runs = new Map<string, { buffers: Array<Uint8Array | undefined>; sequence?: EffectSequence }>();
  function find(name: string) {
    const run = runs.get(name);
    if (!run?.sequence) throw new RpcError('unknown_sequence', name);
    return { buffers: run.buffers, sequence: run.sequence };
  }
  return {
    async start(ctx, args) {
      const name = args.value;
      if (runs.has(name)) throw new RpcError('duplicate_sequence', name);
      const state: { buffers: Array<Uint8Array | undefined>; sequence?: EffectSequence } = { buffers: [] };
      runs.set(name, state);
      try {
        state.sequence = await ctx.effect(`pipeline:${name}`, iterate(async function* (step) {
          for (let n = 1; n <= 3; n++) {
            await log(logger, step, `${name}/step:${n}`);
            state.buffers[n - 1] = new Uint8Array(1024);
            const inverse = new Inverse(`buffer:${n}`, async cleanup => {
              await log(logger, cleanup, `${name}/undo:${n}`);
              state.buffers[n - 1] = undefined;
            });
            if (n === 3) return inverse;
            step = yield inverse;
          }
        }));
      } catch (error) { runs.delete(name); throw error; }
      return unit();
    },
    async ready(_ctx, args) { return completion(await find(args.value).sequence.ready()); },
    async release(_ctx, args) { await find(args.value).sequence.dispose(); return unit(); },
    async status(_ctx, args) { return sequenceStatus(await find(args.value).sequence.snapshot()); },
    async count(_ctx, args) { return count(find(args.value).buffers.filter(Boolean).length); },
    async deactivate(ctx) { await log(logger, ctx, 'pipeline.closed'); },
  };
}
