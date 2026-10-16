import { Refusal } from '../../core/error.ts';
import { RpcError } from './context.ts';
import type { Context, DependencyChannel } from './context.ts';
import type { Value } from '../../core/value.ts';

import { Inverse } from '../../core/effects.ts';
import type { EffectProducer, EffectStep, EffectCompletion } from '../../core/effects.ts';
export { Inverse, iterate } from '../../core/effects.ts';
export type { EffectProducer, EffectStep, EffectCompletion } from '../../core/effects.ts';
export class EffectSequence {
  private channel: DependencyChannel;
  private sequence: string;
  private grant: string;
  constructor(channel: DependencyChannel, sequence: string, grant: string) {
    this.channel = channel;
    this.sequence = sequence;
    this.grant = grant;
  }
  private request(command: string) {
    return this.channel.request(command, { sequence: this.sequence, grant: this.grant });
  }
  async ready(): Promise<EffectCompletion> {
    const reply = await this.request('sequence_ready');
    if (
      !reply ||
      typeof reply !== 'object' ||
      Array.isArray(reply) ||
      (reply.completion !== 'completed' && reply.completion !== 'stopped')
    )
      throw new Refusal('protocol', 'invalid sequence completion');
    return reply.completion;
  }
  async dispose(): Promise<void> {
    await this.request('sequence_dispose');
  }
  snapshot(): Promise<Value> {
    return this.request('sequence_status');
  }
}

type StepRecord = { inverse?: Inverse; result?: Promise<void>; released: boolean };
type SequenceRecord = {
  producer?: EffectProducer;
  running: boolean;
  ended: boolean;
  released: boolean;
  next: number;
  steps: Map<string, StepRecord>;
};
export class SequenceRegistry {
  private entries = new Map<string, SequenceRecord>();
  private nextId = 0;
  private steps = 0;
  async register(
    channel: DependencyChannel,
    context: string,
    label: string,
    producer: EffectProducer,
  ): Promise<EffectSequence> {
    if (Buffer.byteLength(label) > 1024)
      throw new Refusal('invalid_argument', 'sequence label too long');
    if (this.nextId >= 256) throw new Refusal('sequences_exhausted', 'sequence history full');
    const sequence = `s${++this.nextId}`;
    // Save the producer before requesting ownership, including acknowledgement loss.
    this.entries.set(sequence, {
      producer,
      running: false,
      ended: false,
      released: false,
      next: 1,
      steps: new Map(),
    });
    const reply = await channel.request('sequence_register', { context, sequence, label });
    if (
      !reply ||
      typeof reply !== 'object' ||
      Array.isArray(reply) ||
      reply.sequence !== sequence ||
      typeof reply.grant !== 'string' ||
      !/^[0-9a-f]{32}$/.test(reply.grant)
    )
      throw new Refusal('protocol', 'invalid sequence registration');
    return new EffectSequence(channel, sequence, reply.grant);
  }
  private entry(id: string): SequenceRecord {
    const entry = this.entries.get(id);
    if (!entry) throw new Refusal('unknown_sequence', id);
    return entry;
  }
  async next(id: string, step: string, ctx: Context): Promise<Value> {
    const entry = this.entry(id);
    if (entry.released || entry.ended || entry.running)
      throw new Refusal('closed', 'sequence cannot advance');
    if (step !== String(entry.next)) throw new Refusal('invalid_step', 'step order mismatch');
    if (this.steps >= 4096) throw new Refusal('steps_exhausted', 'step history full');
    this.steps++;
    entry.next++;
    entry.running = true;
    let result: EffectStep;
    try {
      try {
        result = await entry.producer!(ctx);
      } catch (error) {
        result = {
          done: true,
          error: error instanceof RpcError ? error : new Refusal('step_failed', String(error)),
        };
      }
      // Save any inverse even when validating the result later fails.
      const inverse = result?.inverse;
      entry.steps.set(step, { inverse, released: !inverse });
      if (
        !result ||
        typeof result.done !== 'boolean' ||
        (result.error && !result.done) ||
        (inverse && (!(inverse instanceof Inverse) || Buffer.byteLength(inverse.label) > 1024))
      )
        throw new Refusal('invalid_step', 'invalid producer result');
      entry.ended = result.done;
      return {
        done: result.done,
        inverse: inverse?.label ?? null,
        error: result.error ? { code: result.error.code, message: result.error.message } : null,
      };
    } finally {
      await ctx.finish();
      entry.running = false;
    }
  }
  async undo(id: string, step: string, ctx: Context): Promise<void> {
    const entry = this.entry(id).steps.get(step);
    if (!entry?.inverse && !entry?.result) throw new Refusal('unknown_inverse', step);
    if (!entry.result) {
      entry.result = Promise.resolve().then(async () => {
        try {
          await entry.inverse!.cleanup(ctx);
        } finally {
          await ctx.finish();
        }
        entry.released = true;
        entry.inverse = undefined;
      });
    }
    await entry.result;
  }
  release(id: string): void {
    const entry = this.entry(id);
    if (entry.running || [...entry.steps.values()].some((s) => !s.released))
      throw new Refusal('effects_remaining', 'sequence inverses remain');
    entry.released = true;
    entry.ended = true;
    entry.producer = undefined;
  }
  get released(): boolean {
    return [...this.entries.values()].every((entry) => entry.released);
  }
}
