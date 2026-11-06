import { Identities, makeRoom, Completion, registration } from './retention.ts';
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
  private disposeCompletion = new Completion<void>();
  dispose(): Promise<void> {
    return this.disposeCompletion.observe(async () => { await this.request('sequence_dispose'); });
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
  next: bigint;
  steps: Map<string, StepRecord>;
};
export class SequenceRegistry {
  private entries = new Map<string, SequenceRecord>();
  private nextId = new Identities();
  private steps = 0;
  async register(
    channel: DependencyChannel,
    context: string,
    label: string,
    producer: EffectProducer,
  ): Promise<EffectSequence> {
    if (Buffer.byteLength(label) > 1024)
      throw new Refusal('invalid_argument', 'sequence label too long');
    makeRoom(this.entries, 256, (e) => e.released);
    const sequence = this.nextId.next('s');
    // Save the producer before requesting ownership, including acknowledgement loss.
    this.entries.set(sequence, {
      producer,
      running: false,
      ended: false,
      released: false,
      next: 1n,
      steps: new Map(),
    });
    const reply = await registration(channel.request('sequence_register', { context, sequence, label }), () => this.entries.delete(sequence));
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
    if (this.steps >= 4096) throw new Refusal('overloaded', 'unrecovered step capacity reached');
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
      if (inverse) entry.steps.set(step, { inverse, released: false });
      else this.steps--;
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
        this.steps--;
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
