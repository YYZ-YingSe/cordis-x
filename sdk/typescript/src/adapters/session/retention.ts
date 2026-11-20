import { Refusal } from '../../core/error.ts';

/** Non-reused identities, independent of current resource capacity. */
export class Identities {
  private value = 0n;
  next(prefix: string): string {
    if (this.value === 0xffffffffffffffffn)
      throw new Refusal('exhausted', 'identity space exhausted');
    return `${prefix}${++this.value}`;
  }
}

const nextSweep = new WeakMap<object, number>();

/** Only retire observed entries. Growing live sets amortize registry scans. */
export function sweepRetired<T>(entries: Map<string, T>, retired: (entry: T) => boolean): void {
  if (entries.size < (nextSweep.get(entries) ?? 4096)) return;
  for (const [id, entry] of entries) if (retired(entry)) entries.delete(id);
  nextSweep.set(entries, Math.max(4096, entries.size * 2));
}

/** Copies of a public capability observe one completion; refusals can retry. */
export class Completion<T> {
  private value?: Promise<T>;
  observe(run: () => Promise<T>): Promise<T> {
    return this.value ??= run().catch((error) => {
      if (error instanceof Refusal) this.value = undefined;
      throw error;
    });
  }
}

/** Only a refusal certifies that registration did not transfer ownership. */
export async function registration<T>(pending: Promise<T>, refused: () => void): Promise<T> {
  try { return await pending; }
  catch (error) { if (error instanceof Refusal) refused(); throw error; }
}

/** Independent cancellable observers share only a delivered terminal outcome. */
export class Observation<T> {
  private terminal?: { value: T } | { error: unknown };
  async observe(run: () => Promise<T>): Promise<T> {
    if (this.terminal) {
      if ('error' in this.terminal) throw this.terminal.error;
      return this.terminal.value;
    }
    try {
      const value = await run();
      this.terminal = { value };
      return value;
    } catch (error) {
      if (!(error instanceof Refusal)) this.terminal = { error };
      throw error;
    }
  }
}
