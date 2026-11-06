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

/** A bounded terminal cache is expendable; live and failed work is not. */
export function makeRoom<T>(entries: Map<string, T>, capacity: number, retired: (entry: T) => boolean): void {
  if (entries.size >= capacity) {
    for (const [id, entry] of entries) if (retired(entry)) entries.delete(id);
  }
  if (entries.size >= capacity) throw new Refusal('overloaded', 'live capacity reached');
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
