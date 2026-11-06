import { Refusal } from '../../core/error.ts';

export const SLOTS_PER_LANE = 128;
export const DEFAULT_SLOTS = 16 * SLOTS_PER_LANE;
const MAX_GENERATION = (1n << 64n) - 1n;
type Slot = { generation: bigint; active: boolean };
export function parseOperation(value: string): { slot: number; generation: bigint } | undefined {
  if (!/^(0|[1-9][0-9]*):[1-9][0-9]*$/.test(value) || value.length > 41) return;
  const [rawSlot, rawGeneration] = value.split(':');
  const slot = Number(rawSlot), generation = BigInt(rawGeneration);
  if (!Number.isSafeInteger(slot) || generation > MAX_GENERATION) return;
  return { slot, generation };
}
export class OperationOutbox {
  private slots: Slot[];
  constructor(capacity = DEFAULT_SLOTS) {
    this.slots = Array.from({ length: capacity }, () => ({ generation: 0n, active: false }));
  }
  allocate(lane?: number): string {
    const start = lane === undefined ? 0 : lane * SLOTS_PER_LANE;
    const end = lane === undefined ? this.slots.length : start + SLOTS_PER_LANE;
    const slot = this.slots.findIndex((s, i) => i >= start && i < end && !s.active && s.generation < MAX_GENERATION);
    if (slot < 0) throw new Refusal('overloaded', 'request slots full');
    const entry = this.slots[slot];
    entry.active = true;
    return `${slot}:${++entry.generation}`;
  }
  complete(value: string): boolean {
    const id = parseOperation(value), entry = id && this.slots[id.slot];
    if (!entry || !entry.active || entry.generation !== id!.generation) return false;
    entry.active = false;
    return true;
  }
}
export class OperationInbox {
  private slots: Slot[];
  constructor(capacity = DEFAULT_SLOTS) {
    this.slots = Array.from({ length: capacity }, () => ({ generation: 0n, active: false }));
  }
  admit(value: string): () => void {
    const id = parseOperation(value), entry = id && this.slots[id.slot];
    if (!entry || entry.active || id!.generation <= entry.generation)
      throw new Refusal('duplicate_operation', 'invalid, active or retired request identity');
    entry.generation = id!.generation;
    entry.active = true;
    let completed = false;
    return () => { if (!completed) { completed = true; entry.active = false; } };
  }
}
