import { Refusal } from '../../core/error.ts';

export const RESERVED_CAPACITY = 16;
export const REVERSE_LANES = Array<number>(14).fill(RESERVED_CAPACITY);
export function operationLayout(ordinary: number) {
  const forward = [ordinary, ...Array<number>(8).fill(RESERVED_CAPACITY)];
  const incoming = ordinary + 8 * RESERVED_CAPACITY;
  const output = incoming + REVERSE_LANES.reduce((sum, size) => sum + size, 0);
  const connections = 2 * (ordinary + RESERVED_CAPACITY);
  if (!Number.isSafeInteger(ordinary) || ordinary < 1 || output > 0xffffffff || connections > 0xffffffff)
    throw new Refusal('invalid_argument', 'max_inflight must be positive and representable');
  return { forward, incoming, output, connections };
}
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
  private lanes: { start: number; end: number }[];
  constructor(capacity: number | number[]) {
    let end = 0;
    this.lanes = (typeof capacity === 'number' ? [capacity] : capacity).map(size => {
      const start = end; end += size; return { start, end };
    });
    this.slots = Array.from({ length: end }, () => ({ generation: 0n, active: false }));
  }
  allocate(lane?: number): string {
    const { start, end } = lane === undefined ? { start: 0, end: this.slots.length } : this.lanes[lane];
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
  constructor(capacity: number) {
    this.slots = Array.from({ length: capacity }, () => ({ generation: 0n, active: false }));
  }
  grow(capacity: number) {
    if (capacity < this.slots.length) throw new Error('operation table cannot shrink');
    while (this.slots.length < capacity) this.slots.push({ generation: 0n, active: false });
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
