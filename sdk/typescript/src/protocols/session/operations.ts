import { Refusal } from '../../core/error.ts';

export const RESERVED_CAPACITY = 16;
export function operationLayout(ordinary: number) {
  const auxiliary = Math.max(ordinary, RESERVED_CAPACITY);
  const forward = [ordinary, ...Array<number>(8).fill(auxiliary)];
  const reverse = Array<number>(14).fill(auxiliary);
  const incoming = ordinary + 8 * auxiliary;
  const output = incoming + reverse.reduce((sum, size) => sum + size, 0);
  const connections = 2 * (ordinary + auxiliary);
  if (!Number.isSafeInteger(ordinary) || ordinary < 1 || output > 0xffffffff || connections > 0xffffffff)
    throw new Refusal('invalid_argument', 'max_inflight must be positive and representable');
  return { forward, reverse, incoming, output, connections };
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
  private lanes: { start: number; size: number; entries: Slot[]; free: number[] }[];
  constructor(capacity: number | number[]) {
    let end = 0;
    this.lanes = (typeof capacity === 'number' ? [capacity] : capacity).map(size => {
      const start = end;
      end += size;
      return { start, size, entries: [], free: [] };
    });
  }
  allocate(lane?: number): string {
    const pool = lane === undefined
      ? this.lanes.find(value => value.free.length || value.entries.length < value.size)
      : this.lanes[lane];
    if (!pool) throw new Refusal('overloaded', 'request slots full');
    let index = pool.free.pop();
    if (index === undefined) {
      index = pool.entries.length;
      if (index >= pool.size) throw new Refusal('overloaded', 'request slots full');
      pool.entries.push({ generation: 0n, active: false });
    }
    const entry = pool.entries[index];
    entry.active = true;
    return `${pool.start + index}:${++entry.generation}`;
  }
  complete(value: string): boolean {
    const id = parseOperation(value);
    if (!id) return false;
    const pool = this.lanes.find(lane => id.slot >= lane.start && id.slot < lane.start + lane.size);
    if (!pool) return false;
    const index = id.slot - pool.start, entry = pool.entries[index];
    if (!entry || !entry.active || entry.generation !== id.generation) return false;
    entry.active = false;
    if (entry.generation < MAX_GENERATION) pool.free.push(index);
    return true;
  }
}
export class OperationInbox {
  private slots = new Map<number, Slot>();
  private capacity: number;
  constructor(capacity: number) { this.capacity = capacity; }
  grow(capacity: number) {
    if (capacity < this.capacity) throw new Error('operation table cannot shrink');
    this.capacity = capacity;
  }
  admit(value: string): () => void {
    const id = parseOperation(value);
    if (!id || id.slot >= this.capacity) throw new Refusal('duplicate_operation', 'invalid request identity');
    const entry = this.slots.get(id.slot) ?? { generation: 0n, active: false };
    if (entry.active || id.generation <= entry.generation)
      throw new Refusal('duplicate_operation', 'active or retired request identity');
    entry.generation = id.generation;
    entry.active = true;
    this.slots.set(id.slot, entry);
    return () => { if (entry.generation === id.generation) entry.active = false; };
  }
}
