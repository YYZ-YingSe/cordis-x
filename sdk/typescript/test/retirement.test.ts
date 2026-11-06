import test from 'node:test';
import assert from 'node:assert/strict';
import { EffectRegistry, type Context, type DependencyChannel } from '../src/adapters/process/context.ts';
import { SequenceRegistry } from '../src/adapters/process/sequence.ts';
import { Refusal } from '../src/core/error.ts';

const context = { async finish() {} } as Context;

test('released effects turn over beyond capacity; refused registrations do not retain captures', async () => {
  const effects = new EffectRegistry();
  let inverses = 0, disposals = 0;
  let deny = false;
  const channel = { async request(command: string, args: Record<string, string>) {
    if (command === 'effect_register') {
      if (deny) throw new Refusal('overloaded', 'full');
      return { effect: args.effect, grant: 'b'.repeat(32) };
    }
    assert.equal(command, 'effect_dispose');
    disposals++;
    await effects.release(args.effect, context);
    return {};
  }} as unknown as DependencyChannel;
  const first = await effects.register(channel, 'ctx', 'first', () => { inverses++; });
  await first.dispose();
  deny = true;
  for (let n = 0; n < 4200; n++)
    await assert.rejects(effects.register(channel, 'ctx', 'refused', () => {}), { code: 'overloaded' });
  deny = false;
  for (let n = 0; n < 4200; n++) {
    const effect = await effects.register(channel, 'ctx', 'turn', () => { inverses++; });
    await effect.dispose();
  }
  await first.dispose(); // Result survives removal of its host registry record.
  assert.equal(inverses, 4201);
  assert.equal(disposals, 4201);
  assert.equal(effects.released, true);
});

test('no-effect steps and completed sequences do not consume lifetime quotas', async () => {
  const sequences = new SequenceRegistry();
  const channel = { async request(_command: string, args: Record<string, string>) {
    return { sequence: args.sequence, grant: 'b'.repeat(32) };
  }} as unknown as DependencyChannel;
  for (let n = 1; n <= 300; n++) {
    let step = 0;
    await sequences.register(channel, 'ctx', 'turn', async () => ({ done: ++step === 20 }));
    for (let j = 1; j <= 20; j++) await sequences.next(`s${n}`, String(j), context);
    sequences.release(`s${n}`);
  }
  assert.equal(sequences.released, true);
});
