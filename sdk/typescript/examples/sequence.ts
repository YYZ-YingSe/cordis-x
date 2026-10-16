import { SequencesExport } from '@cordis-x/example-contracts/lifecycle';
import { run } from '../src/adapters/process.ts';
import { sequences } from './lifecycle/sequence.ts';
await run(SequencesExport(sequences, {}), { http: !process.argv.includes('--stdio-only') });
