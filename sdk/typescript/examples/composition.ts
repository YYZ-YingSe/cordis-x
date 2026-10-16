import { ComposerExport } from '@cordis-x/example-contracts/lifecycle';
import { run } from '../src/adapters/process.ts';
import { composer } from './lifecycle/composition.ts';
await run(ComposerExport(composer, {
  publication: process.argv.includes('--missing') ? 'OMIT' : process.argv.includes('--duplicate') ? 'DUPLICATE' : 'NORMAL',
  failActivate: process.argv.includes('--fail'),
}), { http: !process.argv.includes('--stdio-only') });
