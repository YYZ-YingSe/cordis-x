import { PipelineExport } from '@cordis-x/example-contracts/lifecycle';
import { run } from '../src/adapters/process.ts';
import { pipeline } from './lifecycle/middleware.ts';
await run(PipelineExport(pipeline, {
  listen: process.argv.includes('--listen'),
  failActivate: process.argv.includes('--fail-activate'),
}), { http: !process.argv.includes('--stdio-only') });
