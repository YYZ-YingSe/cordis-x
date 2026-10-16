import { HeartbeatsExport } from '@cordis-x/example-contracts/lifecycle';
import { run } from '../src/adapters/process.ts';
import { heartbeats } from './lifecycle/tasks.ts';
await run(HeartbeatsExport(heartbeats, { mode: process.argv.includes('--task-fail') ? 'FAIL' : process.argv.includes('--task-finite') ? 'FINITE' : 'HEARTBEAT', direct: process.argv.includes('--task-direct'), failActivate: process.argv.includes('--fail-activate') }), { http: !process.argv.includes('--stdio-only') });
