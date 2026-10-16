import { ConnectionsExport } from '@cordis-x/example-contracts/lifecycle';
import { run } from '../src/adapters/process.ts';
import { connections } from './lifecycle/scopes.ts';
await run(ConnectionsExport(connections, { failActivate: process.argv.includes('--fail-activate') }), { http: !process.argv.includes('--stdio-only') });
