import { EventsExport } from '@cordis-x/example-contracts/lifecycle';
import { run } from '../src/adapters/process.ts';
import { events } from './lifecycle/events.ts';
await run(EventsExport(events, {
  listen: process.argv.includes('--listen'),
  failActivate: process.argv.includes('--fail-activate'),
}), { http: !process.argv.includes('--stdio-only') });
