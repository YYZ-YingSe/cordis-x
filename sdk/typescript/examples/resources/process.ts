import { probeClient } from '../../test/fixtures/resource-client.ts';
import { run } from '../../src/adapters/process.ts';
import { client, counters } from './service.ts';

const service = process.argv.includes('--probe') ? probeClient() : process.argv.includes('--client') ? client() : counters();
await run(service, {
  http: !process.argv.includes('--stdio-only'),
});
