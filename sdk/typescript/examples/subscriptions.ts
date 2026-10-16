import { SubscriptionsExport, AuditedSubscriptionsExport } from '@cordis-x/example-contracts/lifecycle';
import { run } from '../src/adapters/process.ts';
import { quietSubscriptions, auditedSubscriptions } from './lifecycle/subscriptions.ts';
const service = process.argv.includes('--logger')
  ? AuditedSubscriptionsExport(auditedSubscriptions, {})
  : SubscriptionsExport(quietSubscriptions, {});
await run(service, { http: !process.argv.includes('--stdio-only') });
