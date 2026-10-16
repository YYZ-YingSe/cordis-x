import { RpcError } from '../../src/index.ts';
import type { Context, Effect } from '../../src/index.ts';
import type { SubscriptionsServer, AuditedSubscriptionsServer } from '@cordis-x/example-contracts/lifecycle';
import { AuditedSubscriptionsDependencies } from '@cordis-x/example-contracts/lifecycle';
import { log, unit } from './common.ts';
import { count } from './managed.ts';

type Log = (ctx: Context, message: string) => Promise<void>;
// Both generated contracts use this business implementation. Dependencies are
// fixed by each Export, not enabled by runtime protocol detection.
function subscriptions(write: Log): SubscriptionsServer {
  const names = new Set<string>();
  const listeners = new Set<string>();
  const handles = new Map<string, Effect>();
  return {
    async subscribe(ctx, args) {
      const name = args.value;
      if (names.has(name)) throw new RpcError('duplicate_subscription', name);
      names.add(name);
      try {
        await write(ctx, `opening:${name}`);
        const effect = await ctx.defer(`subscription:${name}`, async cleanup => {
          await write(cleanup, `released:${name}`);
          listeners.delete(name);
          handles.delete(name);
        });
        // No await between registering recovery and making the listener visible.
        handles.set(name, effect);
        listeners.add(name);
      } catch (error) { names.delete(name); throw error; }
      return args;
    },
    async unsubscribe(_ctx, args) { await handles.get(args.value)?.dispose(); return unit(); },
    async publish(_ctx, args) {
      for (const name of listeners) console.error(`[${name}] ${args.value}`);
      return count(listeners.size);
    },
    async count() { return count(listeners.size); },
    async deactivate(ctx) { await write(ctx, 'subscriptions.closed'); },
  };
}
export const quietSubscriptions = (): SubscriptionsServer => subscriptions(async () => {});
export const auditedSubscriptions = (): AuditedSubscriptionsServer => {
  const logger = AuditedSubscriptionsDependencies.logger();
  return subscriptions((ctx, message) => log(logger, ctx, message));
};
