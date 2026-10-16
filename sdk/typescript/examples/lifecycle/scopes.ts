import { create } from '@bufbuild/protobuf';
import { StringValueSchema } from '@bufbuild/protobuf/wkt';
import { Inverse, iterate, RpcError, typed } from '../../src/index.ts';
import type { Scope } from '../../src/index.ts';
import { ConnectionsDependencies, type ConnectionsServer } from '@cordis-x/example-contracts/lifecycle';
import type { ScopeConfig } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/scopes';
import { log, unit } from './common.ts';
import { count, scopeStatus } from './managed.ts';
export function connections(config: ScopeConfig): ConnectionsServer {
  const logger = ConnectionsDependencies.logger();
  const names = new Set<string>();
  const connections = new Map<string, { scope: Scope; resources: Map<string, Uint8Array> }>();
  function find(name: string) {
    const value = connections.get(name);
    if (!value) throw new RpcError('unknown_connection', name);
    return value;
  }
  return {
    async activate(ctx) {
      if (!config.failActivate) return;
      const scope = await ctx.scope('startup');
      await scope.run(async child => { await child.defer('startup', cleanup => log(logger, cleanup, 'startup.cleanup')); });
      throw new RpcError('startup_failed', 'after child registration');
    },
    async open(ctx, args) {
      const name = args.value;
      if (names.has(name)) throw new RpcError('duplicate_connection', name);
      names.add(name);
      let scope: Scope;
      try { scope = await ctx.scope(`connection:${name}`); }
      catch (error) { names.delete(name); throw error; }
      const resources = new Map<string, Uint8Array>();
      connections.set(name, { scope, resources });
      // Keep a committed scope after initialization failure: it owns any partial work.
      await scope.run(async child => {
        await log(logger, child, `${name}/buffer.open`);
        resources.set('buffer', new Uint8Array(1024));
        await child.defer('buffer', async cleanup => {
          await log(logger, cleanup, `${name}/buffer.close`); resources.delete('buffer');
        });
        const subscriptions = await child.effect('subscriptions', iterate(async function* (step) {
          for (let n = 1; n <= 2; n++) {
            await log(logger, step, `${name}/subscribe:${n}`);
            resources.set(`subscription:${n}`, new Uint8Array(1));
            const inverse = new Inverse(`subscription:${n}`, async cleanup => {
              await log(logger, cleanup, `${name}/unsubscribe:${n}`); resources.delete(`subscription:${n}`);
            });
            if (n === 2) return inverse;
            step = yield inverse;
          }
        }));
        await subscriptions.ready();
        const watcher = await child.scope('watcher');
        await watcher.run(async nested => {
          resources.set('watcher', new Uint8Array(1));
          await nested.defer('watcher', async cleanup => {
            await log(logger, cleanup, `${name}/watcher.close`); resources.delete('watcher');
          });
        });
      });
      return unit();
    },
    async count(_ctx, args) { return count(find(args.value).resources.size); },
    async status(_ctx, args) { return scopeStatus.fromJson(await find(args.value).scope.snapshot()); },
    async release(_ctx, args) { await find(args.value).scope.dispose(); return unit(); },
    async run(_ctx, args) {
      const name = args.value;
      const codec = typed.protobuf(StringValueSchema);
      return codec.fromJson(await find(name).scope.run(async child => {
        await log(logger, child, `${name}/run`);
        return codec.toJson(create(StringValueSchema, { value: 'ok' }));
      }));
    },
    async contribute(_ctx, args) {
      const name = args.value; const connection = find(name);
      await connection.scope.run(async child => {
        await log(logger, child, `${name}/late.enter`);
        connection.resources.set('late', new Uint8Array(1));
        await child.defer('late', async cleanup => {
          await log(logger, cleanup, `${name}/late.close`); connection.resources.delete('late');
        });
      });
      return unit();
    },
    async deactivate(ctx) { await log(logger, ctx, 'connections.closed'); },
  };
}
