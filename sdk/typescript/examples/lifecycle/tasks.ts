import { setTimeout as delay } from 'node:timers/promises';
import { RpcError } from '../../src/index.ts';
import type { Context, Scope, Task } from '../../src/index.ts';
import { HeartbeatsDependencies, type HeartbeatsServer } from '@cordis-x/example-contracts/lifecycle';
import { TaskConfig_Mode as Mode } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/tasks';
import type { TaskConfig } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/tasks';
import { log, unit } from './common.ts';
import { taskStatus } from './managed.ts';
export function heartbeats(config: TaskConfig): HeartbeatsServer {
  const logger = HeartbeatsDependencies.logger();
  const names = new Set<string>();
  const connections = new Map<string, { scope?: Scope; task: Task }>();
  function find(name: string) {
    const entry = connections.get(name);
    if (!entry) throw new RpcError('unknown_connection', name);
    return entry;
  }
  async function open(ctx: Context, name: string) {
    if (names.has(name)) throw new RpcError('duplicate_connection', name);
    names.add(name);
    try {
      const scope = config.direct ? undefined : await ctx.scope(`connection:${name}`);
      const start = async (child: Context) => {
        await child.defer('connection', cleanup => log(logger, cleanup, `${name}/connection.close`));
        const task = await child.spawn('heartbeat', async worker => {
          await log(logger, worker, `${name}/started`);
          await worker.defer('task contribution', cleanup => log(logger, cleanup, `${name}/task.cleanup`));
          if (config.mode === Mode.FAIL) throw new RpcError('task_failed', 'heartbeat failed');
          if (config.mode === Mode.FINITE) return;
          let stopping = false;
          void worker.cancelled().then(() => { stopping = true; });
          while (!stopping) {
            await delay(50);
            if (!stopping) await log(logger, worker, `${name}/heartbeat`);
          }
          await log(logger, worker, `${name}/stopped`);
          await worker.defer('last contribution', cleanup => log(logger, cleanup, `${name}/last.cleanup`));
        });
        connections.set(name, { scope, task });
      };
      if (scope) await scope.run(start); else await start(ctx);
    } catch (error) { names.delete(name); throw error; }
  }
  return {
    async activate(ctx) {
      if (config.failActivate) {
        await open(ctx, 'startup');
        throw new RpcError('startup_failed', 'after task registration');
      }
    },
    async open(ctx, args) { await open(ctx, args.value); return unit(); },
    async join(_ctx, args) { await find(args.value).task.join(); return unit(); },
    async status(_ctx, args) { return taskStatus(await find(args.value).task.snapshot()); },
    async release(_ctx, args) {
      const entry = find(args.value);
      if (!entry.scope) throw new RpcError('direct_task', 'direct tasks stop with their owner');
      await entry.scope.dispose(); return unit();
    },
    async deactivate(ctx) { await log(logger, ctx, 'tasks.closed'); },
  };
}
