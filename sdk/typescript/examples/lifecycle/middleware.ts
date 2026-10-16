import { create } from '@bufbuild/protobuf';
import type { Int64Value } from '@bufbuild/protobuf/wkt';
import { RpcError, typed } from '../../src/index.ts';
import type { MiddlewareRegistration } from '../../src/index.ts';
import { PipelineControls as controls, PipelineDependencies, type PipelineServer } from '@cordis-x/example-contracts/lifecycle';
import type { ListenerConfig } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/common';
import { HandlerSpecSchema, HandlerSpec_Behavior as Behavior } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/middleware';
import { add, log, number, unit } from './common.ts';
export function pipeline(config: ListenerConfig): PipelineServer {
  const logger = PipelineDependencies.logger();
  const ids = new Set<string>();
  const handlers = new Map<string, MiddlewareRegistration>();
  let saved: typed.Next<Int64Value, Int64Value> | undefined;
  const server: PipelineServer = {
    async activate(ctx) {
      if (config.listen) await server.register_handler(ctx, create(HandlerSpecSchema, { id: 'subscriber', behavior: Behavior.WRAP }));
      if (config.failActivate) throw new RpcError('startup', 'after registration');
    },
    async register_handler(ctx, args) {
      const id = args.id!;
      if (ids.has(id)) throw new RpcError('duplicate_handler', id);
      ids.add(id);
      try {
        const registration = await controls.request().register(ctx, async (current, value, next) => {
          saved = next;
          await log(logger, current, `${id}.enter`);
          await current.defer(`${id}.resource`, cleanup => log(logger, cleanup, `${id}.cleanup`));
          if (args.behavior === Behavior.FAIL) throw new RpcError('closed', 'business failure');
          if (args.behavior === Behavior.SHORT) return number(add(value.value, 100n));
          const result = await next.run(number(add(value.value, 1n)));
          if (args.behavior === Behavior.DUPLICATE) await next.run(value);
          await log(logger, current, `${id}.exit`);
          return number(add(result.value, 3n));
        });
        handlers.set(id, registration);
      } catch (error) { ids.delete(id); throw error; }
      return unit();
    },
    async release(_ctx, args) {
      const handler = handlers.get(args.value);
      if (!handler) throw new RpcError('unknown_handler', args.value);
      await handler.dispose(); return unit();
    },
    async expired(_ctx, args) {
      if (!saved) throw new RpcError('no_next', 'no middleware frame observed');
      return saved.run(args);
    },
    async run(ctx, args) {
      return controls.request().run(ctx, args, async (terminal, value) => {
        await log(logger, terminal, 'terminal.enter');
        await terminal.defer('terminal.resource', cleanup => log(logger, cleanup, 'terminal.cleanup'));
        return number(add(value.value, value.value));
      });
    },
    async identity(ctx, args) { return controls.request().run(ctx, args, async (_ctx, value) => value); },
    async deactivate(ctx) { await log(logger, ctx, 'middleware.closed'); },
  };
  return server;
}
