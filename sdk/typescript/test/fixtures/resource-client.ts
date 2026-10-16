import {body,jsonBody} from '../binary-support.ts';
// Sends raw fault payloads to resource probes, including malformed factory arguments.
import { RpcError, valueInterface } from '../../src/index.ts';
import type { ResourceHandle, Service } from '../../src/index.ts';
export function probeClient(): Service {
  let resource: ResourceHandle | undefined;
  return {
    interface: valueInterface(true),
    contract: { name: 'example.counter-client', revision: '1' },
    dependencyInterfaces: { 'example.counters': valueInterface(true) },
    dependencies: { 'example.counters': '1' },
    activate() {},
    deactivate() {},
    async invoke(method, args, ctx) {
      if (method === 'scoped') {
        const counter = await ctx.acquire('example.counters', 'counter', args);
        const scope = await ctx.scope('counter scope');
        const result = await scope.run(async (inner) => {
          await inner.defer('read before close', async (cleanup) => {
            await counter.invoke(cleanup, 'get', body({}));
          });
          return jsonBody(await counter.invoke(inner, 'get', body({})));
        });
        await scope.dispose();
        await counter.dispose(ctx);
        return body(result);
      }
      if (method === 'acquire') {
        resource = await ctx.acquire('example.counters', 'counter', args);
        return body(null);
      }
      if (!resource) throw new RpcError('empty', 'acquire a counter first');
      if (method === 'dispose') {
        await resource.dispose(ctx);
        return body(null);
      }
      return resource.invoke(ctx, method, args);
    },
  };
}
