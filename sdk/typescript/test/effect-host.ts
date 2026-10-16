import { valueInterface } from '../src/core/contract.ts';
import { appendFileSync } from 'node:fs';
import { run, RpcError, Inverse } from '../src/adapters/process/host.ts';

await run(
  {
    interface: valueInterface(true),
    contract: { name: 'example.subscriptions', revision: '1' },
    async activate(ctx) {
      if (process.argv[3] === '--sequence') {
        await ctx.effect('startup', () => {
          appendFileSync(process.argv[2], 'step\n');
          return {
            done: true,
            inverse: new Inverse('startup', () => {
              appendFileSync(process.argv[2], 'released\n');
            }),
          };
        });
      } else
        await ctx.defer('startup', () => {
          appendFileSync(process.argv[2], 'released\n');
        });
      throw new RpcError('activation_failed', 'after effect registration');
    },
    invoke() {
      throw new Error('failed activation cannot accept calls');
    },
    deactivate() {
      appendFileSync(process.argv[2], 'deactivated\n');
    },
  },
  { http: false },
);
