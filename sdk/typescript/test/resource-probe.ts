import {body,jsonBody} from './binary-support.ts';
import { valueInterface } from '../src/core/contract.ts';
// Fault injection for the Rust Runtime integration scenarios.
import { run } from '../src/adapters/process.ts';
import { RpcError } from '../src/index.ts';
import type { Value } from '../src/index.ts';

const flag = (args: Value, name: string) =>
  args !== null && typeof args === 'object' && !Array.isArray(args) && args[name] === true;

await run({
  interface: valueInterface(true),
  contract: { name: 'example.counters', revision: '1' },
  dependencyInterfaces: { 'example.logger': valueInterface(true) },
  dependencies: { 'example.logger': '1' },
  activate() {},
  async deactivate(ctx) { await ctx.invoke('example.logger', 'deactivate', body(null)); },
  invoke(method, args, ctx) { return ctx.invoke('example.logger', method, args); },
  async acquire(_kind, args, old) {
    await old.invoke('example.logger', 'acquire', args);
    if (flag(jsonBody(args), 'exit_acquire')) process.exit(77);
    if (flag(jsonBody(args), 'reject')) throw new RpcError('factory_rejected', 'no acquired resource');
    if (flag(jsonBody(args), 'unknown')) throw new Error('unknown acquisition');
    return {
      async invoke(method, _args, ctx) {
        if (method === 'expired') {
          try { await old.invoke('example.logger', 'must-not-run', body(null)); }
          catch (error) { if (error instanceof RpcError && error.code === 'closed') return body(true); throw error; }
          return body(false);
        }
        if (!ctx) throw new Error('missing invocation Context');
        return ctx.invoke('example.logger', 'call', args);
      },
      async close(ctx) {
        if (!ctx) throw new Error('missing cleanup Context');
        await ctx.invoke('example.logger', 'release', args);
        if (flag(jsonBody(args), 'exit_release')) process.exit(78);
        if (flag(jsonBody(args), 'fail_release')) throw new RpcError('close_failed', 'retain resource');
        if (flag(jsonBody(args), 'unknown_release')) throw new Error('unknown release');
      },
    };
  },
}, { http: !process.argv.includes('--stdio-only') });
