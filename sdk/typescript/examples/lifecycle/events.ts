import { create } from '@bufbuild/protobuf';
import { UInt64ValueSchema } from '@bufbuild/protobuf/wkt';
import { setTimeout as delay } from 'node:timers/promises';
import { RpcError, typed } from '../../src/index.ts';
import type { Context, Subscription, Value } from '../../src/index.ts';
import { EventsControls as controls, EventsDependencies, type EventsServer } from '@cordis-x/example-contracts/lifecycle';
import type { ListenerConfig } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/common';
import { ListenerSpec_Behavior as Behavior, ListenerSpecSchema, TickValueSchema, EventDecisionSchema, EventDecisionsSchema, EventResultSchema, DispatchStatusSchema, DispatchFailuresSchema } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/events';
import type { EventResult, ListenerSpec } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/events';
import { log, unit } from './common.ts';

function decision(flow: typed.Flow<EventResult>) {
  return create(EventDecisionSchema, { state: flow.kind === 'break'
    ? { case: 'stopped', value: flow.value } : { case: 'continued', value: unit() } });
}
function decisions(flows: typed.Flow<EventResult>[]) {
  return create(EventDecisionsSchema, { values: flows.map(decision) });
}
function record(value: Value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('invalid dispatch diagnostic');
  return value;
}
function snapshot(value: Value, flows: typed.Flow<EventResult>[]) {
  const outcome = record(value).outcome;
  if (outcome === null) return create(DispatchStatusSchema, { state: { case: 'pending', value: unit() } });
  const result = record(outcome);
  if (result.kind === 'failure') return create(DispatchStatusSchema, { state: {
    case: 'failed', value: typed.protobuf(DispatchFailuresSchema).fromJson({ errors: result.errors }),
  } });
  if (result.kind !== 'success') throw new Error('invalid dispatch outcome');
  return create(DispatchStatusSchema, { state: { case: 'succeeded', value: decisions(flows) } });
}
export function events(config: ListenerConfig): EventsServer {
  const logger = EventsDependencies.logger();
  const ids = new Set<string>();
  const subscriptions = new Map<string, Subscription>();
  let current: typed.TypedDispatch<EventResult> | undefined;
  let calls = 0n;
  const dispatch = () => {
    if (!current) throw new RpcError('no_dispatch', 'no dispatch started');
    return current;
  };
  async function listen(ctx: Context, args: ListenerSpec) {
    const id = args.id!;
    if (ids.has(id)) throw new RpcError('duplicate_listener', id);
    ids.add(id);
    try {
      const subscription = await controls.tick().on(ctx, async (listener, value) => {
        if (calls === (1n << 64n) - 1n) throw new RpcError('overflow', 'event count exhausted');
        calls++;
        await log(logger, listener, `${id}.enter`);
        await listener.defer(`${id}.contribution`, cleanup => log(logger, cleanup, `${id}.cleanup`));
        if (args.behavior === Behavior.NESTED && !value.nested) {
          const nested = await controls.tick().parallel(listener, create(TickValueSchema, { value: value.value, nested: true }));
          return typed.Break(create(EventResultSchema, { state: { case: 'nested', value: decisions(nested) } }));
        }
        if (args.behavior === Behavior.FAIL) throw new RpcError('closed', `${id} failed`);
        return args.behavior === Behavior.BREAK
          ? typed.Break(create(EventResultSchema, { state: { case: 'value', value: value.value! } }))
          : typed.Continue<EventResult>();
      });
      subscriptions.set(id, subscription);
    } catch (error) { ids.delete(id); throw error; }
  }
  return {
    async activate(ctx) {
      if (config.listen || config.failActivate)
        await listen(ctx, create(ListenerSpecSchema, { id: 'subscriber', behavior: Behavior.CONTINUE }));
      if (config.failActivate) throw new RpcError('startup_failed', 'after event registration');
    },
    async listen(ctx, args) { await listen(ctx, args); return unit(); },
    async unsubscribe(_ctx, args) {
      const subscription = subscriptions.get(args.value);
      if (!subscription) throw new RpcError('unknown_listener', args.value);
      await subscription.dispose(); return unit();
    },
    async parallel(ctx, args) { return decisions(await controls.tick().parallel(ctx, args)); },
    async serial(ctx, args) { return decision(await controls.tick().serial(ctx, args)); },
    async emit(ctx, args) { current = await controls.tick().emit(ctx, args); return unit(); },
    async join() { return decisions(await dispatch().join()); },
    async status() { const current=dispatch(); const value=await current.snapshot();
      const result=record(value).outcome;
      return snapshot(value,result!==null && record(result).kind==='success' ? await current.join() : []); },
    async count() { return create(UInt64ValueSchema, { value: calls }); },
    async start(ctx) {
      await ctx.spawn('event heartbeat', async worker => {
        let stopping = false;
        void worker.cancelled().then(() => { stopping = true; });
        while (!stopping) {
          await delay(50);
          if (!stopping) await controls.tick().parallel(worker, create(TickValueSchema, { value: 0n }));
        }
      });
      return unit();
    },
    async deactivate(ctx) { await log(logger, ctx, 'events.closed'); },
  };
}
