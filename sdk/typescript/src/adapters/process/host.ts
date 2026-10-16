import type { Message } from '../../core/message.ts';
import { checkBinary } from '../../core/protobuf.ts';
import { validation } from '../../core/validation.ts';
import { payload as data, one, type Payload } from '../../protocols/session/messages.ts';
import { encodeFrame } from '../../protocols/session/binary.ts';
import { Refusal } from '../../core/error.ts';
import { readFrames, writeFrame } from '../../transports/stdio.ts';
import type { Service } from '../../core/service.ts';
import { normalizeInterface, compareInterfaces, operation } from '../../core/contract.ts';
import { MiddlewareRegistry, Next } from './middleware.ts';
export { Next, MiddlewareRegistration } from './middleware.ts';
export type { MiddlewareCallback, TerminalCallback } from './middleware.ts';
import { EventRegistry } from './events.ts';
export { Continue, Break, Subscription, Dispatch, EventDispatchError } from './events.ts';
export type { EventCallback, EventFlow } from './events.ts';
import { TaskRegistry } from './task.ts';
export { Task } from './task.ts';
export type { TaskCallback } from './task.ts';
import { SequenceRegistry } from './sequence.ts';
import { ScopeRegistry } from './scope.ts';
export { Scope } from './scope.ts';
export type { ScopeCallback } from './scope.ts';
export { EffectSequence, Inverse, iterate } from './sequence.ts';
export type { EffectStep, EffectProducer, EffectCompletion } from './sequence.ts';
import { listen } from '../../protocols/http/server.ts';
import { MAX_MESSAGE, validate } from '../../core/value.ts';
import type { Value } from '../../core/value.ts';
import { ResourceRecord, ResourceClosed } from './resources.ts';
import { Context, DependencyChannel, EffectRegistry, RpcError } from './context.ts';
export { Context, Effect, RpcError } from './context.ts';
import type { Resource } from './resources.ts';
import { checkComposition, emptyComposition } from './composition.ts';
import type { Composition } from './composition.ts';
export { Child } from './composition.ts';
export type { Composition } from './composition.ts';
export type { Value } from '../../core/value.ts';
export type { Resource } from './resources.ts';

import type { Identity, Request, Response } from '../../protocols/session/messages.ts';
const decimal = (value: unknown): value is string =>
  typeof value === 'string' &&
  /^(0|[1-9][0-9]{0,19})$/.test(value) &&
  BigInt(value) <= 18446744073709551615n;
const sameIdentity = (a: Identity, b: Identity) =>
  a.incarnation === b.incarnation && a.fiber === b.fiber && a.epoch === b.epoch;
const work = (command: string) =>
  ['call', 'resource_acquire', 'resource_call'].includes(command);
function object(value: Value): Record<string, Value> {
  if (value === null || Array.isArray(value) || typeof value !== 'object')
    throw new Refusal('invalid_argument', 'expected object');
  return value;
}

/** Session over protobuf frames, with either stdio or HTTP transport. */
export async function run(service: Service, options: { http?: boolean } = {}): Promise<void> {
  const descriptor = normalizeInterface(service.interface);
  // Select the local decoder responsibility once, outside all field loops.
  const check = service.typedDispatch ? (_shape: import('../../core/contract.ts').MessageContract, _bytes: Message) => {}
    : (shape: import('../../core/contract.ts').MessageContract, bytes: Message) => checkBinary(shape, bytes, channel.validation);
  const dependencyInterfaces = Object.fromEntries(Object.entries(service.dependencyInterfaces ?? {}).map(([name, value]) => [name, normalizeInterface(value)]));
  if (JSON.stringify(Object.keys(dependencyInterfaces).sort()) !== JSON.stringify(Object.keys(service.dependencies ?? {}).sort()))
    throw new Error('dependency interface identities differ');
  const input = <T>(check: () => T): T => {
    try { return check(); }
    catch (error) { throw new Refusal('invalid_argument', String(error)); }
  };
  const checkedBusiness = async <T>(errors: string[], call: () => T | Promise<T>): Promise<T> => {
    try { return await call(); }
    catch (error) {
      if (error instanceof RpcError && !(error instanceof Refusal) &&
          (!error.code || (!errors.includes(error.code) && !errors.includes('*'))))
        throw new Error(`undeclared business error: ${error.code}`);
      throw error;
    }
  };
  const composition = checkComposition(service.composition ?? emptyComposition());
  let compositionEnabled = false;
  let session = '',
    activation: Identity | undefined,
    active = false,
    attempted = false,
    inflight = 0;
  const channel = new DependencyChannel();
  const effects = new EffectRegistry();
  let effectsEnabled = false,
    sequencesEnabled = false,
    scopesEnabled = false,
    tasksEnabled = false,
    eventsEnabled = false,
    middlewareEnabled = false;
  const sequences = new SequenceRegistry();
  const scopes = new ScopeRegistry();
  const tasks = new TaskRegistry();
  const eventDeclarations = service.events ?? {};
  const events = new EventRegistry(eventDeclarations);
  const middlewareDeclarations = service.middleware ?? {};
  const middlewares = new MiddlewareRegistry(middlewareDeclarations);
  const lanes = [0, 0, 0, 0, 0, 0, 0, 0, 0];
  const declarations = service.dependencies ?? {};
  const seen = new Set<string>();
  const idle = new Set<() => void>();
  const pending = new Set<Promise<void>>();
  let url: string | null = null;
  const resources = new Map<string, ResourceRecord>();
  let nextResource = 0;
  let fatal: unknown;
  const resourceOperation = async (
    command: string,
    payload: Record<string, Value>,
    ctx: Context,
    body?: Message,
  ): Promise<Payload> => {
    const owner = payload.owner;
    if (typeof owner !== 'string' || !/^[0-9a-f]{32}$/.test(owner))
      throw new Refusal('invalid_argument', 'invalid ownership grant');
    if (command === 'resource_acquire') {
      if (!service.acquire)
        throw new Refusal('unknown_resource_kind', 'service cannot acquire resources');
      if (typeof payload.kind !== 'string')
        throw new Refusal('invalid_argument', 'missing resource kind');
      input(() => check(operation(descriptor.resources, payload.kind as string).args, body!));
      if (nextResource >= 4096) throw new Refusal('resources_exhausted', 'resource history full');
      const id = `r${++nextResource}`;
      const entry = new ResourceRecord(owner);
      entry.kind = payload.kind;
      resources.set(id, entry);
      try {
        entry.commit(await checkedBusiness(operation(descriptor.resources, payload.kind).errors, () => service.acquire!(payload.kind as string, body!, ctx)));
      } catch (error) {
        if (error instanceof RpcError) resources.delete(id);
        throw error;
      }
      return data({ resource: id, owner });
    }
    if (typeof payload.resource !== 'string')
      throw new Refusal('invalid_argument', 'missing resource ID');
    const entry = resources.get(payload.resource);
    if (!entry) throw new Refusal('unknown_resource', 'resource not in this activation');
    if (entry.owner !== owner)
      throw new Refusal('resource_owner_mismatch', 'resource belongs to a different grant');
    if (command === 'resource_release') {
      await entry.close(ctx);
      return data({});
    }
    if (typeof payload.method !== 'string')
      throw new Refusal('invalid_argument', 'missing resource method');
    const signature = input(() => operation(operation(descriptor.resources, entry.kind!).methods, payload.method as string));
    input(() => check(signature.args, body!));
    const result = await checkedBusiness(signature.errors, () => entry.invoke(payload.method as string, body!, ctx));
    check(signature.result, result);
    return data(null, result);
  };
  const failure = (request: Request, code: string, message: string, kind: 'failure' | 'refused'): Response => ({
    session: request.session,
    activation: request.activation,
    operation: request.operation,
    outcome: { kind, error: { code, message } },
  });
  const execute = async (request: Request): Promise<Payload> => {
    const bodies=['call','resource_acquire','resource_call','event_call','middleware_call','middleware_terminal'].includes(request.command)?1:0;
    if(request.payload.messages.length!==bodies) throw new Refusal('invalid_argument','unexpected business message count');
    if (
      Object.keys(request).length !== 5 ||
      !/^[0-9a-f]{32}$/.test(request.session) ||
      !decimal(request.operation) ||
      typeof request.command !== 'string' ||
      !request.activation ||
      Object.keys(request.activation).length !== 3 ||
      !Object.values(request.activation).every(decimal)
    )
      throw new Refusal('protocol', 'invalid envelope');
    if (request.command === 'hello') {
      if (activation)
        throw new Refusal('protocol', 'hello must be first');
      const payload = object(request.payload.control),
        contract = object(payload.contract);
      try { compareInterfaces(descriptor, payload.interface); }
      catch (error) { throw new Refusal('contract_mismatch', String(error)); }
      const expectedInterfaces = object(payload.dependency_interfaces);
      if (Object.keys(expectedInterfaces).length !== Object.keys(dependencyInterfaces).length)
        throw new Refusal('contract_mismatch', 'dependency interface identities differ');
      for (const [name, descriptor] of Object.entries(dependencyInterfaces)) {
        try { compareInterfaces(descriptor, expectedInterfaces[name]); }
        catch (error) { throw new Refusal('contract_mismatch', `dependencies.${name}: ${error}`); }
      }
      if (
        contract.name !== service.contract.name ||
        contract.revision !== service.contract.revision
      )
        throw new Refusal('contract_mismatch', 'exact revision required');
      const expected = object(payload.dependencies ?? {});
      if (
        Object.keys(expected).length !== Object.keys(declarations).length ||
        Object.entries(declarations).some(([name, revision]) => expected[name] !== revision)
      )
        throw new Refusal('dependency_mismatch', 'declared dependencies must match installation');
      const boundEvents = object(payload.events ?? {});
      if (
        Object.keys(boundEvents).length !== Object.keys(eventDeclarations).length ||
        Object.entries(eventDeclarations).some(([alias, contract]) => {
          const binding = object(boundEvents[alias]);
          return (
            binding.name !== contract.name ||
            binding.revision !== contract.revision ||
            !decimal(binding.realm)
          );
        })
      )
        throw new Refusal('event_mismatch', 'event bindings must match installation');
      const boundMiddleware = object(payload.middleware ?? {});
      if (
        Object.keys(boundMiddleware).length !== Object.keys(middlewareDeclarations).length ||
        Object.entries(middlewareDeclarations).some(([alias, contract]) => {
          const binding = object(boundMiddleware[alias]);
          return (
            binding.name !== contract.name ||
            binding.revision !== contract.revision ||
            !decimal(binding.realm)
          );
        })
      )
        throw new Refusal('middleware_mismatch', 'middleware bindings must match installation');
      const bound = checkComposition((payload.composition ?? emptyComposition()) as Composition);
      if (JSON.stringify(bound) !== JSON.stringify(composition))
        throw new Refusal('composition_mismatch', 'composition bindings must match installation');
      channel.validation = input(() => validation(payload.validation));
      activation = request.activation;
      compositionEnabled = object(payload.capabilities ?? {}).composition === true;
      session = request.session;
      seen.add(request.operation);
      channel.configure(session, activation, write);
      channel.enableMetadata(object(payload.capabilities ?? {}).metadata === true);
      channel.enableResources(object(payload.capabilities ?? {}).dependency_resources === true);
      effectsEnabled = object(payload.capabilities ?? {}).effects === true;
      sequencesEnabled = object(payload.capabilities ?? {}).effect_sequences === true;
      scopesEnabled = object(payload.capabilities ?? {}).scopes === true;
      tasksEnabled = object(payload.capabilities ?? {}).tasks === true;
      eventsEnabled = object(payload.capabilities ?? {}).events === true;
      middlewareEnabled = object(payload.capabilities ?? {}).middleware === true;
      channel.reserveCleanup(tasksEnabled || eventsEnabled || middlewareEnabled);
      return data({
        contract: service.contract,
        interface: descriptor as unknown as Value,
        dependency_interfaces: dependencyInterfaces as unknown as Value,
        dependencies: declarations,
        events: boundEvents,
        middleware: boundMiddleware,
        composition,
        http: url,
        capabilities: {
          multiplex: true,
          reverse_calls: true,
          effects: true,
          effect_sequences: true,
          scopes: true,
          tasks: true,
          events: true,
          middleware: true,
          composition: true,
          metadata: true,
          streams: false,
          resources: !!service.acquire,
          dependency_resources: object(payload.capabilities ?? {}).dependency_resources === true,
          max_inflight: 16,
          max_message: MAX_MESSAGE,
        },
      });
    }
    if (!activation || request.session !== session || !sameIdentity(request.activation, activation))
      throw new Refusal('stale_session', 'session or activation mismatch');
    if (seen.size >= 65536) throw new Refusal('session_exhausted', 'operation history full');
    if (seen.has(request.operation))
      throw new Refusal('duplicate_operation', 'operation already admitted');
    seen.add(request.operation);
    const payload = request.payload.control === null ? {} : object(request.payload.control);
    const ctx = new Context(
      channel,
      typeof payload.context === 'string' ? payload.context : '',
      declarations,
      effectsEnabled ? effects : undefined,
      [
        'effect_release',
        'resource_release',
        'deactivate',
        'sequence_undo',
        'sequence_release',
        'middleware_release',
      ].includes(request.command),
      sequencesEnabled ? sequences : undefined,
      scopesEnabled ? scopes : undefined,
      tasksEnabled ? tasks : undefined,
      tasksEnabled && request.command === 'task_call' && typeof payload.task === 'string'
        ? tasks.cancellation(payload.task)
        : undefined,
      eventsEnabled ? events : undefined,
      middlewareEnabled ? middlewares : undefined,
      compositionEnabled ? composition : undefined,
    );
    const scoped = async <T>(callback: () => T | Promise<T>): Promise<T> => {
      try {
        return await callback();
      } finally {
        await ctx.finish();
      }
    };
    if (request.command === 'activate') {
      if (attempted) throw new Refusal('protocol', 'activation already attempted');
      attempted = true;
      await scoped(() => service.activate(ctx));
      active = true;
      return data({});
    }
    if (
      ['middleware_call', 'middleware_terminal', 'middleware_release'].includes(request.command)
    ) {
      if (!middlewareEnabled || !attempted) throw new Refusal('closed', 'middleware unavailable');
      if (request.command === 'middleware_release') {
        if (typeof payload.handler !== 'string')
          throw new Refusal('invalid_argument', 'invalid middleware handler');
        middlewares.release(payload.handler);
        return data({});
      }
      if (typeof payload.call !== 'string')
        throw new Refusal('invalid_argument', 'invalid middleware callback');
      if (request.command === 'middleware_terminal') {
        if (typeof payload.run !== 'string')
          throw new Refusal('invalid_argument', 'invalid middleware run');
        return scoped(() =>
          middlewares.terminal(payload.run as string, payload.call as string, one(request.payload), ctx),
        );
      }
      if (
        typeof payload.handler !== 'string' ||
        typeof payload.next !== 'string' ||
        !/^[0-9a-f]{32}$/.test(payload.next) ||
        typeof payload.context !== 'string'
      )
        throw new Refusal('invalid_argument', 'invalid middleware continuation');
      const next = new Next(channel, payload.context, payload.call, payload.next);
      return scoped(() =>
        middlewares.invoke(
          payload.handler as string,
          payload.call as string,
          one(request.payload),
          ctx,
          next,
        ),
      );
    }
    if (request.command === 'event_call' || request.command === 'event_release') {
      if (!eventsEnabled || !attempted || typeof payload.listener !== 'string')
        throw new Refusal('closed', 'event unavailable');
      if (request.command === 'event_release') {
        events.release(payload.listener);
        return data({});
      }
      if (typeof payload.call !== 'string')
        throw new Refusal('invalid_argument', 'invalid event callback');
      return scoped(() =>
        events.invoke(payload.listener as string, payload.call as string, one(request.payload), ctx),
      );
    }
    if (request.command === 'task_call' || request.command === 'task_cancel') {
      if (!tasksEnabled || !attempted || typeof payload.task !== 'string')
        throw new Refusal('closed', 'task unavailable');
      if (request.command === 'task_cancel') {
        tasks.cancel(payload.task);
        return data({});
      }
      return data(await scoped(() => tasks.invoke(payload.task as string, ctx)));
    }
    if (request.command === 'scope_call') {
      if (
        !scopesEnabled ||
        !attempted ||
        typeof payload.scope !== 'string' ||
        typeof payload.run !== 'string'
      )
        throw new Refusal('closed', 'scope callback unavailable');
      return data(await scoped(() => scopes.invoke(payload.scope as string, payload.run as string, ctx)));
    }
    if (['sequence_next', 'sequence_undo', 'sequence_release'].includes(request.command)) {
      if (!sequencesEnabled || !attempted || typeof payload.sequence !== 'string')
        throw new Refusal('closed', 'sequence operation unavailable');
      if (request.command === 'sequence_release') {
        sequences.release(payload.sequence);
        return data({});
      }
      if (typeof payload.step !== 'string') throw new Refusal('invalid_step', 'missing step');
      if (request.command === 'sequence_next')
        return data(await scoped(() => sequences.next(payload.sequence as string, payload.step as string, ctx)));
      await scoped(() => sequences.undo(payload.sequence as string, payload.step as string, ctx));
      return data({});
    }
    if (request.command === 'effect_release') {
      if (!effectsEnabled || !attempted || typeof payload.effect !== 'string')
        throw new Refusal('invalid_argument', 'invalid effect release');
      await scoped(() => effects.release(payload.effect as string, ctx));
      return data({});
    }
    if (request.command === 'deactivate') {
      active = false;
      if (inflight) await new Promise<void>((resolve) => idle.add(resolve));
      if (!middlewares.released)
        throw new Refusal('middleware_remaining', 'middleware not released');
      if (!events.released)
        throw new Refusal('listeners_remaining', 'event subscriptions not released');
      if (!tasks.idle) throw new Refusal('tasks_running', 'task callbacks still running');
      if (!scopes.idle) throw new Refusal('scopes_running', 'scope callbacks still running');
      if (!sequences.released)
        throw new Refusal('effects_remaining', 'sequences not confirmed released');
      if (!effects.released)
        throw new Refusal('effects_remaining', 'caller effects not confirmed released');
      if ([...resources.values()].some((entry) => !entry.released))
        throw new Refusal('resources_remaining', 'caller resources not confirmed released');
      if (attempted) await scoped(() => service.deactivate(ctx));
      return data({});
    }
    if (request.command === 'resource_release') {
      if (!attempted) throw new Refusal('closed', 'resource unavailable');
      return scoped(() => resourceOperation(request.command, payload, ctx));
    }
    if (!work(request.command)) throw new Refusal('unknown_command', request.command);
    if (!active) throw new Refusal('closed', 'activation not accepting calls');
    if (inflight >= 16) throw new Refusal('overloaded', 'call capacity reached');
    if (
      request.command === 'call' &&
      (Object.keys(payload).length !== (Object.hasOwn(payload, 'context') ? 2 : 1) || typeof payload.method !== 'string')
    )
      throw new Refusal('invalid_argument', 'call requires method and args');
    inflight++;
    try {
      if (request.command === 'call') {
        const signature = input(() => operation(descriptor.methods, payload.method as string));
        const args = input(() => one(request.payload));
        input(() => check(signature.args, args));
        const result = await checkedBusiness(signature.errors, () => scoped(() => service.invoke(payload.method as string, args, ctx)));
        check(signature.result, result);
        return data(null, result);
      }
      return await scoped(() => resourceOperation(request.command, payload, ctx, one(request.payload)));
    } finally {
      inflight--;
      if (!inflight) {
        for (const notify of idle) notify();
        idle.clear();
      }
    }
  };
  const dispatch = async (request: Request): Promise<Response> => {
    try {
      if (fatal) throw fatal;
      return {
            session: request.session,
        activation: request.activation,
        operation: request.operation,
        outcome: { kind: 'success', value: await execute(request) },
      };
    } catch (error) {
      if (!(error instanceof RpcError) && !(error instanceof ResourceClosed)) {
        // Session failures have no certainty field. Losing the session preserves
        // unknown responsibility instead of acknowledging a known rejection.
        fatal = error;
        active = false;
        channel.close();
        process.exitCode = 1;
        process.stdin.destroy();
        throw error;
      }
      return failure(
        request,
        error instanceof RpcError
          ? error.code
          : 'closed',
        error instanceof RpcError ? error.message : String(error),
        error instanceof Refusal || error instanceof ResourceClosed ? 'refused' : 'failure',
      );
    }
  };
  const server = await listen(
    options.http !== false,
    { active: () => !!activation, validation: () => channel.validation },
    dispatch,
  );
  url = server.url;
  const write = (bytes: Uint8Array) => writeFrame(process.stdout, bytes);
  const respond = (response: Response) => write(encodeFrame(response));
  let acknowledged = false;
  try {
    for await (const value of readFrames(process.stdin, () => session ? channel.validation : 'audit')) {
      if ('outcome' in value) { channel.receive(value); continue; }
      const request = value;
      if (request.command === 'hello') {
        await respond(await dispatch(request));
        continue;
      }
      const lane =
        request.command === 'middleware_call'
          ? 7
          : request.command === 'middleware_terminal'
            ? 8
            : request.command === 'event_call'
              ? 6
              : request.command === 'task_call'
                ? 4
                : request.command === 'task_cancel'
                  ? 5
                  : request.command === 'scope_call'
                    ? 3
                    : request.command === 'sequence_next'
                      ? 2
                      : [
                            'effect_release',
                            'resource_release',
                            'sequence_undo',
                            'sequence_release',
                            'event_release',
                            'middleware_release',
                          ].includes(request.command)
                        ? 1
                        : 0;
      if (lanes[lane] >= 16) {
        await respond(failure(request, 'overloaded', 'worker queue full', 'refused'));
        continue;
      }
      // Keep reading dependency replies while lifecycle and business callbacks await them.
      lanes[lane]++;
      const task = (async () => {
        const result = await dispatch(request);
        await respond(result);
        if (request.command === 'deactivate') {
          acknowledged = true;
          process.exitCode = result.outcome.kind === 'success' ? 0 : 1;
          process.stdin.destroy();
        }
      })();
      pending.add(task);
      task.then(
        () => {
          pending.delete(task);
          lanes[lane]--;
        },
        () => {
          lanes[lane]--;
          process.exitCode = 1;
          process.stdin.destroy();
        },
      );
    }
    if (fatal) throw fatal;
    if (acknowledged) return;
    throw new Error('EOF without acknowledged deactivation');
  } catch (error) {
    if (fatal) throw fatal;
    if (!acknowledged) throw error;
  } finally {
    channel.close();
    await Promise.allSettled(pending);
    await server.close();
  }
}
