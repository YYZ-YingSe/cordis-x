import type { Message } from '../../core/message.ts';
import { payload, controlOnly } from '../../protocols/session/messages.ts';
import { Refusal } from '../../core/error.ts';
import type { Context } from '../../core/context.ts';
import type { ResourceHandle } from '../../core/handles.ts';
import type { Value } from '../../core/value.ts';
import { operationOutcome } from '../../protocols/session/outcome.ts';
import type { DependencyChannel } from '../../protocols/session/channel.ts';

type Request = (
  dispose: boolean,
  resource: string,
  grant: string,
  method: string,
  args: Message | undefined,
) => Promise<Message | undefined>;
const contexts = new WeakMap<Context, { channel: DependencyChannel; request: Request }>();

/** Adapter-only access; a structural imitation of Context carries no authority. */
export function bindResourceContext(ctx: Context, channel: DependencyChannel, request: Request) {
  contexts.set(ctx, { channel, request });
}

function object(value: Value): value is Record<string, Value> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}


export async function acquireResource(
  channel: DependencyChannel,
  context: string,
  dependency: string,
  kind: string,
  args: Message | undefined,
): Promise<ResourceHandle> {
  const resource = channel.resourceId();
  const reply = operationOutcome(
    controlOnly(await channel.requestPayload('dependency_resource_acquire', payload({ context, dependency, kind, resource }, args))),
  );
  if (
    !object(reply) ||
    Object.keys(reply).length !== 2 ||
    reply.resource !== resource ||
    typeof reply.grant !== 'string' ||
    !/^[0-9a-f]{32}$/.test(reply.grant)
  )
    throw new Error('invalid resource acquisition acknowledgement');
  return handle(new WeakRef(channel), resource, reply.grant);
}

function handle(session: WeakRef<DependencyChannel>, resource: string, grant: string): ResourceHandle {
  // Only the weak session and opaque capability survive acquisition, never its Context.
  const request = (ctx: Context, dispose: boolean, method: string, args: Message | undefined) => {
    const current = contexts.get(ctx);
    if (!current || session.deref() !== current.channel)
      return Promise.reject(new Refusal('invalid_handle', 'resource belongs to another session'));
    return current.request(dispose, resource, grant, method, args);
  };
  return Object.freeze({
    async invoke(ctx: Context, method: string, args: Message) {
      const result = await request(ctx, false, method, args);
      if (!result) throw new Error('missing resource body');
      return result;
    },
    async dispose(ctx: Context) {
      await request(ctx, true, '', undefined);
    },
  });
}
