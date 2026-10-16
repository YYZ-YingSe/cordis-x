import { Refusal } from '../../core/error.ts';
import { RpcError } from './context.ts';
import type { Context, DependencyChannel } from './context.ts';
import type { Value } from '../../core/value.ts';

import type { TaskCallback } from '../../core/handles.ts';
export type { TaskCallback } from '../../core/handles.ts';
/** A cached join capability. Dropping a handle does not stop its task. */
export class Task {
  private channel: DependencyChannel;
  private task: string;
  private grant: string;
  constructor(channel: DependencyChannel, task: string, grant: string) {
    this.channel = channel;
    this.task = task;
    this.grant = grant;
  }
  async join(): Promise<void> {
    await this.channel.request('task_join', { task: this.task, grant: this.grant });
  }
  snapshot(): Promise<Value> {
    return this.channel.request('task_status', { task: this.task, grant: this.grant });
  }
}

export class TaskCancellation {
  readonly promise: Promise<void>;
  private resolve!: () => void;
  constructor() {
    this.promise = new Promise<void>((resolve) => {
      this.resolve = resolve;
    });
  }
  cancel() {
    this.resolve();
  }
}
type Entry = {
  callback?: TaskCallback;
  started: boolean;
  running: boolean;
  cancellation: TaskCancellation;
};
export class TaskRegistry {
  private entries = new Map<string, Entry>();
  private next = 0;
  async register(
    channel: DependencyChannel,
    context: string,
    label: string,
    callback: TaskCallback,
  ): Promise<Task> {
    if (Buffer.byteLength(label) > 1024)
      throw new Refusal('invalid_argument', 'task label too long');
    if (this.next >= 4096) throw new Refusal('tasks_exhausted', 'task history full');
    const task = `t${++this.next}`;
    const entry: Entry = {
      callback,
      started: false,
      running: false,
      cancellation: new TaskCancellation(),
    };
    // A callback or cancellation may arrive before the registration acknowledgement.
    this.entries.set(task, entry);
    const reply = await channel.request('task_register', { context, task, label });
    if (
      !reply ||
      typeof reply !== 'object' ||
      Array.isArray(reply) ||
      reply.task !== task ||
      typeof reply.grant !== 'string' ||
      !/^[0-9a-f]{32}$/.test(reply.grant)
    )
      throw new Refusal('protocol', 'invalid task registration');
    return new Task(channel, task, reply.grant);
  }
  cancellation(task: string): TaskCancellation {
    const entry = this.entries.get(task);
    if (!entry) throw new Refusal('closed', 'task unavailable');
    return entry.cancellation;
  }
  cancel(task: string) {
    this.cancellation(task).cancel();
  }
  async invoke(task: string, ctx: Context): Promise<Value> {
    const entry = this.entries.get(task);
    if (!entry || entry.started || !entry.callback)
      throw new Refusal('closed', 'task callback unavailable');
    entry.started = entry.running = true;
    const callback = entry.callback;
    try {
      try {
        await callback(ctx);
        return { kind: 'success', value: null };
      } catch (error) {
        return {
          kind: error instanceof Refusal ? 'refused' : 'failure',
          error: {
            code: error instanceof RpcError ? error.code : 'task_failed',
            message: error instanceof Error ? error.message : String(error),
          },
        };
      }
    } finally {
      await ctx.finish();
      entry.callback = undefined;
      entry.running = false;
    }
  }
  get idle(): boolean {
    return [...this.entries.values()].every((entry) => !entry.running);
  }
}
