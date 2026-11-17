import type { Context } from './context.ts';
import type { Message } from '../../core/message.ts';

import type { Resource } from '../../core/service.ts';
export type { Resource } from '../../core/service.ts';
export class ResourceClosed extends Error {}

/** Single-use inverse with a per-resource admission/drain barrier. */
export class ResourceRecord {
  kind?: string;
  owner: string;
  private value: Resource | undefined;
  private calls = 0;
  private idle = new Set<() => void>();
  private closing = false;
  private completion: Promise<void> | undefined;
  released = false;
  constructor(owner: string, value?: Resource) {
    this.owner = owner;
    this.value = value;
  }
  commit(value: Resource) {
    if (!value || typeof value.invoke !== 'function' || typeof value.close !== 'function')
      throw new Error('resource factory returned an invalid object');
    this.value = value;
  }
  async invoke(method: string, args: Message, ctx?: Context): Promise<Message> {
    if (this.closing || !this.value) throw new ResourceClosed('resource is closing or pending');
    this.calls++;
    try {
      return await this.value!.invoke(method, args, ctx);
    } finally {
      await ctx?.finish();
      this.calls--;
      if (!this.calls) {
        for (const resolve of this.idle) resolve();
        this.idle.clear();
      }
    }
  }
  close(ctx?: Context): Promise<void> {
    if (this.completion) return this.completion;
    this.closing = true;
    this.completion = (async () => {
      if (this.calls) await new Promise<void>((resolve) => this.idle.add(resolve));
      if (!this.value) throw new Error('resource acquisition uncertain');
      try {
        await this.value!.close(ctx);
      } finally {
        await ctx?.finish();
      }
      this.value = undefined;
      this.released = true;
    })();
    return this.completion;
  }
}
