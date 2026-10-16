import { Refusal } from '../../core/error.ts';
import { DependencyChannel, RpcError } from './context.ts';
import type { Value } from '../../core/value.ts';
export { checkComposition, emptyComposition } from '../../core/composition.ts';
export type { Composition } from '../../core/composition.ts';
/** Session-bound capability; parent ownership survives a lost acknowledgement. */
export class Child {
  private channel: DependencyChannel;
  private child: string;
  private grant: string;
  constructor(channel: DependencyChannel, reply: Value) {
    if (
      !reply ||
      typeof reply !== 'object' ||
      Array.isArray(reply) ||
      Object.keys(reply).length !== 2 ||
      typeof reply.child !== 'string' ||
      !/^[0-9a-f]{32}$/.test(reply.child) ||
      typeof reply.grant !== 'string' ||
      !/^[0-9a-f]{32}$/.test(reply.grant)
    )
      throw new Refusal('protocol', 'invalid child acknowledgement');
    this.channel = channel;
    this.child = reply.child;
    this.grant = reply.grant;
  }
  private call(command: string): Promise<Value> {
    return this.channel.request(command, { child: this.child, grant: this.grant });
  }
  ready(): Promise<Value> {
    return this.call('child_ready');
  }
  settled(): Promise<Value> {
    return this.call('child_settled');
  }
  status(): Promise<Value> {
    return this.call('child_status');
  }
  async dispose(): Promise<void> {
    await this.call('child_dispose');
  }
}
