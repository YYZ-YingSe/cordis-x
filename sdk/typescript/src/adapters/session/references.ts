import type { Interface } from '../../core/contract.ts';
import { normalizeInterface, operation } from '../../core/contract.ts';
import type { ReferenceObject } from '../../core/reference.ts';
import type { Context } from './context.ts';
import type { Message } from '../../core/message.ts';
import { Refusal, RpcError } from '../../core/error.ts';
import { checkBinary } from '../../core/protobuf.ts';
import { checked } from '../../core/typed.ts';
import { Identities, sweepRetired } from './retention.ts';
import { ResourceRecord } from './resources.ts';

/** Adapter-owned implementations share the existing resource drain barrier.
 * Host-issued keys never serve as local callback identifiers. A failed close
 * stays retained so shutdown cannot report that uncertain effects disappeared. */
export class ReferenceRegistry {
  private ids = new Identities();
  private entries = new Map<string, ResourceRecord>();
  get released(): boolean { return [...this.entries.values()].every(e => e.released); }
  async export(descriptor: Interface, object: ReferenceObject,
    publish: (id: string, descriptor: Interface) => Promise<string>): Promise<string> {
    sweepRetired(this.entries, e => e.released);
    const contract = normalizeInterface(descriptor);
    const entry = new ResourceRecord('', {
      async invoke(method, args, call) {
        if (!call) throw new Refusal('closed', 'reference requires a live caller');
        const signature = operation(contract.methods, method);
        if (!object.typedDispatch) checkBinary(signature.args, args, call.validation);
        const result = await checked(signature.errors, async () => object.invoke(method, args, call));
        if (!object.typedDispatch) checkBinary(signature.result, result, call.validation);
        return result;
      },
      close: call => object.close(call),
    });
    const id = this.ids.next('i');
    this.entries.set(id, entry);
    try { return await publish(id, contract); }
    catch (error) {
      // A known refusal never transferred ownership from the exporter. Unknown
      // outcomes retain the callback for eventual host-driven recovery.
      if (error instanceof RpcError) this.entries.delete(id);
      throw error;
    }
  }
  async invoke(id: string, method: string, args: Message, ctx: Context): Promise<Message> {
    const entry = this.entries.get(id);
    if (!entry) throw new Refusal('closed', 'reference implementation retired');
    return entry.invoke(method, args, ctx);
  }
  async close(id: string, ctx: Context): Promise<void> {
    const entry = this.entries.get(id);
    if (!entry) throw new Refusal('closed', 'reference implementation retired');
    await entry.close(ctx);
    this.entries.delete(id);
  }
}
