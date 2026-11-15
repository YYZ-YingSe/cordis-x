import type { Context } from './context.ts';
import type { Interface } from './contract.ts';
import type { Message } from './message.ts';
import type { Resource } from './service.ts';

/** An implementation exported by a generated object dispatcher. */
export interface ReferenceObject extends Resource {
  readonly typedDispatch?: true;
}

/** A callable capability within one host lifetime. Its key is transportable,
 * but is not a persistent service address. Disposal closes the shared object;
 * copying this wrapper does not create another implementation or close duty. */
export class InterfaceRef {
  readonly key: string;
  readonly digest: string;
  constructor(key: string, digest: string) {
    this.key = key;
    this.digest = digest;
  }
  static async export(ctx: Context, descriptor: Interface, digest: string,
    object: ReferenceObject): Promise<InterfaceRef> {
    return new InterfaceRef(await ctx.exportReference(descriptor, object), digest);
  }
  invoke(ctx: Context, method: string, args: Message): Promise<Message> {
    return ctx.invokeReference(this.key, this.digest, method, args);
  }
  dispose(ctx: Context): Promise<void> { return ctx.disposeReference(this.key); }
}
