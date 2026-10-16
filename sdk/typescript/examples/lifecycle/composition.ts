import { create } from '@bufbuild/protobuf';
import { RpcError } from '../../src/index.ts';
import type { Context, Child } from '../../src/index.ts';
import { ComposerControls as controls, type ComposerServer } from '@cordis-x/example-contracts/lifecycle';
import { CompositionConfig_Publication as Publication } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/composition';
import type { CompositionConfig } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/composition';
import { WorkerConfigSchema, SettingsSchema } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/common';
import type { WorkerConfig } from '@cordis-x/example-contracts/lifecycle/messages/lifecycle/common';
import { childStatus, unit } from './common.ts';
export function composer(config: CompositionConfig): ComposerServer {
  const names = new Set<string>();
  const children = new Map<string, Child>();
  function child(name: string) {
    const found = children.get(name);
    if (!found) throw new RpcError('unknown_child', name);
    return found;
  }
  async function install(ctx: Context, args: WorkerConfig) {
    const name = args.name!;
    if (names.has(name)) throw new RpcError('duplicate_child', name);
    names.add(name);
    try {
      const value = await controls.worker().install(ctx, args);
      children.set(name, value);
      return value;
    } catch (error) { names.delete(name); throw error; }
  }
  return {
    async activate(ctx) {
      if (config.publication !== Publication.OMIT)
        await controls.settings().provide(ctx, create(SettingsSchema, { answer: 42 }));
      if (config.publication === Publication.DUPLICATE)
        await controls.settings().provide(ctx, create(SettingsSchema, { answer: 43 }));
      if (config.failActivate) {
        await controls.worker().install(ctx, create(WorkerConfigSchema, { name: 'startup' }));
        throw new RpcError('startup_failed', 'after child installation');
      }
    },
    async install(ctx, args) { return childStatus.fromJson(await (await install(ctx, args)).ready()); },
    async install_pending(ctx, args) { await install(ctx, args); return unit(); },
    async release(_ctx, args) { await child(args.value).dispose(); return unit(); },
    async ready(_ctx, args) { return childStatus.fromJson(await child(args.value).ready()); },
    async settled(_ctx, args) { return childStatus.fromJson(await child(args.value).settled()); },
    async status(_ctx, args) { return childStatus.fromJson(await child(args.value).status()); },
  };
}
