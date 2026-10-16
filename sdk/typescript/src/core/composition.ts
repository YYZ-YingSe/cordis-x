import { Refusal } from './error.ts';
import { RpcError } from './error.ts';
const byteLength = (value: string) => new TextEncoder().encode(value).length;
export type Composition = {
  factories: string[];
  provisions: Record<string, { name: string; revision: string }>;
};
export const emptyComposition = (): Composition => ({ factories: [], provisions: {} });
export function checkComposition(value: Composition): Composition {
  if (
    !value ||
    Object.keys(value).length !== 2 ||
    !Array.isArray(value.factories) ||
    new Set(value.factories).size !== value.factories.length ||
    value.factories.some((name) => typeof name !== 'string' || !name || byteLength(name) > 1024) ||
    !value.provisions ||
    Array.isArray(value.provisions) ||
    typeof value.provisions !== 'object'
  )
    throw new Refusal('composition_mismatch', 'invalid composition manifest');
  for (const [alias, contract] of Object.entries(value.provisions)) {
    if (
      !alias ||
      byteLength(alias) > 1024 ||
      !contract ||
      Object.keys(contract).length !== 2 ||
      typeof contract.name !== 'string' ||
      !contract.name ||
      typeof contract.revision !== 'string' ||
      !contract.revision
    )
      throw new Refusal('composition_mismatch', 'invalid provision manifest');
  }
  return {
    factories: [...value.factories].sort(),
    provisions: Object.fromEntries(
      Object.entries(value.provisions)
        .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
        .map(([alias, c]) => [alias, { name: c.name, revision: c.revision }]),
    ),
  };
}
