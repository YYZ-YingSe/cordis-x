export type Value = null | boolean | number | string | Value[] | { [key: string]: Value };
export const MAX_MESSAGE = 1024 * 1024;
export function validate(value: Value, depth = 0): void {
  if (depth > 96) throw new Error('value nesting too deep');
  if (!['object', 'string', 'boolean', 'number'].includes(typeof value))
    throw new Error('unsupported value type');
  if (typeof value === 'number' && !Number.isFinite(value))
    throw new Error('non-finite JSON number');
  if (typeof value === 'string' && !value.isWellFormed()) throw new Error('invalid Unicode string');
  if (value !== null && typeof value === 'object') {
    if (Array.isArray(value)) {
      for (const child of value) validate(child, depth + 1);
      return;
    }
    if (Object.getPrototypeOf(value) !== null && Object.getPrototypeOf(value) !== Object.prototype)
      throw new Error('expected plain value object');
    for (const [key, child] of Object.entries(value)) {
      if (!key.isWellFormed()) throw new Error('invalid Unicode key');
      validate(child, depth + 1);
    }
  }
}
