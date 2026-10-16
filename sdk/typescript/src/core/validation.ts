/** Host-owned binding policy, never a claim supplied by business messages. */
export type Validation = 'contract' | 'checked' | 'audit';
export function validation(value: unknown): Validation {
  if (value === 'contract' || value === 'checked' || value === 'audit') return value;
  throw new Error('invalid validation policy');
}
