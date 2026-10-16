import { type DescMessage, ScalarType } from '@bufbuild/protobuf';
import { BinaryReader, WireType } from '@bufbuild/protobuf/wire';

type Field = { wire: WireType; nested?: Map<number, Field>; packed?: boolean };
const plans = new WeakMap<DescMessage, Map<number, Field>>();
function field(message?: DescMessage, scalar?: ScalarType, packed = false): Field {
  const wire = message || scalar === ScalarType.STRING || scalar === ScalarType.BYTES ? WireType.LengthDelimited
    : scalar === ScalarType.DOUBLE || scalar === ScalarType.FIXED64 || scalar === ScalarType.SFIXED64 ? WireType.Bit64
    : scalar === ScalarType.FLOAT || scalar === ScalarType.FIXED32 || scalar === ScalarType.SFIXED32 ? WireType.Bit32
    : WireType.Varint;
  return { wire, nested: message ? plan(message) : undefined, packed };
}
function plan(schema: DescMessage): Map<number, Field> {
  const found = plans.get(schema);
  if (found) return found;
  const fields = new Map<number, Field>();
  plans.set(schema, fields); // Recursive message descriptions share the same plan.
  for (const desc of schema.fields) {
    fields.set(desc.number, desc.fieldKind === 'map'
      ? { wire: WireType.LengthDelimited, nested: new Map([
        [1, field(undefined, desc.mapKey)], [2, field(desc.message, desc.scalar)],
      ]) }
      : field(desc.message, desc.scalar, desc.fieldKind === 'list' && !desc.message && desc.scalar !== ScalarType.STRING && desc.scalar !== ScalarType.BYTES));
  }
  return fields;
}
/** Inspect original fields before maps/oneofs can discard them. No business decoding. */
export function auditBinary(schema: DescMessage, bytes: Uint8Array): void {
  let remaining = bytes.byteLength + 1; // Every field/message visit consumes a tag/length byte.
  const walk = (fields: Map<number, Field>, bytes: Uint8Array, depth: number): void => {
    if (depth > 100 || --remaining < 0) throw new Error('protobuf audit traversal limit exceeded');
    const reader = new BinaryReader(bytes);
    while (reader.pos < reader.len) {
      if (--remaining < 0) throw new Error('protobuf audit node limit exceeded');
      const [number, wire] = reader.tag();
      const field = fields.get(number);
      if (!field) throw new Error('undeclared protobuf field');
      if (wire !== field.wire && !(field.packed && wire === WireType.LengthDelimited))
        throw new Error('undeclared protobuf wire type');
      if (field.nested) walk(field.nested, reader.bytes(), depth + 1);
      else reader.skip(wire, number);
    }
  };
  walk(plan(schema), bytes, 0);
}
