//! Inspect raw protobuf fields before map/oneof decoding can discard them.
//! Standard prost wire readers handle tags, lengths and skipping. Only declared
//! message fields recurse; bytes and strings stay opaque.
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use prost_reflect::{Kind, MessageDescriptor};

pub fn audit_binary(bytes: &[u8], descriptor: &MessageDescriptor) -> Result<(), String> {
    // Each field visit consumes at least a tag byte. Nested message visits also
    // consume a length byte. This bounds traversal without tightening DTO limits.
    walk(bytes, descriptor, 0, &mut (bytes.len() + 1))
}
fn invalid(message: &str) -> String {
    message.into()
}
fn wire_error(error: impl std::fmt::Display) -> String {
    error.to_string()
}
fn take(remaining: &mut usize) -> Result<(), String> {
    *remaining = remaining
        .checked_sub(1)
        .ok_or_else(|| invalid("protobuf audit node limit exceeded"))?;
    Ok(())
}
fn walk(
    mut bytes: &[u8],
    descriptor: &MessageDescriptor,
    depth: usize,
    remaining: &mut usize,
) -> Result<(), String> {
    if depth > 100 {
        return Err(invalid("protobuf audit nesting limit exceeded"));
    }
    take(remaining)?;
    while !bytes.is_empty() {
        take(remaining)?;
        let (tag, wire) = decode_key(&mut bytes).map_err(wire_error)?;
        let field = descriptor
            .get_field(tag)
            .ok_or_else(|| invalid("undeclared protobuf field"))?;
        let kind = field.kind();
        let expected = match &kind {
            Kind::Double | Kind::Fixed64 | Kind::Sfixed64 => WireType::SixtyFourBit,
            Kind::Float | Kind::Fixed32 | Kind::Sfixed32 => WireType::ThirtyTwoBit,
            Kind::String | Kind::Bytes | Kind::Message(_) => WireType::LengthDelimited,
            _ => WireType::Varint,
        };
        if wire != expected
            && !(field.is_list()
                && !matches!(kind, Kind::String | Kind::Bytes | Kind::Message(_))
                && wire == WireType::LengthDelimited)
        {
            return Err(invalid("undeclared protobuf wire type"));
        }
        if let Kind::Message(nested) = kind {
            let length = usize::try_from(decode_varint(&mut bytes).map_err(wire_error)?)
                .map_err(wire_error)?;
            if length > bytes.len() {
                return Err(invalid("truncated protobuf message"));
            }
            let (body, rest) = bytes.split_at(length);
            walk(body, &nested, depth + 1, remaining)?;
            bytes = rest;
        } else {
            skip_field(wire, tag, &mut bytes, DecodeContext::default()).map_err(wire_error)?;
        }
    }
    Ok(())
}
