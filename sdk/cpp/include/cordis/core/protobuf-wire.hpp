#pragma once
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format.h>
#include <stdexcept>

namespace cordis::protobuf {
// A parsed map/oneof may have discarded input fields. Audit raw boundaries with
// protobuf's wire reader; never interpret a bytes field as a business message.
inline void audit_binary(const uint8_t *data, int length,
                          const google::protobuf::Descriptor *descriptor,
                          unsigned depth, unsigned &remaining) {
  using Wire = google::protobuf::internal::WireFormatLite;
  if (depth > 100 || remaining == 0)
    throw std::runtime_error("protobuf audit traversal limit exceeded");
  --remaining;
  google::protobuf::io::CodedInputStream input(data, length);
  while (input.CurrentPosition() < length) {
    if (remaining == 0)
      throw std::runtime_error("protobuf audit node limit exceeded");
    --remaining;
    const auto tag = input.ReadTag();
    const auto *field = descriptor->FindFieldByNumber(Wire::GetTagFieldNumber(tag));
    if (!tag || !field)
      throw std::runtime_error("undeclared protobuf field");
    const auto wire = Wire::GetTagWireType(tag);
    const auto expected = google::protobuf::internal::WireFormat::WireTypeForFieldType(field->type());
    if (wire != expected && !(field->is_packable() && wire == Wire::WIRETYPE_LENGTH_DELIMITED))
      throw std::runtime_error("undeclared protobuf wire type");
    if (field->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
      uint32_t size;
      if (!input.ReadVarint32(&size) || size > static_cast<unsigned>(length - input.CurrentPosition()))
        throw std::runtime_error("truncated protobuf message");
      audit_binary(data + input.CurrentPosition(), static_cast<int>(size),
                    field->message_type(), depth + 1, remaining);
      if (!input.Skip(static_cast<int>(size)))
        throw std::runtime_error("truncated protobuf message");
    } else if (!Wire::SkipField(&input, tag)) {
      throw std::runtime_error("invalid protobuf field");
    }
  }
}
inline void audit_binary(const uint8_t *data, int length,
                          const google::protobuf::Descriptor *descriptor) {
  if (length < 0) throw std::runtime_error("invalid protobuf byte length");
  unsigned remaining = static_cast<unsigned>(length) + 1;
  audit_binary(data, length, descriptor, 0, remaining);
}
} // namespace cordis::protobuf
