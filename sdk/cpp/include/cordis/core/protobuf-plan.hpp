#pragma once
#include "validation.hpp"
#include <algorithm>
#include <cmath>
#include <concepts>
#include <cordis/options.pb.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/message.h>
#include <optional>
#include <stdexcept>
#include <unordered_map>
#include <utf8_validity.h>
#include <vector>

// Windows headers define GetMessage as an object-like macro. Keep the
// reflection method name intact without changing the includer's macro state.
#pragma push_macro("GetMessage")
#undef GetMessage

namespace cordis::protobuf {
inline constexpr std::size_t max_bytes = 1024 * 1024;
inline constexpr std::size_t max_nodes = 65536;
inline constexpr std::size_t max_depth = 64;
using Message = google::protobuf::Message;
using Field = google::protobuf::FieldDescriptor;
inline const Field *annotation(const Message &options, const char *name) {
  const auto *field =
      google::protobuf::DescriptorPool::generated_pool()->FindExtensionByName(
          name);
  return field && options.GetReflection()->HasField(options, field) ? field
                                                                    : nullptr;
}
inline bool flag(const Message &options, const char *name) {
  const auto *field = annotation(options, name);
  return field && options.GetReflection()->GetBool(options, field);
}
inline int semantics(const google::protobuf::Descriptor *descriptor) {
  const auto &options = descriptor->options();
  const auto *field = annotation(options, "cordis.schema.semantics");
  return field ? options.GetReflection()->GetEnumValue(options, field) : 0;
}
inline void take(std::size_t depth, std::size_t &budget) {
  if (depth > max_depth || !budget)
    throw std::runtime_error("protobuf depth/node limit exceeded");
  --budget;
}

// Owns a finite descriptor graph's checking metadata, never a checked DTO.
// Descriptor lifetime is supplied by generated code or the owning Schema.
class Plan {
  struct Step {
    const Field *field;
    Field::CppType kind;
    bool required, explicit_presence, repeated, text;
    std::optional<std::uint32_t> exact;
    std::optional<std::int64_t> minimum, maximum;
    std::vector<int> enum_values;
    std::size_t child = 0;
  };
  struct Node {
    const google::protobuf::Descriptor *descriptor;
    std::vector<const google::protobuf::OneofDescriptor *> selected;
    std::vector<Step> fields;
  };
  std::vector<Node> nodes_;

  void leaf(const Message &message, const Step &step, int index,
            std::size_t depth, std::size_t &budget, std::size_t &bytes,
            Validation policy, bool decoded) const {
    const auto *r = message.GetReflection();
    const auto *f = step.field;
    const bool repeated = index >= 0;
    if (step.kind == Field::CPPTYPE_MESSAGE) {
      visit(step.child,
            repeated ? r->GetRepeatedMessage(message, f, index)
                     : r->GetMessage(message, f),
            depth, budget, bytes, policy, decoded);
      return;
    }
    take(depth, budget);
    switch (step.kind) {
    case Field::CPPTYPE_FLOAT:
      if (!std::isfinite(repeated ? r->GetRepeatedFloat(message, f, index)
                                  : r->GetFloat(message, f)))
        throw std::runtime_error("non-finite float32");
      break;
    case Field::CPPTYPE_DOUBLE:
      if (!std::isfinite(repeated ? r->GetRepeatedDouble(message, f, index)
                                  : r->GetDouble(message, f)))
        throw std::runtime_error("non-finite float64");
      break;
    case Field::CPPTYPE_ENUM: {
      const auto value = repeated ? r->GetRepeatedEnumValue(message, f, index)
                                  : r->GetEnumValue(message, f);
      if (!std::binary_search(step.enum_values.begin(), step.enum_values.end(),
                              value))
        throw std::runtime_error("undeclared enum value");
      break;
    }
    case Field::CPPTYPE_STRING: {
      std::string scratch;
      const auto &text =
          repeated ? r->GetRepeatedStringReference(message, f, index, &scratch)
                   : r->GetStringReference(message, f, &scratch);
      if (text.size() > bytes)
        throw std::runtime_error("message byte limit exceeded");
      bytes -= text.size();
      if (!decoded && step.text && !utf8_range::IsStructurallyValid(text))
        throw std::runtime_error("invalid Unicode string");
      break;
    }
    default:
      break;
    }
  }
  void visit(std::size_t node, const Message &message, std::size_t depth,
             std::size_t &budget, std::size_t &bytes, Validation policy,
             bool decoded) const {
    take(depth, budget);
    const auto *r = message.GetReflection();
    if (policy == Validation::Audit && !r->GetUnknownFields(message).empty())
      throw std::runtime_error("undeclared protobuf field");
    for (const auto *oneof : nodes_[node].selected)
      if (!r->HasOneof(message, oneof))
        throw std::runtime_error("no branch selected");
    for (const auto &step : nodes_[node].fields) {
      const auto *f = step.field;
      if (step.required && !r->HasField(message, f))
        throw std::runtime_error("required field missing: " +
                                 std::string(f->full_name()));
      if (step.explicit_presence && !r->HasField(message, f))
        continue;
      const auto count = step.repeated ? r->FieldSize(message, f) : 0;
      if (step.exact) {
        std::size_t actual;
        if (step.repeated)
          actual = static_cast<std::size_t>(count);
        else {
          std::string scratch;
          const auto &text = r->GetStringReference(message, f, &scratch);
          actual = text.size();
          if (step.text) {
            actual = 0;
            for (unsigned char c : text)
              if ((c & 0xc0) != 0x80)
                ++actual;
          }
        }
        if (actual != *step.exact)
          throw std::runtime_error("fixed size mismatch: " +
                                   std::string(f->full_name()));
      }
      const auto lo = step.minimum, hi = step.maximum;
      if (lo || hi) {
        if (step.kind == Field::CPPTYPE_UINT32 ||
            step.kind == Field::CPPTYPE_UINT64) {
          const auto n = step.kind == Field::CPPTYPE_UINT32
                             ? r->GetUInt32(message, f)
                             : r->GetUInt64(message, f);
          if (lo && *lo > 0 && n < static_cast<std::uint64_t>(*lo))
            throw std::runtime_error("integer below minimum");
          if (hi && (*hi < 0 || n > static_cast<std::uint64_t>(*hi)))
            throw std::runtime_error("integer above maximum");
        } else {
          const auto n = step.kind == Field::CPPTYPE_INT32
                             ? r->GetInt32(message, f)
                             : r->GetInt64(message, f);
          if (lo && n < *lo)
            throw std::runtime_error("integer below minimum");
          if (hi && n > *hi)
            throw std::runtime_error("integer above maximum");
        }
      }
      if (step.repeated) {
        for (int j = 0; j < count; ++j)
          leaf(message, step, j, depth + 1, budget, bytes, policy, decoded);
      } else
        leaf(message, step, -1, depth + 1, budget, bytes, policy, decoded);
    }
  }

public:
  explicit Plan(const google::protobuf::Descriptor *root) {
    std::vector<const google::protobuf::Descriptor *> pending{root};
    std::unordered_map<const google::protobuf::Descriptor *, std::size_t>
        indexes{{root, 0}};
    // No recursive construction or pointer cache beyond the descriptor owner.
    for (std::size_t i = 0; i < pending.size(); ++i) {
      const auto *descriptor = pending[i];
      Node node{descriptor, {}, {}};
      const bool selected = semantics(descriptor) != 0 ||
                            descriptor->full_name() == "google.protobuf.Value";
      for (int n = 0; n < descriptor->real_oneof_decl_count(); ++n) {
        const auto *oneof = descriptor->oneof_decl(n);
        if (selected || flag(oneof->options(), "cordis.schema.selected"))
          node.selected.push_back(oneof);
      }
      for (int n = 0; n < descriptor->field_count(); ++n) {
        const auto *field = descriptor->field(n);
        const auto &options = field->options();
        Step step{field,
                  field->cpp_type(),
                  flag(options, "cordis.schema.required"),
                  field->has_presence(),
                  field->is_repeated(),
                  field->type() == Field::TYPE_STRING,
                  {},
                  {},
                  {},
                  {},
                  0};
        if (const auto *size = annotation(options, "cordis.schema.exact_size"))
          step.exact = options.GetReflection()->GetUInt32(options, size);
        if (const auto *lo = annotation(options, "cordis.schema.minimum"))
          step.minimum = options.GetReflection()->GetInt64(options, lo);
        if (const auto *hi = annotation(options, "cordis.schema.maximum"))
          step.maximum = options.GetReflection()->GetInt64(options, hi);
        if (step.kind == Field::CPPTYPE_ENUM) {
          for (int j = 0; j < field->enum_type()->value_count(); ++j)
            step.enum_values.push_back(field->enum_type()->value(j)->number());
          std::sort(step.enum_values.begin(), step.enum_values.end());
        }
        if (step.kind == Field::CPPTYPE_MESSAGE) {
          const auto [where, added] =
              indexes.emplace(field->message_type(), pending.size());
          step.child = where->second;
          if (added)
            pending.push_back(field->message_type());
        }
        node.fields.push_back(std::move(step));
      }
      nodes_.push_back(std::move(node));
    }
  }
  void validate(const Message &message, Validation policy = Validation::Audit,
                bool decoded = false) const {
    if (policy == Validation::Contract)
      return;
    if (message.GetDescriptor() != nodes_[0].descriptor)
      throw std::runtime_error("validation schema mismatch");
    std::size_t budget = max_nodes, bytes = max_bytes;
    visit(0, message, 0, budget, bytes, policy, decoded);
  }
};

// Borrowed dynamic descriptors have no global lifetime guarantee. Their owner
// (Schema) retains a Plan; a standalone validation builds a scoped plan.
inline void validate(const Message &message,
                     Validation policy = Validation::Audit,
                     bool decoded = false) {
  if (policy != Validation::Contract)
    Plan(message.GetDescriptor()).validate(message, policy, decoded);
}
template <class T>
  requires std::derived_from<T, Message> && requires { T::descriptor(); }
inline void validate(const T &message, Validation policy = Validation::Audit,
                     bool decoded = false) {
  if (policy == Validation::Contract)
    return;
  static const Plan plan(T::descriptor());
  plan.validate(message, policy, decoded);
}
} // namespace cordis::protobuf
#pragma pop_macro("GetMessage")
