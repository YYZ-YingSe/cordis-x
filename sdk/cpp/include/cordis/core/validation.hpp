#pragma once
#include <stdexcept>
#include <string_view>
namespace cordis {
enum class Validation { Contract, Checked, Audit };
inline Validation validation(std::string_view value) {
  if (value == "contract") return Validation::Contract;
  if (value == "checked") return Validation::Checked;
  if (value == "audit") return Validation::Audit;
  throw std::runtime_error("invalid validation policy");
}
} // namespace cordis
