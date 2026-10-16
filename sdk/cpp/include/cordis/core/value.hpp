#pragma once
#include <nlohmann/json.hpp>

namespace cordis {
// An in-memory portable value tree. This alias selects no wire encoding.
using Value = nlohmann::json;
constexpr std::int64_t max_integer = 9007199254740991LL;
} // namespace cordis
