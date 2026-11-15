#pragma once
#include "validation.hpp"
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>

namespace cordis {
// Owned immutable protobuf body. A receiver still validates it against the
// operation's contract; bytes from another domain carry no validation proof.
class Message {
  std::shared_ptr<const std::string> bytes_;

public:
  explicit Message(std::string bytes)
      : bytes_(std::make_shared<const std::string>(std::move(bytes))) {
    if (bytes_->size() > static_cast<std::size_t>(std::numeric_limits<int>::max()))
      throw std::runtime_error("message exceeds protobuf's representable size");
  }
  const std::string &bytes() const { return *bytes_; }
};
} // namespace cordis
