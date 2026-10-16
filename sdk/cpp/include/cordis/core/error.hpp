#pragma once
#include <stdexcept>
#include <string>
#include <utility>
namespace cordis {
struct RpcError : std::runtime_error {
  std::string code;
  RpcError(std::string code, std::string message)
      : std::runtime_error(std::move(message)), code(std::move(code)) {}
};

struct Refusal : RpcError { using RpcError::RpcError; };

} // namespace cordis
