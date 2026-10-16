#pragma once
#include "contract.hpp"
#include "protobuf.hpp"
#include "service.hpp"
#include <algorithm>
#include <variant>

namespace cordis::typed {
template <class T> struct Codec {
  static Message encode(const T &value, Validation policy = Validation::Contract) {
    return protobuf::encode_binary(value, policy);
  }
  static T decode(const Message &value, Validation policy = Validation::Contract) {
    return protobuf::decode_binary<T>(value, policy);
  }
};
template <class T> void identity(const Value &shape) {
  if (shape.at("message") != T::descriptor()->full_name())
    throw std::runtime_error("protobuf codec/contract mismatch");
}
template <class T> T input(const Message &value, const Value &shape, Validation policy = Validation::Contract) {
  try {
    identity<T>(shape);
    return Codec<T>::decode(value, policy);
  } catch (const std::exception &error) {
    throw Refusal("invalid_argument", error.what());
  }
}
template <class T> T result(const Message &value, const Value &shape, Validation policy = Validation::Contract) {
  identity<T>(shape);
  return Codec<T>::decode(value, policy);
}
template <class T> Message argument(T value, const Value &shape, Validation policy = Validation::Contract) {
  try {
    identity<T>(shape);
    auto encoded = Codec<T>::encode(std::move(value), policy);
    return encoded;
  } catch (const std::exception &error) {
    throw Refusal("invalid_argument", error.what());
  }
}
template <class T> Message output(T value, const Value &shape, Validation policy = Validation::Contract) {
  identity<T>(shape);
  auto encoded = Codec<T>::encode(std::move(value), policy);
  return encoded;
}
template <class T> T json_input(const Value &value, const Value &shape) {
  try {
    identity<T>(shape);
    return protobuf::decode<T>(value);
  } catch (const std::exception &e) {
    throw Refusal("invalid_argument", e.what());
  }
}
template <class F>
auto checked(const Value &errors, F call) -> decltype(call()) {
  try {
    co_return co_await call();
  } catch (const Refusal &) {
    throw;
  } catch (const RpcError &error) {
    if (error.code.empty() ||
        (std::find(errors.begin(), errors.end(), error.code) == errors.end() &&
         std::find(errors.begin(), errors.end(), "*") == errors.end()))
      throw std::runtime_error("undeclared business error: " + error.code);
    throw;
  }
}
// These are non-member coroutines. They own every argument before first
// suspend.
template <class Args, class Result>
Async<Result> invoke(Context ctx, std::string dependency, std::string method,
                     Args args, Value args_shape, Value result_shape,
                     Value errors) {
  auto encoded = argument(std::move(args), args_shape, ctx.validation());
  auto value = co_await checked(errors, [&] {
    return ctx.invoke(dependency, method, std::move(encoded));
  });
  co_return result<Result>(value, result_shape, ctx.validation());
}
template <class Args, class Result>
Async<Result> resource_call(ResourceHandle handle, Context ctx,
                            std::string method, Args args, Value args_shape,
                            Value result_shape, Value errors) {
  auto encoded = argument(std::move(args), args_shape, ctx.validation());
  auto value = co_await checked(
      errors, [&] { return handle.invoke(ctx, method, std::move(encoded)); });
  co_return result<Result>(value, result_shape, ctx.validation());
}
template <class Result, class Args>
Async<Result> acquire(Context ctx, std::string dependency, std::string kind,
                      Args args, Value args_shape, Value errors) {
  auto encoded = argument(std::move(args), args_shape, ctx.validation());
  auto handle = co_await checked(errors, [&] {
    return ctx.acquire(dependency, kind, std::move(encoded));
  });
  co_return Result(std::move(handle));
}
template <class T>
Async<void> activate(std::shared_ptr<T> server, Context ctx) {
  co_await server->activate(std::move(ctx));
}
template <class T>
Async<void> deactivate(std::shared_ptr<T> server, Context ctx) {
  co_await server->deactivate(std::move(ctx));
}
template <class T> Async<void> close(std::shared_ptr<T> server, Context ctx) {
  co_await server->close(std::move(ctx));
}
} // namespace cordis::typed

#include "typed/controls.hpp"
