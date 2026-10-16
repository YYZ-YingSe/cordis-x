#pragma once
#include <cordis/core/value.hpp>
#include <type_traits>

namespace cordis::adapters {
template <class Model, class = void> struct Events {
  static Value get() { return Value::object(); }
};
template <class Model>
struct Events<Model, std::void_t<decltype(Model::events())>> {
  static Value get() { return Model::events(); }
};
template <class Model, class = void> struct Middleware {
  static Value get() { return Value::object(); }
};
template <class Model>
struct Middleware<Model, std::void_t<decltype(Model::middleware())>> {
  static Value get() { return Model::middleware(); }
};
template <class Model, class = void> struct Composition {
  static Value get() {
    return {{"factories", Value::array()}, {"provisions", Value::object()}};
  }
};
template <class Model>
struct Composition<Model, std::void_t<decltype(Model::composition())>> {
  static Value get() { return Model::composition(); }
};
} // namespace cordis::adapters
