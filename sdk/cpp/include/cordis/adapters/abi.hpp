#pragma once
#include "abi/core.hpp"
#include "abi/managed.hpp"
#include "context.hpp"
#include "model.hpp"
#include <cordis/cordis.hpp>

namespace cordis::abi {
namespace detail {
class ResourceBridge final : public ContextResource {
  std::shared_ptr<cordis::Resource> resource;

public:
  explicit ResourceBridge(std::shared_ptr<cordis::Resource> resource)
      : resource(std::move(resource)) {
    if (!this->resource)
      throw std::runtime_error("null resource");
  }
  Async<Message> invoke(Context ctx, std::string method,
                        Message args) override {
    co_return co_await resource->invoke(adapters::context(ctx), method, args);
  }
  Async<void> release(Context ctx) override {
    co_await resource->close(adapters::context(ctx));
  }
};
class ServiceBridge final : public ContextService {
  std::shared_ptr<cordis::Service> service;

public:
  explicit ServiceBridge(std::shared_ptr<cordis::Service> service)
      : service(std::move(service)) {
    if (!this->service)
      throw std::runtime_error("null service");
  }
  Async<void> activate(Context ctx) override {
    co_await service->activate(adapters::context(ctx));
  }
  Async<Message> invoke(Context ctx, std::string method,
                        Message args) override {
    co_return co_await service->invoke(adapters::context(ctx), method, args);
  }
  Async<std::shared_ptr<ContextResource>> acquire(Context ctx, std::string kind,
                                                  Message args) override {
    co_return std::make_shared<ResourceBridge>(
        co_await service->acquire(adapters::context(ctx), kind, args));
  }
  Async<void> deactivate(Context ctx) override {
    co_await service->deactivate(adapters::context(ctx));
  }
};
template <class Model> struct ModelBridge {
  static std::string contract_name() { return Model::contract_name(); }
  static std::string contract_revision() { return Model::contract_revision(); }
  static Value dependencies() { return Model::dependencies(); }
  static Value interface() { return Model::interface(); }
  static Value dependency_interfaces() {
    return Model::dependency_interfaces();
  }
  static Value bindings() {
    return {{"events", adapters::Events<Model>::get()},
            {"middleware", adapters::Middleware<Model>::get()}};
  }
  static Value composition() { return adapters::Composition<Model>::get(); }
  static std::shared_ptr<ContextService> create(Value config) {
    return std::make_shared<ServiceBridge>(Model::create(config));
  }
};
} // namespace detail
template <class Model> using Plugin = ContextHost<detail::ModelBridge<Model>>;
} // namespace cordis::abi

// Only entry points know the C ABI. The business model includes cordis.hpp.
#define CORDIS_ABI_PLUGIN(Model)                                               \
  extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_v1(                         \
      uint32_t version, CordisAbiApi *out, size_t size) {                      \
    return ::cordis::abi::Plugin<Model>::entry(version, out, size);            \
  }                                                                            \
  extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_context_v1(                 \
      uint32_t version, CordisAbiContextApi *out, size_t size) {               \
    return ::cordis::abi::Plugin<Model>::context_entry(version, out, size);    \
  }                                                                            \
  extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_managed_v1(                 \
      uint32_t version, CordisAbiManagedApi *out, size_t size) {               \
    return ::cordis::abi::Plugin<Model>::managed_entry(version, out, size);    \
  }                                                                            \
  extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_reactive_v1(                \
      uint32_t version, CordisAbiReactiveApi *out, size_t size) {              \
    return ::cordis::abi::Plugin<Model>::reactive_entry(version, out, size);   \
  }                                                                            \
  extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_composition_v1(             \
      uint32_t version, CordisAbiCompositionApi *out, size_t size) {           \
    return ::cordis::abi::Plugin<Model>::composition_entry(version, out,       \
                                                           size);              \
  }                                                                            \
  extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_resources_v1(               \
      uint32_t version, CordisAbiResourceApi *out, size_t size) {              \
    return ::cordis::abi::Plugin<Model>::resource_entry(version, out, size);   \
  }
