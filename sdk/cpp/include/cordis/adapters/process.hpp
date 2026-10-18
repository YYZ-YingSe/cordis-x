#pragma once
#include "context.hpp"
#include "model.hpp"
#include "process/core.hpp"
#include "process/host.hpp"
#include <cordis/cordis.hpp>

namespace cordis::process {
namespace detail {
class ResourceBridge final : public Resource {
  std::shared_ptr<cordis::Resource> resource;

public:
  explicit ResourceBridge(std::shared_ptr<cordis::Resource> resource)
      : resource(std::move(resource)) {
    if (!this->resource)
      throw std::runtime_error("null resource");
  }
  Async<Message> invoke(std::string, Message) override {
    co_await corral::noop();
    throw Refusal("context", "resource requires callback authority");
  }
  Async<void> close() override {
    co_await corral::noop();
    throw Refusal("context", "resource requires callback authority");
  }
  Async<Message> invoke(std::string method, Message args,
                        Context ctx) override {
    return resource->invoke(adapters::context(std::move(ctx)),
                            std::move(method), std::move(args));
  }
  Async<void> close(Context ctx) override {
    return resource->close(adapters::context(ctx));
  }
};

template <class Model> class ServiceBridge final : public Service {
  std::shared_ptr<cordis::Service> service;
  Value config_;

public:
  explicit ServiceBridge(const Value &config) : config_(config) {}
  bool typed_dispatch() const override { return service && service->typed_dispatch(); }
  Json interface() const override { return Model::interface(); }
  Json dependency_interfaces() const override {
    return Model::dependency_interfaces();
  }
  Json contract() const override {
    return {{"name", Model::contract_name()},
            {"revision", Model::contract_revision()}};
  }
  Json dependencies() const override { return Model::dependencies(); }
  Json events() const override { return adapters::Events<Model>::get(); }
  Json middleware() const override {
    return adapters::Middleware<Model>::get();
  }
  Json composition() const override {
    return adapters::Composition<Model>::get();
  }
  Async<void> activate(Context ctx) override {
    if (service)
      throw Refusal("already_active", "service already created");
    service = Model::create(std::move(config_));
    if (!service)
      throw std::runtime_error("null service");
    co_await service->activate(adapters::context(ctx));
  }
  Async<Message> invoke(std::string method, Message args,
                        Context ctx) override {
    if (!service)
      throw Refusal("inactive", "service has not been activated");
    return service->invoke(adapters::context(std::move(ctx)), std::move(method),
                           std::move(args));
  }
  Async<std::unique_ptr<Resource>> acquire(std::string kind, Message args,
                                           Context ctx) override {
    if (!service)
      throw Refusal("inactive", "service has not been activated");
    co_return std::make_unique<ResourceBridge>(co_await service->acquire(
        adapters::context(std::move(ctx)), std::move(kind), std::move(args)));
  }
  Async<void> deactivate(Context ctx) override {
    if (service)
      co_await service->deactivate(adapters::context(ctx));
  }
};
} // namespace detail

// Model supplies contract_name/revision, dependencies and create(Value).
template <class Model>
int run(bool http = true, const Value &config = Value::object()) {
  detail::ServiceBridge<Model> service(config);
  return Host(service, http).run();
}
} // namespace cordis::process
