#pragma once
// Included by typed.hpp after codec and failure-boundary helpers.
#include <optional>
#include <vector>

namespace cordis::typed {
template <class T> struct Flow {
  std::optional<T> value;
};
template <class T> Flow<T> Continue() { return {}; }
template <class T> Flow<T> Break(T value) { return {std::move(value)}; }

template <class R>
Flow<R> decode_flow(const cordis::Flow &flow, const Value &shape, Validation policy) {
  if (!flow.value)
    return {};
  return Break(result<R>(*flow.value, shape, policy));
}
template <class R> class TypedDispatch {
  cordis::Dispatch dispatch_;
  Value shape_, errors_;
  Validation validation_;
  static Async<std::vector<Flow<R>>> join_impl(TypedDispatch self) {
    auto values =
        co_await checked(self.errors_, [&] { return self.dispatch_.join(); });
    std::vector<Flow<R>> result;
    for (const auto &value : values)
      result.push_back(decode_flow<R>(value, self.shape_, self.validation_));
    co_return result;
  }

public:
  TypedDispatch(cordis::Dispatch dispatch, Value shape, Value errors, Validation validation)
      : dispatch_(std::move(dispatch)), shape_(std::move(shape)),
        errors_(std::move(errors)), validation_(validation) {}
  Async<std::vector<Flow<R>>> join() const { return join_impl(*this); }
  Async<Value> snapshot() const { return dispatch_.snapshot(); }
};
template <class A, class R> class Event {
  std::string alias_;
  Value signature_;
  using Handler = std::function<Async<Flow<R>>(Context, A)>;
  static Async<cordis::Flow> callback(Handler handler, Value signature,
                                      Context ctx, Message value) {
    auto args = input<A>(value, signature.at("args"), ctx.validation());
    auto flow = co_await checked(signature.at("errors"),
                                 [&] { return handler(ctx, std::move(args)); });
    if (!flow.value)
      co_return cordis::Continue();
    co_return cordis::Break(
        output(std::move(*flow.value), signature.at("result"), ctx.validation()));
  }
  static Async<Effect> on_impl(Event self, Context ctx, Handler handler) {
    co_return co_await ctx.on(self.alias_, [handler = std::move(handler),
                                            signature = self.signature_](
                                               Context current, Message args) {
      return callback(handler, signature, std::move(current), std::move(args));
    });
  }
  static Async<TypedDispatch<R>> emit_impl(Event self, Context ctx, A args) {
    auto dispatch = co_await ctx.emit(
        self.alias_, argument(std::move(args), self.signature_.at("args"), ctx.validation()));
    co_return TypedDispatch<R>(std::move(dispatch),
                               self.signature_.at("result"),
                               self.signature_.at("errors"), ctx.validation());
  }
  static Async<std::vector<Flow<R>>> parallel_impl(Event self, Context ctx,
                                                   A args) {
    auto dispatch = co_await self.emit(std::move(ctx), std::move(args));
    co_return co_await dispatch.join();
  }
  static Async<Flow<R>> serial_impl(Event self, Context ctx, A args) {
    auto encoded = argument(std::move(args), self.signature_.at("args"), ctx.validation());
    auto flow = co_await checked(self.signature_.at("errors"), [&] {
      return ctx.serial(self.alias_, std::move(encoded));
    });
    co_return decode_flow<R>(flow, self.signature_.at("result"), ctx.validation());
  }

public:
  Event(std::string alias, Value signature)
      : alias_(std::move(alias)), signature_(std::move(signature)) {}
  Async<Effect> on(Context ctx, Handler handler) const {
    return on_impl(*this, std::move(ctx), std::move(handler));
  }
  Async<TypedDispatch<R>> emit(Context ctx, A args) const {
    return emit_impl(*this, std::move(ctx), std::move(args));
  }
  Async<std::vector<Flow<R>>> parallel(Context ctx, A args) const {
    return parallel_impl(*this, std::move(ctx), std::move(args));
  }
  Async<Flow<R>> serial(Context ctx, A args) const {
    return serial_impl(*this, std::move(ctx), std::move(args));
  }
};
template <class A, class R> class Next {
  cordis::Next next_;
  Validation validation_;
  Value signature_;
  static Async<R> run_impl(Next self, A args) {
    auto encoded = argument(std::move(args), self.signature_.at("args"), self.validation_);
    auto value = co_await checked(self.signature_.at("errors"), [&] {
      return self.next_.run(std::move(encoded));
    });
    co_return result<R>(value, self.signature_.at("result"), self.validation_);
  }

public:
  Next(cordis::Next next, Value signature, Validation validation)
      : next_(std::move(next)), validation_(validation), signature_(std::move(signature)) {}
  Async<R> run(A args) const { return run_impl(*this, std::move(args)); }
};
template <class A, class R> class Middleware {
  std::string alias_;
  Value signature_;
  using Handler = std::function<Async<R>(Context, A, Next<A, R>)>;
  using Terminal = std::function<Async<R>(Context, A)>;
  static Async<Message> callback(Handler handler, Value signature, Context ctx,
                                 Message value, cordis::Next next) {
    auto args = input<A>(value, signature.at("args"), ctx.validation());
    auto response = co_await checked(signature.at("errors"), [&] {
      return handler(ctx, std::move(args),
                     Next<A, R>(std::move(next), signature, ctx.validation()));
    });
    co_return output(std::move(response), signature.at("result"), ctx.validation());
  }
  static Async<Effect> register_impl(Middleware self, Context ctx,
                                     Handler handler) {
    co_return co_await ctx.middleware(
        self.alias_,
        [handler = std::move(handler), signature = self.signature_](
            Context current, Message args, cordis::Next next) {
          return callback(handler, signature, std::move(current),
                          std::move(args), std::move(next));
        });
  }
  static Async<Message> terminal_call(Terminal terminal, Value signature,
                                      Context ctx, Message value) {
    auto args = input<A>(value, signature.at("args"), ctx.validation());
    auto response = co_await checked(
        signature.at("errors"), [&] { return terminal(ctx, std::move(args)); });
    co_return output(std::move(response), signature.at("result"), ctx.validation());
  }
  static Async<R> run_impl(Middleware self, Context ctx, A args,
                           Terminal terminal) {
    auto encoded = argument(std::move(args), self.signature_.at("args"), ctx.validation());
    auto value = co_await checked(self.signature_.at("errors"), [&] {
      return ctx.run(self.alias_, std::move(encoded),
                     [terminal, signature = self.signature_](Context current,
                                                             Message args) {
                       return terminal_call(terminal, signature,
                                            std::move(current),
                                            std::move(args));
                     });
    });
    co_return result<R>(value, self.signature_.at("result"), ctx.validation());
  }

public:
  Middleware(std::string alias, Value signature)
      : alias_(std::move(alias)), signature_(std::move(signature)) {}
  Async<Effect> register_handler(Context ctx, Handler handler) const {
    return register_impl(*this, std::move(ctx), std::move(handler));
  }
  Async<R> run(Context ctx, A args, Terminal terminal) const {
    return run_impl(*this, std::move(ctx), std::move(args),
                    std::move(terminal));
  }
};
template <class C> class Factory {
  std::string alias_;
  Value config_, errors_;
  static Async<Child> install_impl(Factory self, Context ctx, C config) {
    auto value = argument(std::move(config), self.config_, ctx.validation());
    co_return co_await checked(self.errors_, [&] {
      return ctx.install(self.alias_, std::move(value));
    });
  }

public:
  Factory(std::string alias, Value config, Value errors)
      : alias_(std::move(alias)), config_(std::move(config)),
        errors_(std::move(errors)) {}
  Async<Child> install(Context ctx, C config) const {
    return install_impl(*this, std::move(ctx), std::move(config));
  }
  template <class F>
  auto build(Message config, F factory, Validation policy = Validation::Checked) const -> std::invoke_result_t<F, C> {
    auto decoded = input<C>(config, config_, policy);
    try {
      return factory(std::move(decoded));
    } catch (const Refusal &) {
      throw;
    } catch (const RpcError &error) {
      if (error.code.empty() ||
          (std::find(errors_.begin(), errors_.end(), error.code) ==
               errors_.end() &&
           std::find(errors_.begin(), errors_.end(), "*") == errors_.end()))
        throw std::runtime_error("undeclared factory business error: " +
                                 error.code);
      throw;
    }
  }
};
template <class T> class Provision {
  std::string alias_;
  Value shape_;
  static Async<void> provide_impl(Provision self, Context ctx, T value) {
    co_await ctx.provide(self.alias_, argument(std::move(value), self.shape_, ctx.validation()));
  }

public:
  Provision(std::string alias, Value shape)
      : alias_(std::move(alias)), shape_(std::move(shape)) {}
  Async<void> provide(Context ctx, T value) const {
    return provide_impl(*this, std::move(ctx), std::move(value));
  }
  T decode(const Message &value, Validation policy = Validation::Checked) const { return result<T>(value, shape_, policy); }
};
} // namespace cordis::typed
