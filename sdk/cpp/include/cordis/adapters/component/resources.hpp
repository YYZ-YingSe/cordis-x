#pragma once
#include "context.hpp"

namespace cordis::component::detail {
// Store-local object storage only. Native Runtime owns the recovery obligation.
class Resources {
  struct Entry {
    std::string owner;
    std::shared_ptr<Resource> value;
    std::size_t calls = 0;
    bool closing = false, finished = false;
    std::exception_ptr failure;
    Event drained, completed;
    Async<Message> invoke(Context ctx, std::string method, Message args) {
      if (closing || !value)
        throw Refusal("closed", "resource pending or closing");
      ++calls;
      struct Call {
        Entry &entry;
        ~Call() {
          if (--entry.calls == 0 && entry.closing)
            entry.drained.trigger();
        }
      } call{*this};
      co_return co_await value->invoke(std::move(ctx), std::move(method),
                                       std::move(args));
    }
    Async<void> close(Context ctx) {
      if (!closing) {
        closing = true;
        if (calls)
          co_await drained;
        try {
          if (!value)
            throw Unknown("resource acquisition unresolved");
          co_await value->close(std::move(ctx));
          value.reset();
        } catch (...) {
          failure = std::current_exception();
        }
        finished = true;
        completed.trigger();
      } else {
        co_await completed;
      }
      if (failure)
        std::rethrow_exception(failure);
    }
  };
  std::map<uint64_t, std::shared_ptr<Entry>> entries;
  uint64_t next = 0;
  std::shared_ptr<Entry> get(const Value &args) const {
    if (!args.contains("resource") || !args["resource"].is_number_integer() ||
        args["resource"] <= 0)
      throw Refusal("invalid_argument", "missing resource ID");
    const auto it = entries.find(args["resource"].get<uint64_t>());
    if (it == entries.end() || !args.contains("owner") ||
        args["owner"] != it->second->owner)
      throw Refusal("closed", "resource absent or ownership mismatch");
    return it->second;
  }

public:
  Async<Value> acquire(Service &service, Context ctx, Payload payload) {
    const auto &args = payload.control;
    if (!args.contains("owner") || !args.contains("kind") ||
        !args["kind"].is_string())
      throw Refusal("invalid_argument", "incomplete resource acquisition");
    if (!valid_grant(args["owner"]))
      throw Refusal("invalid_argument", "invalid resource owner");
    const auto owner = args["owner"].get<std::string>();
    if (entries.size() >= 4096 || next >= static_cast<uint64_t>(max_integer))
      throw Refusal("overloaded", "resource history full");
    const auto id = ++next;
    auto entry = std::make_shared<Entry>();
    entry->owner = owner;
    entries.emplace(id, entry); // Reserve before business code can acquire.
    try {
      entry->value = co_await service.acquire(
          std::move(ctx), args["kind"].get<std::string>(), payload.one());
      if (!entry->value)
        throw Unknown("factory returned no resource");
    } catch (const RpcError &) {
      entries.erase(id); // Known rejection promises no residual resource.
      throw;
    }
    // All uncertain outcomes retain their reserved record.
    co_return id;
  }
  Async<Message> invoke(Context ctx, Payload payload) const {
    const auto &args = payload.control;
    auto entry = get(args);
    if (!args.contains("method") || !args["method"].is_string())
      throw Refusal("invalid_argument", "incomplete resource invocation");
    co_return co_await entry->invoke(
        std::move(ctx), args["method"].get<std::string>(), payload.one());
  }
  Async<void> release(Context ctx, Value args) const {
    auto entry = get(args);
    co_await entry->close(std::move(ctx));
  }
  bool recovered() const {
    return std::all_of(entries.begin(), entries.end(), [](const auto &item) {
      return item.second->finished && !item.second->failure;
    });
  }
};
} // namespace cordis::component::detail
