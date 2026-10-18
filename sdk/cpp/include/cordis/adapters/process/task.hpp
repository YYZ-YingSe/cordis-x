#pragma once
#include "context.hpp"

namespace cordis::process {
using TaskCallback = std::function<Async<void>(Context)>;

class Task {
  std::shared_ptr<DependencyChannel> channel;
  std::string id, grant;
  Async<Json> request(std::string command) const {
    return cordis::detail::invoke_owned(
        [*this](std::string command) -> Async<Json> {
          Json args = {{"task", id}, {"grant", grant}};
          co_return co_await channel->request(command, std::move(args));
        },
        std::move(command));
  }

public:
  Task(std::shared_ptr<DependencyChannel> channel, std::string id,
       std::string grant)
      : channel(std::move(channel)), id(std::move(id)),
        grant(std::move(grant)) {}
  Async<void> join() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<void> { (co_await request("task_join")); });
  }
  Async<Json> snapshot() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Json> {
      co_return (co_await request("task_status"));
    });
  }
};

class TaskRegistry {
  struct Entry {
    TaskCallback callback;
    bool started = false, running = false, cancelled = false;
    std::shared_ptr<corral::Event> cancellation =
        std::make_shared<corral::Event>();
  };
  std::mutex mutex;
  std::uint64_t next = 0;
  std::map<std::string, std::shared_ptr<Entry>> entries;

public:
  Async<Task> add(std::shared_ptr<DependencyChannel> channel,
                  std::string context, std::string label,
                  TaskCallback callback) {
    if (label.size() > 1024)
      throw Refusal("invalid_argument", "task label too long");
    auto entry = std::make_shared<Entry>();
    entry->callback = std::move(callback);
    std::string id;
    {
      std::lock_guard lock(mutex);
      if (next >= 4096)
        throw Refusal("tasks_exhausted", "task history full");
      id = "t" + std::to_string(++next);
      // Store before registration: execution/cancellation can precede its
      // reply.
      entries.emplace(id, entry);
    }
    Json args = {{"context", context}, {"task", id}, {"label", label}};
    const auto reply =
        co_await channel->request("task_register", std::move(args));
    const auto grant = reply.at("grant").get<std::string>();
    if (reply.at("task") != id || grant.size() != 32 ||
        !std::all_of(grant.begin(), grant.end(), [](char c) {
          return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }))
      throw Refusal("protocol", "invalid task registration");
    co_return Task(channel, id, grant);
  }
  std::shared_ptr<corral::Event> cancellation(std::string id) {
    std::lock_guard lock(mutex);
    const auto found = entries.find(id);
    if (found == entries.end())
      throw Refusal("closed", "task unavailable");
    return found->second->cancellation;
  }
  void cancel(std::string id) {
    std::shared_ptr<corral::Event> signal;
    {
      std::lock_guard lock(mutex);
      const auto found = entries.find(id);
      if (found == entries.end())
        throw Refusal("closed", "task unavailable");
      auto &entry = *found->second;
      if (!entry.cancelled) {
        entry.cancelled = true;
        signal = entry.cancellation;
      }
    }
    if (signal)
      signal->trigger();
  }
  Async<Json> invoke(std::string id, Context ctx) {
    std::shared_ptr<Entry> entry;
    TaskCallback callback;
    {
      std::lock_guard lock(mutex);
      const auto found = entries.find(id);
      if (found == entries.end() || found->second->started ||
          !found->second->callback)
        throw Refusal("closed", "task callback unavailable");
      entry = found->second;
      entry->started = entry->running = true;
      callback = entry->callback;
    }
    struct Guard {
      TaskRegistry &registry;
      std::shared_ptr<Entry> entry;
      Context ctx;
      ~Guard() {
        std::lock_guard lock(registry.mutex);
        entry->callback = {};
        entry->running = false;
      }
    } guard{*this, entry, ctx};
    try {
      (co_await native::with_cleanup([&] { return callback(ctx); },
                                     [ctx] { return ctx.finish(); }));
      co_return Json{{"kind", "success"}, {"value", nullptr}};
    } catch (const RpcError &error) {
      co_return Json{
          {"kind",
           dynamic_cast<const Refusal *>(&error) ? "refused" : "failure"},
          {"error", {{"code", error.code}, {"message", error.what()}}}};
    } catch (const std::exception &error) {
      co_return Json{
          {"kind", "failure"},
          {"error", {{"code", "task_failed"}, {"message", error.what()}}}};
    } catch (...) {
      co_return Json{
          {"kind", "failure"},
          {"error",
           {{"code", "task_failed"}, {"message", "non-standard exception"}}}};
    }
  }
  void cancel_all() {
    std::vector<std::shared_ptr<corral::Event>> signals;
    {
      std::lock_guard lock(mutex);
      for (auto &[_, entry] : entries) {
        entry->cancelled = true;
        signals.push_back(entry->cancellation);
      }
    }
    for (auto &signal : signals)
      signal->trigger();
  }
  bool idle() {
    std::lock_guard lock(mutex);
    return std::all_of(entries.begin(), entries.end(), [](const auto &entry) {
      return !entry.second->running;
    });
  }
};
inline Async<Task> Context::spawn(std::string label,
                                  TaskCallback callback) const {
  return cordis::detail::invoke_owned(
      [*this](std::string label, TaskCallback callback) -> Async<Task> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty() || frame->cleanup)
            throw Refusal("closed", "context cannot spawn tasks");
          if (!frame->tasks)
            throw Refusal("unsupported_tasks",
                          "task capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        co_return (co_await frame->tasks->add(frame->channel, frame->token,
                                              label, std::move(callback)));
      },
      std::move(label), std::move(callback));
}
} // namespace cordis::process
