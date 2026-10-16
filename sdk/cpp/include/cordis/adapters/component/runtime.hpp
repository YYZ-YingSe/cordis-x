#pragma once
#include "value.hpp"
#include <list>
#include <map>

namespace cordis::component::detail {
struct Waiter {
  std::coroutine_handle<> continuation;
  virtual ~Waiter() = default;
  virtual bool complete(managed_plugin_event_t event) = 0;
};
struct Frame : std::enable_shared_from_this<Frame> {
  std::string token;
  Validation validation = Validation::Checked;
  bool open = true, cleanup = false, done = false;
  managed_plugin_waitable_set_t waitset = managed_plugin_waitable_set_new();
  corral::UnsafeNursery tasks{corral::EventLoopID(this)};
  std::map<uint32_t, Waiter *> waits;
  Payload result;
  std::exception_ptr error;
  ~Frame();
  void attach(uint32_t handle, Waiter &waiter);
  void complete(managed_plugin_event_t event);
};
Frame *current();
Async<Payload> request_payload(std::shared_ptr<Frame> authority,
                               std::string command, Payload args);
Async<Value> request(std::shared_ptr<Frame> authority, std::string command,
                     Value args);
Async<void> execute(std::shared_ptr<Frame> frame,
                    std::function<Async<Payload>()> run);
managed_plugin_callback_code_t finish(std::shared_ptr<Frame> *holder);

// Cross-export completion uses a canonical unit stream to wake the receiving
// component task. Resuming another export directly would run with the wrong
// canonical task/context and could return a result to the wrong caller.
class Event {
  struct Pending final : Waiter {
    Event &event;
    uint32_t reader = 0, writer = 0;
    explicit Pending(Event &event) : event(event) {}
    bool await_ready() const noexcept { return event.triggered_; }
    void await_suspend(std::coroutine_handle<> continuation);
    void await_resume() {}
    bool complete(managed_plugin_event_t event) override;
  };
  bool triggered_ = false;
  std::list<Pending *> waiters_;

public:
  ~Event();
  void trigger();
  auto operator co_await() { return Pending(*this); }
};
} // namespace cordis::component::detail
