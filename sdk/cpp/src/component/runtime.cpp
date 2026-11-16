#include <cassert>
#include <cordis/adapters/component/runtime.hpp>

// Canonical unit streams, also used by wit-bindgen's Rust inter-task waker.
// These are Component Model intrinsics and introduce no host WIT operation.
extern "C" {
__attribute__((import_module("$root"),
               import_name("[stream-new-unit]"))) uint64_t
cordis_stream_new();
__attribute__((import_module("$root"),
               import_name("[async-lower][stream-read-unit]"))) uint32_t
cordis_stream_read(uint32_t, void *, uint32_t);
__attribute__((import_module("$root"),
               import_name("[async-lower][stream-write-unit]"))) uint32_t
cordis_stream_write(uint32_t, const void *, uint32_t);
__attribute__((import_module("$root"),
               import_name("[stream-drop-readable-unit]"))) void
cordis_stream_drop_reader(uint32_t);
__attribute__((import_module("$root"),
               import_name("[stream-drop-writable-unit]"))) void
cordis_stream_drop_writer(uint32_t);
}
namespace cordis::component::detail {
Frame *current() {
  auto holder =
      static_cast<std::shared_ptr<Frame> *>(managed_plugin_context_get_0());
  return holder ? holder->get() : nullptr;
}
Frame::~Frame() {
  assert(waits.empty() && tasks.empty());
  managed_plugin_waitable_set_drop(waitset);
}
void Frame::attach(uint32_t handle, Waiter &waiter) {
  if (!waits.emplace(handle, &waiter).second)
    std::terminate();
  managed_plugin_waitable_join(handle, waitset);
}
void Frame::complete(managed_plugin_event_t event) {
  // Host cancellation is a request, not proof that imports or resource
  // acquisitions stopped. Drain outstanding operations before returning.
  if (event.event == MANAGED_PLUGIN_EVENT_CANCEL)
    return;
  auto it = waits.find(event.waitable);
  if (it == waits.end())
    std::terminate();
  if (event.event == MANAGED_PLUGIN_EVENT_SUBTASK &&
      event.code == MANAGED_PLUGIN_SUBTASK_STARTED)
    return;
  auto waiter = it->second;
  auto resume = waiter->continuation;
  waits.erase(it);
  managed_plugin_waitable_join(event.waitable, 0);
  if (!waiter->complete(event))
    std::terminate();
  resume.resume();
}
// Only these host-defined queries may withdraw an import. Business imports
// keep their canonical task and callback storage until actual completion.
static bool observation_command(const std::string &command) {
  return command == "wait" || command == "info" || command == "metadata" ||
    command == "join" || command == "task-status" || command == "effect-ready" ||
    command == "effect-status" || command == "scope-ready" || command == "scope-status" ||
    command == "plugin-ready" || command == "plugin-settled" || command == "plugin-status" ||
    command == "event-join" || command == "event-status";
}
struct Import final : Waiter {
  cordis_plugin_host_call_args_t args;
  cordis_plugin_host_result_payload_failure_t result{};
  Frame *frame = nullptr;
  uint32_t handle = 0;
  bool observation = false, returned = false;
  ~Import() {
    // A cancellation can race with a normal return. The awaiter owns any
    // returned allocation even when Corral suppresses await_resume().
    if (returned) cordis_plugin_host_result_payload_failure_free(&result);
  }
  bool await_ready() const noexcept { return false; }
  bool terminal(uint32_t state) {
    if (state == MANAGED_PLUGIN_SUBTASK_RETURNED) returned = true;
    else if (state != MANAGED_PLUGIN_SUBTASK_STARTED_CANCELLED &&
             state != MANAGED_PLUGIN_SUBTASK_RETURNED_CANCELLED) return false;
    if (handle) managed_plugin_subtask_drop(handle);
    handle = 0;
    return true;
  }
  bool await_suspend(std::coroutine_handle<> h) {
    continuation = h;
    frame = current();
    if (!frame) throw Refusal("closed", "operation requires a live component call");
    auto status = cordis_plugin_host_call(&args, &result);
    handle = MANAGED_PLUGIN_SUBTASK_HANDLE(status);
    if (terminal(MANAGED_PLUGIN_SUBTASK_STATE(status))) return false;
    frame->attach(handle, *this);
    return true;
  }
  bool await_early_cancel() const noexcept { return observation; }
  bool await_cancel(std::coroutine_handle<>) noexcept {
    if (!observation) return false;
    // Canonical cancellation polls the subtask synchronously. Remove it from
    // the wait set first; otherwise the Component Model rejects concurrent
    // synchronous and asynchronous observation of the same waitable.
    managed_plugin_waitable_join(handle, 0);
    auto status = managed_plugin_subtask_cancel(handle);
    auto state = MANAGED_PLUGIN_SUBTASK_STATE(status);
    if (state != MANAGED_PLUGIN_SUBTASK_RETURNED &&
        state != MANAGED_PLUGIN_SUBTASK_STARTED_CANCELLED &&
        state != MANAGED_PLUGIN_SUBTASK_RETURNED_CANCELLED) {
      managed_plugin_waitable_join(handle, frame->waitset);
      return false;
    }
    frame->waits.erase(handle);
    return terminal(state);
  }
  bool await_must_resume() const noexcept { return !observation; }
  void await_resume() {
    if (!returned) throw Refusal("observation_cancelled", "observation was withdrawn");
  }
  bool complete(managed_plugin_event_t event) override {
    return event.event == MANAGED_PLUGIN_EVENT_SUBTASK && terminal(event.code);
  }
};
Async<Value> request(std::shared_ptr<Frame> authority, std::string command,
                     Value args) {
  co_return (co_await request_payload(std::move(authority), std::move(command),
                                      Payload(std::move(args))))
      .control_only();
}
Async<Payload> request_payload(std::shared_ptr<Frame> authority,
                               std::string command, Payload args) {
  if (!authority->open || !current())
    throw Refusal("closed", "Context expired");
  Buffer buffer(args);
  Import operation;
  operation.observation = observation_command(command);
  operation.args = {borrowed(authority->token), borrowed(command),
                    buffer.view()};
  co_await operation;
  auto &result = operation.result;
  if (result.is_err) {
    operation.returned = false; // raise consumes this owned error value.
    raise(result.val.err);
  }
  try {
    co_return decode(result.val.ok, authority->validation);
  } catch (const std::exception &e) {
    throw Unknown(e.what());
  }
}

Async<void> execute(std::shared_ptr<Frame> frame,
                    std::function<Async<Payload>()> run) {
  try {
    frame->result = co_await run();
  } catch (...) {
    frame->error = std::current_exception();
  }
  frame->open = false;
  frame->done = true;
}
managed_plugin_callback_code_t finish(std::shared_ptr<Frame> *holder) {
  auto frame = *holder;
  if (!frame->done)
    return MANAGED_PLUGIN_CALLBACK_CODE_WAIT(frame->waitset);
  assert(frame->waits.empty() && frame->tasks.empty());
  exports_cordis_plugin_managed_service_result_payload_failure_t result{};
  try {
    if (frame->error)
      std::rethrow_exception(frame->error);
    try {
      result.val.ok = Buffer(frame->result).release();
    } catch (const std::exception &e) {
      throw Unknown(e.what());
    }
  } catch (...) {
    result.is_err = true;
    result.val.err = failure(std::current_exception());
  }
  exports_cordis_plugin_managed_service_call_return(result);
  exports_cordis_plugin_managed_service_result_payload_failure_free(&result);
  managed_plugin_context_set_0(nullptr);
  delete holder;
  return MANAGED_PLUGIN_CALLBACK_CODE_EXIT;
}
Event::~Event() { assert(waiters_.empty()); }
void Event::Pending::await_suspend(std::coroutine_handle<> h) {
  continuation = h;
  auto endpoints = cordis_stream_new();
  reader = uint32_t(endpoints);
  writer = uint32_t(endpoints >> 32);
  auto status = cordis_stream_read(reader, nullptr, 1);
  assert(status == MANAGED_PLUGIN_WAITABLE_STATUS_BLOCKED);
  current()->attach(reader, *this);
  event.waiters_.push_back(this);
}
bool Event::Pending::complete(managed_plugin_event_t event) {
  if (event.event != MANAGED_PLUGIN_EVENT_STREAM_READ ||
      MANAGED_PLUGIN_WAITABLE_STATE(event.code) !=
          MANAGED_PLUGIN_WAITABLE_COMPLETED ||
      MANAGED_PLUGIN_WAITABLE_COUNT(event.code) != 1)
    return false;
  cordis_stream_drop_reader(reader);
  cordis_stream_drop_writer(writer);
  return true;
}
void Event::trigger() {
  triggered_ = true;
  auto pending = std::move(waiters_);
  for (auto waiter : pending) {
    auto status = cordis_stream_write(waiter->writer, nullptr, 1);
    assert(MANAGED_PLUGIN_WAITABLE_STATE(status) ==
           MANAGED_PLUGIN_WAITABLE_COMPLETED);
    assert(MANAGED_PLUGIN_WAITABLE_COUNT(status) == 1);
  }
}
} // namespace cordis::component::detail
