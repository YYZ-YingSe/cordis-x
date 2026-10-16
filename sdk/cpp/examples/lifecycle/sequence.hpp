#pragma once
#include "managed.hpp"
namespace example::lifecycle {
class Sequences final : public ::lifecycle::SequencesServer {
  using Dependencies = ::lifecycle::SequencesDependencies;
  struct Run {
    std::mutex mutex;
    std::vector<std::shared_ptr<std::vector<char>>> buffers{3};
    std::optional<EffectSequence> sequence;
  };
  std::mutex mutex_;
  std::map<std::string, std::shared_ptr<Run>> runs_;
  std::shared_ptr<Run> find(const std::string &name) {
    std::lock_guard lock(mutex_);
    auto found = runs_.find(name);
    if (found == runs_.end() || !found->second->sequence)
      throw RpcError("unknown_sequence", name);
    return found->second;
  }

public:
  static std::shared_ptr<Sequences> create(cordis::data::Unit) {
    return std::make_shared<Sequences>();
  }
  Async<Unit> start(Context ctx, Text args) override {
    const auto name = args.value();
    auto state = std::make_shared<Run>();
    {
      std::lock_guard lock(mutex_);
      if (!runs_.emplace(name, state).second)
        throw RpcError("duplicate_sequence", name);
    }
    try {
      auto sequence = co_await ctx.effect(
          "pipeline:" + name,
          [state, name, n = 0](Context step) mutable -> Async<EffectStep> {
            const auto current = ++n;
            co_await log<Dependencies>(
                step, name + "/step:" + std::to_string(current));
            {
              std::lock_guard lock(state->mutex);
              state->buffers.at(current - 1) =
                  std::make_shared<std::vector<char>>(1024);
            }
            Inverse inverse{
                "buffer:" + std::to_string(current),
                [state, name, current](Context cleanup) -> Async<void> {
                  co_await log<Dependencies>(
                      std::move(cleanup),
                      name + "/undo:" + std::to_string(current));
                  std::lock_guard lock(state->mutex);
                  state->buffers.at(current - 1).reset();
                }};
            co_return current == 3 ? EffectStep::complete(std::move(inverse))
                                   : EffectStep::yielded(std::move(inverse));
          });
      std::lock_guard lock(mutex_);
      state->sequence = std::move(sequence);
    } catch (...) {
      std::lock_guard lock(mutex_);
      runs_.erase(name);
      throw;
    }
    co_return Unit{};
  }
  Async<::lifecycle::Completion> ready(Context, Text args) override {
    co_return completion(co_await find(args.value())->sequence->ready());
  }
  Async<Unit> release(Context, Text args) override {
    co_await find(args.value())->sequence->dispose();
    co_return Unit{};
  }
  Async<::lifecycle::SequenceStatus> status(Context, Text args) override {
    co_return sequence_status(
        co_await find(args.value())->sequence->snapshot());
  }
  Async<google::protobuf::UInt64Value> count(Context, Text args) override {
    auto state = find(args.value());
    std::lock_guard lock(state->mutex);
    co_return example::lifecycle::count(
        std::count_if(state->buffers.begin(), state->buffers.end(),
                      [](const auto &b) { return bool(b); }));
  }
  Async<void> deactivate(Context ctx) override {
    co_await log<Dependencies>(std::move(ctx), "pipeline.closed");
  }
};
using SequencesModel = ::lifecycle::SequencesModel<Sequences>;
} // namespace example::lifecycle
