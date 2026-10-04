// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>

#include "omnistack/mpsc/queue.h"
#include "opentelemetry/sdk/common/thread_instrumentation.h"
#include "src/trace/batch_span_processor_queue.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

struct RingOsSpanQueueStats
{
  size_t queued;
  uint64_t accepted;
  uint64_t dropped_full;
  uint64_t exported;
  uint64_t producer_registration_failures;
};

/** RingOS-backed MPSC queue used by BatchSpanProcessor. */
class RingOsSpanQueue final : public BatchSpanProcessorQueue
{
public:
  RingOsSpanQueue(
      size_t requested_capacity,
      size_t producer_count,
      uint32_t batch_size = 64,
      std::shared_ptr<sdk::common::ThreadInstrumentation> reader_instrumentation = nullptr);
  ~RingOsSpanQueue() override;
  size_t TryEnqueueBatch(std::unique_ptr<Recordable> *records, size_t count) noexcept override;

  bool TryEnqueue(std::unique_ptr<Recordable> &recordable) noexcept override;
  size_t Size() const noexcept override;
  bool Empty() const noexcept override;
  size_t TryDequeueBatch(size_t max_count, ConsumeBatchCallback callback) noexcept override;

  size_t RequestedCapacity() const noexcept;
  size_t EffectiveCapacity() const noexcept;
  RingOsSpanQueueStats GetStats() const noexcept;

private:
  omnistack::mpsc::Producer *GetProducerForCurrentThread() noexcept;
  void ReaderLoop() noexcept;

  // Handles are destroyed before queue_, which owns their shared state.
  omnistack::mpsc::Queue queue_;
  std::vector<omnistack::mpsc::Producer> producers_;
  std::unique_ptr<std::mutex[]> producer_mutexes_;
  std::mutex timer_mutex_;
  std::condition_variable timer_cv_;
  bool timer_stop_ = false;
  std::thread timer_;
  omnistack::mpsc::Consumer consumer_;
  const size_t handoff_capacity_;
  std::unique_ptr<Recordable *[]> handoff_;
  std::atomic<size_t> handoff_head_{0};
  std::atomic<size_t> handoff_tail_{0};
  std::shared_ptr<sdk::common::ThreadInstrumentation> reader_instrumentation_;
  std::atomic<bool> reader_stop_{false};
  std::thread reader_thread_;
  const uint64_t instance_id_;
  std::atomic<size_t> next_producer_slot_{0};
  std::atomic<size_t> queued_{0};
  std::atomic<uint64_t> accepted_{0};
  std::atomic<uint64_t> dropped_full_{0};
  std::atomic<uint64_t> exported_{0};
  std::atomic<uint64_t> producer_registration_failures_{0};
};

std::unique_ptr<BatchSpanProcessorQueue> CreateRingOsSpanQueue(size_t requested_capacity,
                                                               size_t producer_count,
                                                               uint32_t batch_size = 64,
                                                               std::shared_ptr<sdk::common::ThreadInstrumentation>
                                                                   reader_instrumentation = nullptr);

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
