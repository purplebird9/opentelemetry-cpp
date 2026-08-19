// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "omnistack/mpsc/queue.h"
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
  RingOsSpanQueue(size_t requested_capacity, size_t producer_count);

  bool TryEnqueue(std::unique_ptr<Recordable> &recordable) noexcept override;
  size_t Size() const noexcept override;
  bool Empty() const noexcept override;
  size_t TryDequeueBatch(size_t max_count, ConsumeBatchCallback callback) noexcept override;

  size_t RequestedCapacity() const noexcept;
  size_t EffectiveCapacity() const noexcept;
  RingOsSpanQueueStats GetStats() const noexcept;

private:
  omnistack::mpsc::Producer *GetProducerForCurrentThread() noexcept;

  // Handles are destroyed before queue_, which owns their shared state.
  omnistack::mpsc::Queue queue_;
  std::vector<omnistack::mpsc::Producer> producers_;
  omnistack::mpsc::Consumer consumer_;
  const uint64_t instance_id_;
  std::atomic<size_t> next_producer_slot_{0};
  std::atomic<size_t> queued_{0};
  std::atomic<uint64_t> accepted_{0};
  std::atomic<uint64_t> dropped_full_{0};
  std::atomic<uint64_t> exported_{0};
  std::atomic<uint64_t> producer_registration_failures_{0};
};

std::unique_ptr<BatchSpanProcessorQueue> CreateRingOsSpanQueue(size_t requested_capacity,
                                                               size_t producer_count);

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
