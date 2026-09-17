// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#include "src/trace/ring_os_span_queue.h"

#include <algorithm>
#include <array>
#include <limits>
#include <stdexcept>
#include <utility>

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{
namespace
{

std::atomic<uint64_t> g_next_ring_os_queue_id{1};

struct ThreadProducerAssignment
{
  const RingOsSpanQueue *queue;
  uint64_t instance_id;
  size_t producer_slot;
};

thread_local std::vector<ThreadProducerAssignment> g_thread_producer_assignments;

omnistack::mpsc::QueueOptions MakeQueueOptions(size_t requested_capacity,
                                               size_t producer_count, uint32_t batch_size)
{
  if (producer_count == 0 ||
      producer_count > static_cast<size_t>((std::numeric_limits<int>::max)()))
  {
    throw std::invalid_argument("RingOS producer count must be between 1 and INT_MAX");
  }

  omnistack::mpsc::QueueOptions options;
  options.producer_count           = static_cast<uint32_t>(producer_count);
  options.requested_total_capacity = requested_capacity;
  options.batch_size               = batch_size;
  options.enable_statistics        = false;
  return options;
}

}  // namespace

RingOsSpanQueue::RingOsSpanQueue(size_t requested_capacity, size_t producer_count, uint32_t batch_size)
    : queue_(MakeQueueOptions(requested_capacity, producer_count, batch_size)),
      producers_(),
      producer_mutexes_(new std::mutex[producer_count]),
      consumer_([this]() {
        auto consumer = queue_.tryRegisterConsumer();
        if (!consumer)
        {
          throw std::runtime_error("failed to register RingOS consumer");
        }
        return std::move(*consumer);
      }()),
      instance_id_(g_next_ring_os_queue_id.fetch_add(1, std::memory_order_relaxed))
{
  producers_.reserve(producer_count);
  for (size_t slot = 0; slot < producer_count; ++slot)
  {
    auto producer = queue_.tryRegisterProducer(static_cast<uint32_t>(slot));
    if (!producer)
    {
      throw std::runtime_error("failed to register RingOS producer");
    }
    producers_.push_back(std::move(*producer));
  }
  timer_ = std::thread([this] {
    std::unique_lock<std::mutex> timer_lock(timer_mutex_);
    while (!timer_cv_.wait_for(timer_lock, std::chrono::milliseconds(1), [this] { return timer_stop_; }))
    {
      for (size_t i = 0; i < producers_.size(); ++i)
      {
        std::lock_guard<std::mutex> lock(producer_mutexes_[i]);
        producers_[i].flush();
      }
    }
  });
}

RingOsSpanQueue::~RingOsSpanQueue()
{
  { std::lock_guard<std::mutex> lock(timer_mutex_); timer_stop_ = true; }
  timer_cv_.notify_all();
  if (timer_.joinable()) timer_.join();
}

size_t RingOsSpanQueue::TryEnqueueBatch(std::unique_ptr<Recordable> *records, size_t count) noexcept
{
  auto *producer = GetProducerForCurrentThread();
  if (!producer) return 0;
  std::lock_guard<std::mutex> lock(producer_mutexes_[producer->id()]);
  std::array<void *, 256> payloads;
  size_t total = 0;
  while (total < count)
  {
    const size_t requested = (std::min)(payloads.size(), count - total);
    for (size_t i = 0; i < requested; ++i)
      if (!records[total + i]) return total;
    for (size_t i = 0; i < requested; ++i) payloads[i] = records[total + i].release();
    queued_.fetch_add(requested, std::memory_order_relaxed);
    const auto result = producer->tryPutBatch(payloads.data(), requested);
    for (size_t i = result.accepted; i < requested; ++i)
      records[total + i].reset(static_cast<Recordable *>(payloads[i]));
    queued_.fetch_sub(requested - result.accepted, std::memory_order_relaxed);
    accepted_.fetch_add(result.accepted, std::memory_order_relaxed);
    total += result.accepted;
    if (result.accepted < requested) break;
  }
  dropped_full_.fetch_add(count - total, std::memory_order_relaxed);
  return total;
}

omnistack::mpsc::Producer *RingOsSpanQueue::GetProducerForCurrentThread() noexcept
{
  for (const auto &assignment : g_thread_producer_assignments)
  {
    if (assignment.queue == this && assignment.instance_id == instance_id_)
    {
      return &producers_[assignment.producer_slot];
    }
  }

  const size_t slot = next_producer_slot_.fetch_add(1, std::memory_order_relaxed);
  if (slot >= producers_.size())
  {
    producer_registration_failures_.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
  }

  try
  {
    g_thread_producer_assignments.push_back(ThreadProducerAssignment{this, instance_id_, slot});
  }
  catch (...)
  {
    producer_registration_failures_.fetch_add(1, std::memory_order_relaxed);
    return nullptr;
  }
  return &producers_[slot];
}

bool RingOsSpanQueue::TryEnqueue(std::unique_ptr<Recordable> &recordable) noexcept
{
  if (!recordable)
  {
    return false;
  }

  auto *producer = GetProducerForCurrentThread();
  if (producer == nullptr)
  {
    return false;
  }

  // Reserve the accounting slot before publishing so the consumer cannot underflow queued_.
  std::lock_guard<std::mutex> lock(producer_mutexes_[producer->id()]);
  queued_.fetch_add(1, std::memory_order_relaxed);
  Recordable *raw_recordable = recordable.release();
  if (producer->tryPut(raw_recordable) != omnistack::mpsc::PutStatus::accepted)
  {
    recordable.reset(raw_recordable);
    queued_.fetch_sub(1, std::memory_order_relaxed);
    dropped_full_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  accepted_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

size_t RingOsSpanQueue::Size() const noexcept
{
  return queued_.load(std::memory_order_acquire);
}

bool RingOsSpanQueue::Empty() const noexcept
{
  return Size() == 0;
}

size_t RingOsSpanQueue::TryDequeueBatch(size_t max_count,
                                        ConsumeBatchCallback callback) noexcept
{
  constexpr size_t kScratchSize = 256;
  std::array<void *, kScratchSize> payloads;
  size_t total = 0;

  while (total < max_count)
  {
    const size_t requested = (std::min)(max_count - total, payloads.size());
    const size_t received  = consumer_.tryGetBatch(payloads.data(), requested);
    if (received == 0)
    {
      break;
    }

    queued_.fetch_sub(received, std::memory_order_release);
    exported_.fetch_add(received, std::memory_order_relaxed);
    total += received;
    for (size_t i = 0; i < received; ++i)
    {
      callback(std::unique_ptr<Recordable>(static_cast<Recordable *>(payloads[i])));
    }

    if (received < requested)
    {
      break;
    }
  }
  return total;
}

size_t RingOsSpanQueue::RequestedCapacity() const noexcept
{
  return queue_.requestedCapacity();
}

size_t RingOsSpanQueue::EffectiveCapacity() const noexcept
{
  return queue_.effectiveCapacity();
}

RingOsSpanQueueStats RingOsSpanQueue::GetStats() const noexcept
{
  return RingOsSpanQueueStats{queued_.load(std::memory_order_relaxed),
                              accepted_.load(std::memory_order_relaxed),
                              dropped_full_.load(std::memory_order_relaxed),
                              exported_.load(std::memory_order_relaxed),
                              producer_registration_failures_.load(std::memory_order_relaxed)};
}

std::unique_ptr<BatchSpanProcessorQueue> CreateRingOsSpanQueue(size_t requested_capacity,
                                                               size_t producer_count, uint32_t batch_size)
{
  return std::unique_ptr<BatchSpanProcessorQueue>(
      new RingOsSpanQueue(requested_capacity, producer_count, batch_size));
}

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
