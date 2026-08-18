// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <utility>

#include "opentelemetry/nostd/function_ref.h"
#include "opentelemetry/sdk/common/atomic_unique_ptr.h"
#include "opentelemetry/sdk/common/circular_buffer.h"
#include "opentelemetry/sdk/common/circular_buffer_range.h"
#include "opentelemetry/sdk/trace/recordable.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace sdk
{
namespace trace
{

/**
 * A callback invoked for each recordable removed from a BatchSpanProcessorQueue.
 *
 * Ownership of the recordable is transferred to the callback.
 */
using ConsumeBatchCallback =
    nostd::function_ref<void(std::unique_ptr<Recordable> &&recordable)>;

/**
 * An internal queue interface used by BatchSpanProcessor to store ended spans before export.
 *
 * Implementations must support multiple concurrent producers and a single consumer. All methods
 * are non-blocking except for work performed by the supplied consume callback.
 */
class BatchSpanProcessorQueue
{
public:
  virtual ~BatchSpanProcessorQueue() = default;

  /**
   * Attempts to enqueue an ended span without blocking.
   *
   * On success, the queue takes ownership and sets recordable to null. On failure, recordable
   * remains owned by the caller.
   *
   * @param recordable The ended span to enqueue.
   * @return true if the recordable was enqueued, false otherwise.
   */
  virtual bool TryEnqueue(std::unique_ptr<Recordable> &recordable) noexcept = 0;

  /**
   * Returns a snapshot of the number of recordables currently queued.
   *
   * @return The current queue size.
   */
  virtual size_t Size() const noexcept  = 0;

  /**
   * Checks whether the queue contains no recordables.
   *
   * @return true if the queue is empty, false otherwise.
   */
  virtual bool Empty() const noexcept   = 0;

  /**
   * Removes up to max_count recordables and transfers each one to the supplied callback.
   *
   * This method must only be called from the consumer thread.
   *
   * @param max_count The maximum number of recordables to remove.
   * @param callback The callback that receives ownership of each removed recordable.
   * @return The number of recordables removed from the queue.
   */
  virtual size_t TryDequeueBatch(size_t max_count,
                                 ConsumeBatchCallback callback) noexcept = 0;
};

/**
 * The default BatchSpanProcessorQueue implementation backed by CircularBuffer.
 */
class CircularBufferSpanQueue final : public BatchSpanProcessorQueue
{
public:
  /**
   * Creates a CircularBuffer-backed queue with the specified maximum size.
   *
   * @param max_size The maximum number of recordables that can be queued.
   */
  explicit CircularBufferSpanQueue(size_t max_size) : buffer_(max_size) {}

  bool TryEnqueue(std::unique_ptr<Recordable> &recordable) noexcept override
  {
    return buffer_.Add(recordable);
  }

  size_t Size() const noexcept override { return buffer_.size(); }

  bool Empty() const noexcept override { return buffer_.empty(); }

  size_t TryDequeueBatch(size_t max_count,
                         ConsumeBatchCallback callback) noexcept override
  {
    const size_t count = (std::min)(max_count, buffer_.size());
    if (count == 0)
    {
      return 0;
    }

    buffer_.Consume(count, [&](RecordableRange range) noexcept {
      range.ForEach([&](common::AtomicUniquePtr<Recordable> &ptr) noexcept {
        std::unique_ptr<Recordable> recordable;
        ptr.Swap(recordable);
        callback(std::move(recordable));
        return true;
      });
    });
    return count;
  }

private:
  using RecordableRange = common::CircularBufferRange<common::AtomicUniquePtr<Recordable>>;

  common::CircularBuffer<Recordable> buffer_;
};

}  // namespace trace
}  // namespace sdk
OPENTELEMETRY_END_NAMESPACE
