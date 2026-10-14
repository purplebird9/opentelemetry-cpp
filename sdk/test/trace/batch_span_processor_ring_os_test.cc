/* Counting Exporter and 2 smoke cases */
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

#include "opentelemetry/nostd/span.h"
#include "opentelemetry/sdk/common/exporter_utils.h"
#include "opentelemetry/sdk/trace/batch_span_processor.h"
#include "opentelemetry/sdk/trace/batch_span_processor_options.h"
#include "opentelemetry/sdk/trace/exporter.h"
#include "opentelemetry/sdk/trace/recordable.h"
#include "opentelemetry/sdk/trace/span_data.h"
#include "opentelemetry/version.h"

OPENTELEMETRY_BEGIN_NAMESPACE
namespace
{

struct CountingExporterState
{
  std::atomic<size_t> exported{0};
  std::atomic<size_t> force_flushes{0};
  std::atomic<bool> is_shutdown{false};
};

class CountingSpanExporter final : public sdk::trace::SpanExporter
{
public:
  explicit CountingSpanExporter(std::shared_ptr<CountingExporterState> state) noexcept
      : state_(std::move(state))
  {}

  std::unique_ptr<sdk::trace::Recordable> MakeRecordable() noexcept override
  {
    return std::unique_ptr<sdk::trace::Recordable>(new sdk::trace::SpanData);
  }

  sdk::common::ExportResult Export(
      const nostd::span<std::unique_ptr<sdk::trace::Recordable>> &recordables) noexcept override
  {
    state_->exported.fetch_add(recordables.size(), std::memory_order_relaxed);
    return sdk::common::ExportResult::kSuccess;
  }

  bool ForceFlush(std::chrono::microseconds /* timeout */) noexcept override
  {
    state_->force_flushes.fetch_add(1, std::memory_order_relaxed);
    return true;
  }

  bool Shutdown(std::chrono::microseconds /* timeout */) noexcept override
  {
    state_->is_shutdown.store(true, std::memory_order_release);
    return true;
  }

private:
  std::shared_ptr<CountingExporterState> state_;
};

void RunRingOsBatchSpanProcessorSmoke(size_t producer_count,
                                      size_t spans_per_producer = 64,
                                      uint32_t ring_batch_size = 64)
{
  constexpr size_t kMaxQueueSize     = 65536;
  constexpr auto kTimeout            = std::chrono::seconds(10);

  auto exporter_state = std::make_shared<CountingExporterState>();

  sdk::trace::BatchSpanProcessorOptions options{};
  options.max_queue_size         = kMaxQueueSize;
  options.max_export_batch_size  = 512;
  options.schedule_delay_millis  = std::chrono::milliseconds(100);
  options.queue_backend          = sdk::trace::BatchSpanProcessorQueueBackend::kRingOs;
  options.ring_os_producer_count = producer_count;
  options.ring_os_batch_size     = ring_batch_size;

  auto processor = std::make_shared<sdk::trace::BatchSpanProcessor>(
      std::unique_ptr<sdk::trace::SpanExporter>(new CountingSpanExporter(exporter_state)), options);

  std::vector<std::thread> producers;
  producers.reserve(producer_count);
  for (size_t producer = 0; producer < producer_count; ++producer)
  {
    producers.emplace_back([processor, spans_per_producer]() {
      for (size_t span_index = 0; span_index < spans_per_producer; ++span_index)
      {
        processor->OnEnd(processor->MakeRecordable());
      }
    });
  }

  for (auto &producer : producers)
  {
    producer.join();
  }

  ASSERT_TRUE(processor->ForceFlush(kTimeout));
  EXPECT_GE(exporter_state->force_flushes.load(std::memory_order_relaxed), 1);
  ASSERT_TRUE(processor->Shutdown(kTimeout));
  EXPECT_TRUE(exporter_state->is_shutdown.load(std::memory_order_acquire));

  const size_t ended = producer_count * spans_per_producer;
  EXPECT_EQ(ended, exporter_state->exported.load(std::memory_order_relaxed));
}

TEST(BatchSpanProcessorRingOs, SmokeOneProducerForceFlushAndShutdown)
{
  RunRingOsBatchSpanProcessorSmoke(1);
}

TEST(BatchSpanProcessorRingOs, SmokeThirtyTwoProducersForceFlushAndShutdown)
{
  RunRingOsBatchSpanProcessorSmoke(32);
}

TEST(BatchSpanProcessorRingOs, SmokePartialTailWithoutTimer)
{
  RunRingOsBatchSpanProcessorSmoke(1, 1, 64);
  RunRingOsBatchSpanProcessorSmoke(1, 65, 64);
}

}  // namespace
OPENTELEMETRY_END_NAMESPACE
