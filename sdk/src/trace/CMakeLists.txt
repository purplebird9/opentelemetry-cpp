# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

add_library(
  opentelemetry_trace
  tracer_context.cc
  tracer_context_factory.cc
  tracer_provider.cc
  tracer_provider_factory.cc
  tracer.cc
  span.cc
  exporter.cc
  provider.cc
  batch_span_processor.cc
  batch_span_processor_factory.cc
  batch_span_processor_options.cc
  simple_processor_factory.cc
  span_data.cc
  samplers/always_on_factory.cc
  samplers/always_off_factory.cc
  samplers/parent.cc
  samplers/parent_factory.cc
  samplers/trace_id_ratio.cc
  samplers/trace_id_ratio_factory.cc
  samplers/ot_trace_state.cc
  samplers/composite_sampler.cc
  samplers/composite_sampler_factory.cc
  samplers/composable_probability.cc
  samplers/composable_parent_threshold.cc
  samplers/composable_rule_based.cc
  multi_recordable.cc
  random_id_generator.cc
  random_id_generator_factory.cc
  tracer_config.cc)

option(OPENTELEMETRY_WITH_RINGOS
       "Build the BatchSpanProcessor RingOS queue backend" OFF)

if(OPENTELEMETRY_WITH_RINGOS)
  find_path(OMNISTACK_MPSC_INCLUDE_DIR omnistack/mpsc/queue.h REQUIRED)
  find_library(OMNISTACK_MPSC_LIBRARY NAMES omnistack_mpsc REQUIRED)
  target_sources(opentelemetry_trace PRIVATE ring_os_span_queue.cc)
  target_include_directories(opentelemetry_trace PRIVATE
                             ${OMNISTACK_MPSC_INCLUDE_DIR})
  target_compile_definitions(opentelemetry_trace PRIVATE
                             OPENTELEMETRY_WITH_RINGOS=1)
  target_link_libraries(opentelemetry_trace PRIVATE
                        ${OMNISTACK_MPSC_LIBRARY})
  if(UNIX)
    find_package(Threads REQUIRED)
    target_link_libraries(opentelemetry_trace PRIVATE Threads::Threads)
    if(NOT APPLE)
      target_link_libraries(opentelemetry_trace PRIVATE rt)
    endif()
  endif()
endif()

set_target_properties(opentelemetry_trace PROPERTIES EXPORT_NAME trace)
set_target_version(opentelemetry_trace)

target_link_libraries(opentelemetry_trace PUBLIC opentelemetry_common
                                                 opentelemetry_resources)

target_include_directories(
  opentelemetry_trace
  PUBLIC "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/sdk/include>")

if(OPENTELEMETRY_INSTALL)
  opentelemetry_add_pkgconfig(
    trace "OpenTelemetry SDK - Trace"
    "Components for exporting traces in the OpenTelemetry SDK."
    "opentelemetry_resources")
endif()
