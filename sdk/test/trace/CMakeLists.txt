# Copyright The OpenTelemetry Authors
# SPDX-License-Identifier: Apache-2.0

foreach(
  testname
  tracer_provider_set_test
  tracer_provider_test
  span_data_test
  simple_processor_test
  tracer_test
  always_off_sampler_test
  always_on_sampler_test
  parent_sampler_test
  trace_id_ratio_sampler_test
  composable_sampler_test
  batch_span_processor_test
  tracer_config_test)
  add_executable(${testname} "${testname}.cc")
  target_link_libraries(
    ${testname}
    ${GTEST_BOTH_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    opentelemetry_common
    opentelemetry_trace
    opentelemetry_resources
    opentelemetry_exporter_in_memory)
  gtest_add_tests(
    TARGET ${testname}
    TEST_PREFIX trace.
    TEST_LIST ${testname})
endforeach()

if(OPENTELEMETRY_WITH_RINGOS)
  add_executable(batch_span_processor_ring_os_test
                 batch_span_processor_ring_os_test.cc)
  target_link_libraries(
    batch_span_processor_ring_os_test
    ${GTEST_BOTH_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
    opentelemetry_common
    opentelemetry_trace
    opentelemetry_resources)
  gtest_add_tests(
    TARGET batch_span_processor_ring_os_test
    TEST_PREFIX trace.
    TEST_LIST batch_span_processor_ring_os_test)

  set(RINGOS_LOADGEN_SOURCE "" CACHE FILEPATH
      "External source file for the RingOS BatchSpanProcessor load generator")
  if(RINGOS_LOADGEN_SOURCE)
    add_executable(otel_ringos_loadgen ${RINGOS_LOADGEN_SOURCE})
    target_compile_features(otel_ringos_loadgen PRIVATE cxx_std_17)
    target_include_directories(otel_ringos_loadgen PRIVATE
                               ${OMNISTACK_MPSC_INCLUDE_DIR})
    target_compile_definitions(
      otel_ringos_loadgen PRIVATE
      OSCHANNEL_GIT_COMMIT="${OSCHANNEL_GIT_COMMIT}"
      OPENTELEMETRY_GIT_COMMIT="${OPENTELEMETRY_GIT_COMMIT}")
    target_link_libraries(
      otel_ringos_loadgen
      ${CMAKE_THREAD_LIBS_INIT}
      opentelemetry_common
      opentelemetry_trace
      opentelemetry_resources
      ${OMNISTACK_MPSC_LIBRARY})
  endif()
endif()

if(WITH_BENCHMARK)
  add_executable(sampler_benchmark sampler_benchmark.cc)
  target_link_libraries(
    sampler_benchmark benchmark::benchmark ${CMAKE_THREAD_LIBS_INIT}
    opentelemetry_trace opentelemetry_resources
    opentelemetry_exporter_in_memory)

  add_executable(span_data_benchmark span_data_benchmark.cc)
  target_link_libraries(
    span_data_benchmark
    benchmark::benchmark
    ${CMAKE_THREAD_LIBS_INIT}
    opentelemetry_trace
    opentelemetry_resources
    opentelemetry_exporter_in_memory
    opentelemetry_test_common)
endif()
