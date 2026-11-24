cmake_minimum_required(VERSION 3.16)
project(lock LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
# CMake 3.16 can select C++20, but the compiler and standard library
# must implement the features actually used in the project.
include(CheckCXXSourceCompiles)
check_cxx_source_compiles("
#include <atomic>
#include <map>
#include <string>
int main() {
  int value = 0;
  std::atomic_ref<int> a(value);
  std::map<int, int> m;
  std::string s = \"test\";
  return a.load() + m.contains(1) + s.starts_with(\"t\");
}
" LOCK_HAS_REQUIRED_CXX20)
if(NOT LOCK_HAS_REQUIRED_CXX20)
  message(FATAL_ERROR "A C++20-capable C++ compiler and standard library are required (GCC 10+ recommended). On Ubuntu 20.04, install g++-10 and configure with CXX=g++-10 after removing the old build directory.")
endif()
add_library(lock_core STATIC src/config.cc src/locks/mu_state.cc src/locks/handoff_state.cc src/bench/workload.cc)
target_include_directories(lock_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_compile_options(lock_core PRIVATE -Wall -Wextra -Wpedantic)
option(LOCK_BUILD_TESTS "Build tests" ON)
option(LOCK_WITH_RDMA "Build the verbs-based RDMA executables" ON)
if(LOCK_WITH_RDMA)
  find_path(VERBS_INCLUDE_DIR infiniband/verbs.h)
  find_library(VERBS_LIBRARY ibverbs)
  if(VERBS_INCLUDE_DIR AND VERBS_LIBRARY)
    find_package(Threads REQUIRED)
    add_library(lock_rdma STATIC src/rdma/transport.cc src/locks/replicated_log.cc
             src/locks/mu_compact_log.cc)
    target_include_directories(lock_rdma PUBLIC include)
    target_link_libraries(lock_rdma PUBLIC lock_core ${VERBS_LIBRARY})
    target_compile_options(lock_rdma PRIVATE -Wall -Wextra -Wpedantic)
    add_library(lock_bench STATIC src/bench/client.cc)
    target_link_libraries(lock_bench PUBLIC lock_rdma)
    add_executable(lock_client src/client_main.cc)
    target_link_libraries(lock_client PRIVATE lock_bench)
    add_executable(lock_server src/server_main.cc src/locks/mu_leader_runtime.cc
                   src/locks/handoff_leader_runtime.cc)
    target_link_libraries(lock_server PRIVATE lock_rdma Threads::Threads)
    add_executable(lock_rdma_smoke src/rdma/smoke_main.cc)
    target_link_libraries(lock_rdma_smoke PRIVATE lock_rdma)
  else()
    message(STATUS "libibverbs unavailable: building core-only")
  endif()
endif()
if(LOCK_BUILD_TESTS)
  enable_testing()
  add_executable(lock_core_test tests/unit/core_test.cc)
  target_link_libraries(lock_core_test PRIVATE lock_core)
  add_test(NAME core COMMAND lock_core_test)
  add_executable(lock_mu_test tests/unit/mu_test.cc)
  target_link_libraries(lock_mu_test PRIVATE lock_core)
  add_test(NAME mu_queue COMMAND lock_mu_test)
  find_package(Threads REQUIRED)
  add_executable(lock_mu_encoding_test tests/unit/mu_encoding_test.cc)
  target_link_libraries(lock_mu_encoding_test PRIVATE lock_core Threads::Threads)
  add_test(NAME mu_encoding_queue COMMAND lock_mu_encoding_test)
  add_executable(lock_handoff_test tests/unit/handoff_test.cc)
  target_link_libraries(lock_handoff_test PRIVATE lock_core)
  add_test(NAME handoff_queue COMMAND lock_handoff_test)
  add_executable(lock_handoff_trace_test tests/unit/handoff_trace_test.cc)
  target_link_libraries(lock_handoff_trace_test PRIVATE lock_core)
  add_test(NAME handoff_trace COMMAND lock_handoff_trace_test)
  # Compile the actual client engine with a deterministic fake transport.
  add_executable(lock_peer_read_client_test tests/unit/peer_read_client_test.cc
                 src/bench/client.cc)
  target_link_libraries(lock_peer_read_client_test PRIVATE lock_core)
  add_test(NAME peer_read_client COMMAND lock_peer_read_client_test)
  add_executable(lock_lane_layout_test tests/unit/lane_layout_test.cc)
  target_link_libraries(lock_lane_layout_test PRIVATE lock_core)
  add_test(NAME lane_layout COMMAND lock_lane_layout_test)
  add_executable(lock_leader_batch_test tests/unit/leader_batch_test.cc
                 src/locks/mu_compact_log.cc)
  target_link_libraries(lock_leader_batch_test PRIVATE lock_core)
  add_test(NAME leader_batch COMMAND lock_leader_batch_test)
  add_executable(lock_latency_samples_test tests/unit/latency_samples_test.cc)
  target_link_libraries(lock_latency_samples_test PRIVATE lock_core)
  add_test(NAME latency_samples COMMAND lock_latency_samples_test)
  add_executable(lock_workload_test tests/unit/workload_test.cc)
  target_link_libraries(lock_workload_test PRIVATE lock_core)
  add_test(NAME workload COMMAND lock_workload_test)
endif()
