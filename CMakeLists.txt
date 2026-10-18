cmake_minimum_required(VERSION 3.20)
project(distributed_kv_store LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

option(KV_BUILD_SERVER "Build the gRPC server and client (requires gRPC and Protobuf)" ON)
option(KV_BUILD_TESTS "Build the test suite" ON)
option(KV_SANITIZE "Build with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(KV_TSAN "Build with ThreadSanitizer" OFF)

add_compile_options(-Wall -Wextra -Wpedantic)
if(KV_SANITIZE)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()
if(KV_TSAN)
  add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
  add_link_options(-fsanitize=thread)
endif()

find_package(Threads REQUIRED)

# Core library: storage engine, Raft and sharding. No gRPC dependency, so the
# whole consensus layer can be tested in-process with a simulated network.
add_library(kvcore
  src/storage/bloom.cc
  src/storage/wal.cc
  src/storage/memtable.cc
  src/storage/sstable.cc
  src/storage/lsm.cc
  src/raft/raft_log.cc
  src/raft/raft_node.cc
  src/raft/command.cc
  src/shard/hash_ring.cc
  src/shard/shard_group.cc
)
target_include_directories(kvcore PUBLIC src)
target_link_libraries(kvcore PUBLIC Threads::Threads)

if(KV_BUILD_SERVER)
  find_package(Protobuf CONFIG QUIET)
  if(NOT Protobuf_FOUND)
    find_package(Protobuf REQUIRED)
  endif()
  find_package(gRPC CONFIG REQUIRED)

  set(PROTO_SRC ${CMAKE_CURRENT_SOURCE_DIR}/proto/kv.proto)
  set(GEN_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
  file(MAKE_DIRECTORY ${GEN_DIR})
  get_target_property(GRPC_CPP_PLUGIN gRPC::grpc_cpp_plugin LOCATION)
  if(TARGET protobuf::protoc)
    set(PROTOC_BIN $<TARGET_FILE:protobuf::protoc>)
  else()
    set(PROTOC_BIN ${Protobuf_PROTOC_EXECUTABLE})
  endif()
  add_custom_command(
    OUTPUT ${GEN_DIR}/kv.pb.cc ${GEN_DIR}/kv.pb.h ${GEN_DIR}/kv.grpc.pb.cc ${GEN_DIR}/kv.grpc.pb.h
    COMMAND ${PROTOC_BIN}
      --proto_path=${CMAKE_CURRENT_SOURCE_DIR}/proto
      --cpp_out=${GEN_DIR}
      --grpc_out=${GEN_DIR}
      --plugin=protoc-gen-grpc=${GRPC_CPP_PLUGIN}
      ${PROTO_SRC}
    DEPENDS ${PROTO_SRC}
    COMMENT "Generating gRPC code from kv.proto")

  add_library(kvproto ${GEN_DIR}/kv.pb.cc ${GEN_DIR}/kv.grpc.pb.cc)
  target_include_directories(kvproto PUBLIC ${GEN_DIR})
  target_link_libraries(kvproto PUBLIC gRPC::grpc++ protobuf::libprotobuf)
  target_compile_options(kvproto PRIVATE -w)

  add_library(kvnet src/server/grpc_transport.cc src/server/kv_service.cc src/client/kv_client.cc)
  target_link_libraries(kvnet PUBLIC kvcore kvproto)

  add_executable(kvd src/server/main.cc)
  target_link_libraries(kvd PRIVATE kvnet)

  add_executable(kvctl src/client/kvctl.cc)
  target_link_libraries(kvctl PRIVATE kvnet)

  add_executable(kvbench src/client/kvbench.cc)
  target_link_libraries(kvbench PRIVATE kvnet)
endif()

if(KV_BUILD_TESTS)
  include(FetchContent)
  FetchContent_Declare(googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG v1.15.2
    GIT_SHALLOW TRUE)
  set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  enable_testing()
  add_executable(kv_tests
    tests/thread_pool_test.cc
    tests/storage_test.cc
    tests/lsm_test.cc
    tests/hash_ring_test.cc
    tests/raft_test.cc
    tests/raft_partition_test.cc
    tests/shard_test.cc
  )
  target_link_libraries(kv_tests PRIVATE kvcore GTest::gtest_main)
  include(GoogleTest)
  gtest_discover_tests(kv_tests DISCOVERY_TIMEOUT 60 PROPERTIES TIMEOUT 120)
endif()
