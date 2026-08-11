cmake_minimum_required(VERSION 3.20)
project(HFT_KV_DB VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable aggressive optimizations for the hot path, but keep debug symbols for testing
set(CMAKE_CXX_FLAGS "-O3 -g -pthread -Wall -Wextra")

# --- 1. Main Server Binary ---
add_executable(database_server src/main.cpp)

# --- 2. GoogleTest Setup ---
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

enable_testing()

# --- 3. SPSC Queue Test Binary ---
add_executable(test_spsc_queue tests/test_spsc_queue.cpp)
target_link_libraries(test_spsc_queue gtest_main)

include(GoogleTest)
gtest_discover_tests(test_spsc_queue)

# --- 4. MemTable Test Binary ---
add_executable(test_memtable tests/test_memtable.cpp)
target_link_libraries(test_memtable gtest_main)
gtest_discover_tests(test_memtable)

# --- 5. SSTable Test Binary ---
add_executable(test_sstable tests/test_sstable.cpp)
target_link_libraries(test_sstable gtest_main)
gtest_discover_tests(test_sstable)