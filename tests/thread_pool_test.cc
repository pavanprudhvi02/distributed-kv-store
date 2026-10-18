#include <gtest/gtest.h>

#include <atomic>
#include <vector>

#include "common/thread_pool.h"

namespace kv {

TEST(ThreadPoolTest, RunsAllTasksAndReturnsResults) {
  ThreadPool pool(4);
  std::vector<std::future<int>> results;
  for (int i = 0; i < 100; ++i) results.push_back(pool.Submit([i] { return i * i; }));
  int sum = 0;
  for (auto& f : results) sum += f.get();
  EXPECT_EQ(sum, 328350);
}

TEST(ThreadPoolTest, ShutdownDrainsQueuedWork) {
  std::atomic<int> done{0};
  {
    ThreadPool pool(2);
    for (int i = 0; i < 50; ++i) {
      pool.Submit([&done] {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        done.fetch_add(1);
      });
    }
  }  // destructor shuts down
  EXPECT_EQ(done.load(), 50);
}

TEST(ThreadPoolTest, PropagatesExceptionsThroughFutures) {
  ThreadPool pool(1);
  auto f = pool.Submit([]() -> int { throw std::runtime_error("boom"); });
  EXPECT_THROW(f.get(), std::runtime_error);
}

}  // namespace kv
