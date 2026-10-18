#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <thread>
#include <type_traits>
#include <vector>

namespace kv {

// Fixed-size worker pool. Used for LSM flushes/compactions and for
// dispatching outgoing Raft RPCs so a slow peer never blocks the node.
class ThreadPool {
 public:
  explicit ThreadPool(size_t threads) {
    if (threads == 0) threads = 1;
    workers_.reserve(threads);
    for (size_t i = 0; i < threads; ++i) {
      workers_.emplace_back([this] { WorkerLoop(); });
    }
  }

  ~ThreadPool() { Shutdown(); }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  template <typename F>
  auto Submit(F&& fn) -> std::future<std::invoke_result_t<F>> {
    using R = std::invoke_result_t<F>;
    auto task = std::make_shared<std::packaged_task<R()>>(std::forward<F>(fn));
    std::future<R> result = task->get_future();
    {
      std::lock_guard lock(mu_);
      if (stopping_) {
        // Run inline rather than dropping work submitted during shutdown.
        (*task)();
        return result;
      }
      tasks_.emplace([task] { (*task)(); });
    }
    cv_.notify_one();
    return result;
  }

  // Finishes queued tasks, then joins the workers. Safe to call twice.
  void Shutdown() {
    {
      std::lock_guard lock(mu_);
      if (stopping_) return;
      stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_) {
      if (t.joinable()) t.join();
    }
  }

  size_t size() const { return workers_.size(); }

 private:
  void WorkerLoop() {
    for (;;) {
      std::function<void()> task;
      {
        std::unique_lock lock(mu_);
        cv_.wait(lock, [this] { return stopping_ || !tasks_.empty(); });
        if (tasks_.empty()) return;  // stopping and drained
        task = std::move(tasks_.front());
        tasks_.pop();
      }
      task();
    }
  }

  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex mu_;
  std::condition_variable cv_;
  bool stopping_ = false;
};

}  // namespace kv
