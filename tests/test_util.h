#pragma once

#include <chrono>
#include <filesystem>
#include <functional>
#include <random>
#include <string>
#include <thread>

namespace kv::test {

// Fresh directory under the system temp dir, removed when the object dies.
class TempDir {
 public:
  TempDir() {
    std::random_device rd;
    path_ = std::filesystem::temp_directory_path() / ("kvtest-" + std::to_string(rd()) + std::to_string(rd()));
    std::filesystem::create_directories(path_);
  }
  ~TempDir() {
    std::error_code ec;
    std::filesystem::remove_all(path_, ec);
  }
  std::string path() const { return path_.string(); }
  std::string sub(const std::string& name) const { return (path_ / name).string(); }

 private:
  std::filesystem::path path_;
};

// Polls `cond` until it holds or the timeout expires.
inline bool Eventually(const std::function<bool()>& cond, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
  auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (cond()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return cond();
}

}  // namespace kv::test
