#pragma once

#include <string>
#include <utility>

namespace kv {

// Small status type so storage and Raft code can report errors without exceptions
// crossing thread boundaries.
class Status {
 public:
  enum class Code { kOk, kNotFound, kCorruption, kIOError, kNotLeader, kTimeout, kInvalidArgument };

  Status() = default;
  static Status Ok() { return {}; }
  static Status NotFound(std::string msg = {}) { return {Code::kNotFound, std::move(msg)}; }
  static Status Corruption(std::string msg) { return {Code::kCorruption, std::move(msg)}; }
  static Status IOError(std::string msg) { return {Code::kIOError, std::move(msg)}; }
  static Status NotLeader(std::string leader_hint = {}) { return {Code::kNotLeader, std::move(leader_hint)}; }
  static Status Timeout(std::string msg = {}) { return {Code::kTimeout, std::move(msg)}; }
  static Status InvalidArgument(std::string msg) { return {Code::kInvalidArgument, std::move(msg)}; }

  bool ok() const { return code_ == Code::kOk; }
  bool IsNotFound() const { return code_ == Code::kNotFound; }
  bool IsNotLeader() const { return code_ == Code::kNotLeader; }
  bool IsTimeout() const { return code_ == Code::kTimeout; }
  Code code() const { return code_; }
  const std::string& message() const { return message_; }

  std::string ToString() const {
    switch (code_) {
      case Code::kOk: return "OK";
      case Code::kNotFound: return "NotFound: " + message_;
      case Code::kCorruption: return "Corruption: " + message_;
      case Code::kIOError: return "IOError: " + message_;
      case Code::kNotLeader: return "NotLeader: " + message_;
      case Code::kTimeout: return "Timeout: " + message_;
      case Code::kInvalidArgument: return "InvalidArgument: " + message_;
    }
    return "Unknown";
  }

 private:
  Status(Code code, std::string msg) : code_(code), message_(std::move(msg)) {}
  Code code_ = Code::kOk;
  std::string message_;
};

}  // namespace kv
