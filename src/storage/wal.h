#pragma once

#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <string_view>

#include "common/status.h"

namespace kv {

enum class RecordType : uint8_t { kPut = 1, kDelete = 2 };

// Append-only write-ahead log. Each record is
//   [crc32 of payload : 4][payload length : 4][payload]
// with payload = [type : 1][key (length-prefixed)][value (length-prefixed)].
// Replay stops at the first truncated or corrupt record, which is what a
// crash in the middle of an append leaves behind.
class WriteAheadLog {
 public:
  WriteAheadLog() = default;
  ~WriteAheadLog();
  WriteAheadLog(const WriteAheadLog&) = delete;
  WriteAheadLog& operator=(const WriteAheadLog&) = delete;

  Status Open(const std::string& path);
  Status Append(RecordType type, std::string_view key, std::string_view value, bool sync);
  Status Sync();
  void Close();
  const std::string& path() const { return path_; }

  using Visitor = std::function<void(RecordType, std::string_view key, std::string_view value)>;
  // Calls `visit` for every intact record. `valid_records` reports how many were read.
  static Status Replay(const std::string& path, const Visitor& visit, size_t* valid_records = nullptr);

 private:
  std::FILE* file_ = nullptr;
  std::string path_;
};

}  // namespace kv
