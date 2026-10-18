#pragma once

#include <map>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>

namespace kv {

// A value or a tombstone. Tombstones must be kept until compaction so a
// delete hides older versions of the key in older SSTables.
struct Entry {
  bool deleted = false;
  std::string value;
};

// Sorted in-memory buffer for recent writes. Thread-safe: readers take a
// shared lock, writers an exclusive one.
class MemTable {
 public:
  void Put(std::string_view key, std::string_view value);
  void Delete(std::string_view key);

  // nullopt: key never written here. Entry with deleted=true: tombstone.
  std::optional<Entry> Get(std::string_view key) const;

  size_t ApproximateBytes() const;
  size_t size() const;
  bool empty() const { return size() == 0; }

  // Snapshot of the contents in key order (used when flushing to an SSTable).
  std::map<std::string, Entry, std::less<>> Snapshot() const;

 private:
  mutable std::shared_mutex mu_;
  std::map<std::string, Entry, std::less<>> table_;
  size_t bytes_ = 0;
};

}  // namespace kv
