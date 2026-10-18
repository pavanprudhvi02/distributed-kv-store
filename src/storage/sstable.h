#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/memtable.h"

namespace kv {

// Immutable sorted file. Layout:
//
//   [data: records in key order]
//   [sparse index: (key, offset) for every 16th record]
//   [bloom filter]
//   [footer: index_off, index_size, bloom_off, bloom_size (8 bytes each),
//            crc32 of everything before the footer (4), magic (4)]
//
// A record is [type : 1][key (length-prefixed)][value (length-prefixed)].
// Readers keep the index and bloom filter in memory and read one index
// interval from disk per lookup.
class SSTable {
 public:
  static constexpr uint32_t kMagic = 0x4b565354;  // "KVST"
  static constexpr size_t kFooterSize = 40;
  static constexpr size_t kIndexInterval = 16;

  // Writes `entries` (already sorted) to `path`, fsyncs it, and returns its size.
  // Entries may come from anywhere that can be iterated in order.
  using EntrySource = std::function<bool(std::string* key, Entry* entry)>;
  static Status Write(const std::string& path, const EntrySource& next, uint64_t* file_size = nullptr);
  static Status Write(const std::string& path, const std::map<std::string, Entry, std::less<>>& entries);

  static Status Open(const std::string& path, std::shared_ptr<SSTable>* table);
  ~SSTable();

  // nullopt if the key is not in this table; a tombstone Entry if it was deleted.
  std::optional<Entry> Get(std::string_view key) const;

  // Sequential scan of every record, in key order. Used by compaction.
  class Iterator {
   public:
    explicit Iterator(const SSTable* table);
    bool Valid() const { return valid_; }
    void Next();
    const std::string& key() const { return key_; }
    const Entry& entry() const { return entry_; }

   private:
    const SSTable* table_;
    uint64_t pos_ = 0;
    bool valid_ = false;
    std::string buffer_;
    uint64_t buffer_start_ = 0;
    std::string key_;
    Entry entry_;
  };
  std::unique_ptr<Iterator> NewIterator() const { return std::make_unique<Iterator>(this); }

  const std::string& path() const { return path_; }
  uint64_t data_size() const { return index_offset_; }
  size_t entry_count() const { return entry_count_; }

 private:
  SSTable() = default;
  bool ReadAt(uint64_t offset, size_t n, std::string* out) const;

  std::string path_;
  int fd_ = -1;
  uint64_t index_offset_ = 0;
  std::vector<std::pair<std::string, uint64_t>> index_;
  std::string bloom_;
  size_t entry_count_ = 0;
};

}  // namespace kv
