#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "common/thread_pool.h"
#include "storage/memtable.h"
#include "storage/sstable.h"
#include "storage/wal.h"

namespace kv {

struct LsmOptions {
  std::string dir;
  size_t memtable_bytes = 4 * 1024 * 1024;
  size_t compaction_trigger = 4;  // compact when this many SSTables exist
  bool sync_writes = false;       // fsync the WAL on every write
};

struct LsmStats {
  size_t sstables = 0;
  uint64_t flushes = 0;
  uint64_t compactions = 0;
};

// Log-structured merge tree:
//   writes  -> WAL + active memtable
//   full memtable -> becomes immutable, flushed to a new SSTable in the background
//   too many SSTables -> merged into one in the background (tombstones dropped)
//   reads   -> active memtable, immutable memtable, then SSTables newest first
//
// The list of live SSTables is recorded in a MANIFEST file that is replaced
// atomically, so a crash never leaves the engine pointing at a half-written table.
class LsmTree {
 public:
  static Status Open(const LsmOptions& options, std::unique_ptr<LsmTree>* db);
  ~LsmTree();

  Status Put(std::string_view key, std::string_view value);
  Status Delete(std::string_view key);
  Status Get(std::string_view key, std::string* value) const;

  // Blocks until pending flushes and compactions are done (tests and shutdown).
  void WaitForBackgroundWork();
  // Forces the active memtable to disk.
  Status Flush();
  LsmStats Stats() const;

 private:
  struct Version {
    std::vector<std::shared_ptr<SSTable>> tables;  // newest first
    std::vector<uint64_t> numbers;                 // file numbers, same order
  };

  explicit LsmTree(const LsmOptions& options);
  Status Recover();
  Status Write(RecordType type, std::string_view key, std::string_view value);
  Status RotateMemtableLocked(std::unique_lock<std::mutex>& lock);
  void BackgroundFlush();
  void MaybeScheduleCompaction();
  void BackgroundCompaction();
  Status WriteManifest(const Version& v);
  std::string TablePath(uint64_t number) const;
  std::string WalPath(uint64_t number) const;

  LsmOptions options_;
  ThreadPool pool_{1};  // one background thread: flushes and compactions never race each other

  mutable std::mutex mu_;  // guards everything below
  std::condition_variable bg_cv_;
  std::shared_ptr<MemTable> mem_;
  std::shared_ptr<MemTable> imm_;
  std::unique_ptr<WriteAheadLog> wal_;
  std::string imm_wal_path_;
  std::shared_ptr<const Version> version_;
  uint64_t next_file_number_ = 1;
  bool flush_scheduled_ = false;
  bool compaction_scheduled_ = false;
  Status bg_error_;
  LsmStats stats_;
};

}  // namespace kv
