#pragma once

#include <cstdio>
#include <string>
#include <vector>

#include "common/status.h"
#include "raft/types.h"

namespace kv::raft {

// Durable Raft state: the log plus (currentTerm, votedFor).
//
// The log file is a sequence of CRC-protected records, one per entry. Appends
// go to the end; truncating a conflicting suffix rewrites the file. The term
// and vote live in a small metadata file replaced atomically via rename.
// Both must be on disk before the node answers an RPC that depends on them.
class RaftLog {
 public:
  RaftLog() = default;
  ~RaftLog();

  // `dir` empty keeps everything in memory (used by some tests).
  Status Open(const std::string& dir, bool sync);

  uint64_t current_term() const { return current_term_; }
  const std::string& voted_for() const { return voted_for_; }
  Status SetTermAndVote(uint64_t term, const std::string& voted_for);

  uint64_t LastIndex() const { return entries_.size() - 1; }
  uint64_t LastTerm() const { return entries_.back().term; }
  uint64_t TermAt(uint64_t index) const { return index <= LastIndex() ? entries_[index].term : 0; }
  const LogEntry& At(uint64_t index) const { return entries_[index]; }

  // Returns entries [from, from + max) as copies, for AppendEntries.
  std::vector<LogEntry> Slice(uint64_t from, size_t max) const;

  Status Append(const std::vector<LogEntry>& entries);
  // Removes every entry with index >= `index`.
  Status TruncateFrom(uint64_t index);

 private:
  Status WriteRecords(std::FILE* f, const std::vector<LogEntry>& entries, size_t from);
  Status SyncFile(std::FILE* f);

  std::string dir_;
  bool sync_ = true;
  std::FILE* log_file_ = nullptr;
  std::vector<LogEntry> entries_{LogEntry{}};  // index 0 is a sentinel with term 0
  uint64_t current_term_ = 0;
  std::string voted_for_;
};

}  // namespace kv::raft
