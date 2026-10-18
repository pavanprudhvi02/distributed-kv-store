#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace kv::raft {

enum class Role { kFollower, kCandidate, kLeader };

inline const char* RoleName(Role r) {
  switch (r) {
    case Role::kFollower: return "follower";
    case Role::kCandidate: return "candidate";
    case Role::kLeader: return "leader";
  }
  return "?";
}

struct LogEntry {
  uint64_t term = 0;
  uint64_t index = 0;
  std::string command;  // empty = no-op written by a new leader
};

struct RequestVoteRequest {
  uint32_t group = 0;
  uint64_t term = 0;
  std::string candidate_id;
  uint64_t last_log_index = 0;
  uint64_t last_log_term = 0;
};

struct RequestVoteResponse {
  uint64_t term = 0;
  bool vote_granted = false;
};

struct AppendEntriesRequest {
  uint32_t group = 0;
  uint64_t term = 0;
  std::string leader_id;
  uint64_t prev_log_index = 0;
  uint64_t prev_log_term = 0;
  std::vector<LogEntry> entries;
  uint64_t leader_commit = 0;
};

struct AppendEntriesResponse {
  uint64_t term = 0;
  bool success = false;
  uint64_t match_index = 0;     // on success: last index known to match the leader
  uint64_t conflict_index = 0;  // on failure: where the leader should retry from
};

// How a node reaches its peers. Calls are asynchronous; `ok == false` means
// the RPC did not get an answer (peer down, partitioned, timed out).
class Transport {
 public:
  virtual ~Transport() = default;
  using VoteCallback = std::function<void(bool ok, const RequestVoteResponse&)>;
  using AppendCallback = std::function<void(bool ok, const AppendEntriesResponse&)>;
  virtual void RequestVote(const std::string& peer, const RequestVoteRequest& req, VoteCallback cb) = 0;
  virtual void AppendEntries(const std::string& peer, const AppendEntriesRequest& req, AppendCallback cb) = 0;
};

// The replicated state machine. Entries are applied in log order, once per
// process lifetime; after a restart the log is replayed from the beginning,
// so Apply must give the same result when replayed in order.
class StateMachine {
 public:
  virtual ~StateMachine() = default;
  virtual void Apply(uint64_t index, const std::string& command) = 0;
};

}  // namespace kv::raft
