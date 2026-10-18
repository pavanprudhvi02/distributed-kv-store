#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "raft/raft_log.h"
#include "raft/types.h"

namespace kv::raft {

struct RaftConfig {
  std::string id;
  std::vector<std::string> peers;  // other members of the group, not including `id`
  uint32_t group = 0;              // shard id, so one process can host several groups
  std::string data_dir;            // empty: in-memory only
  std::chrono::milliseconds election_timeout_min{300};
  std::chrono::milliseconds election_timeout_max{600};
  std::chrono::milliseconds heartbeat_interval{60};
  size_t max_entries_per_append = 256;
  bool sync_log = true;
};

struct RaftStatus {
  std::string id;
  Role role = Role::kFollower;
  uint64_t term = 0;
  std::string leader_id;
  uint64_t last_index = 0;
  uint64_t commit_index = 0;
  uint64_t last_applied = 0;
};

// One member of a Raft group (Ongaro & Ousterhout, "In Search of an
// Understandable Consensus Algorithm").
//
// Covered: leader election with randomized timeouts, log replication with
// fast conflict backtracking, commitment only of current-term entries,
// durable term/vote/log, a no-op entry on election, and ReadIndex reads that
// confirm leadership with a heartbeat round before answering.
// Not covered (see README): snapshots/log compaction and membership changes.
//
// Threads: a ticker drives elections and heartbeats, an applier hands
// committed entries to the state machine. RPC handlers and transport
// callbacks may run on any thread; all state is guarded by one mutex.
class RaftNode {
 public:
  RaftNode(RaftConfig config, Transport* transport, StateMachine* state_machine);
  ~RaftNode();
  RaftNode(const RaftNode&) = delete;
  RaftNode& operator=(const RaftNode&) = delete;

  Status Start();
  void Stop();

  // Replicates `command` and waits until it is applied locally.
  // Returns NotLeader (with a leader hint when known) on followers.
  Status Propose(const std::string& command, std::chrono::milliseconds timeout, uint64_t* index = nullptr);

  // Linearizable read barrier: returns Ok once this node is confirmed as leader
  // and has applied everything committed before the call.
  Status ReadBarrier(std::chrono::milliseconds timeout);

  RequestVoteResponse HandleRequestVote(const RequestVoteRequest& req);
  AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& req);

  RaftStatus GetStatus() const;
  bool IsLeader() const;
  const std::string& id() const { return config_.id; }

 private:
  using Clock = std::chrono::steady_clock;

  void TickerLoop();
  void ApplierLoop();

  void BecomeFollowerLocked(uint64_t term, const std::string& leader);
  void StartElectionLocked();
  void BecomeLeaderLocked();
  void BroadcastAppendLocked();
  void SendAppendLocked(const std::string& peer);
  void OnAppendResponse(const std::string& peer, uint64_t sent_term, uint64_t heartbeat_seq, uint64_t last_sent,
                        bool ok, const AppendEntriesResponse& resp);
  void AdvanceCommitLocked();
  void ResetElectionTimerLocked();
  size_t Majority() const { return (config_.peers.size() + 1) / 2 + 1; }

  RaftConfig config_;
  Transport* transport_;
  StateMachine* state_machine_;

  mutable std::mutex mu_;
  std::condition_variable apply_cv_;   // wakes the applier
  std::condition_variable state_cv_;   // wakes Propose/ReadBarrier waiters
  RaftLog log_;
  Role role_ = Role::kFollower;
  std::string leader_id_;
  uint64_t commit_index_ = 0;
  uint64_t last_applied_ = 0;
  Clock::time_point election_deadline_;
  Clock::time_point next_heartbeat_;
  std::mt19937_64 rng_;

  // Candidate state.
  size_t votes_ = 0;

  // Leader state, per peer.
  std::map<std::string, uint64_t> next_index_;
  std::map<std::string, uint64_t> match_index_;
  std::map<std::string, bool> in_flight_;
  // ReadIndex: each broadcast gets a sequence number; a peer's ack of seq N
  // proves we were still leader when broadcast N went out.
  uint64_t heartbeat_seq_ = 0;
  std::map<std::string, uint64_t> acked_seq_;

  size_t outstanding_ = 0;  // transport callbacks not yet run
  std::atomic<bool> running_{false};
  std::thread ticker_;
  std::thread applier_;
};

}  // namespace kv::raft
