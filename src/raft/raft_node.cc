#include "raft/raft_node.h"

#include <algorithm>
#include <functional>

namespace kv::raft {

RaftNode::RaftNode(RaftConfig config, Transport* transport, StateMachine* state_machine)
    : config_(std::move(config)), transport_(transport), state_machine_(state_machine) {
  std::random_device rd;
  rng_.seed((static_cast<uint64_t>(rd()) << 32) ^ std::hash<std::string>{}(config_.id));
}

RaftNode::~RaftNode() { Stop(); }

Status RaftNode::Start() {
  Status s = log_.Open(config_.data_dir, config_.sync_log);
  if (!s.ok()) return s;
  {
    std::lock_guard lock(mu_);
    role_ = Role::kFollower;
    ResetElectionTimerLocked();
  }
  running_ = true;
  if (config_.background_threads) {
    ticker_ = std::thread([this] { TickerLoop(); });
    applier_ = std::thread([this] { ApplierLoop(); });
  }
  return Status::Ok();
}

RaftNode::Clock::time_point RaftNode::Now() const { return config_.clock ? config_.clock() : Clock::now(); }

void RaftNode::Stop() {
  if (!running_.exchange(false)) return;
  apply_cv_.notify_all();
  state_cv_.notify_all();
  if (ticker_.joinable()) ticker_.join();
  if (applier_.joinable()) applier_.join();
  // Transport callbacks capture `this`; wait for the ones already in flight.
  std::unique_lock lock(mu_);
  state_cv_.wait(lock, [this] { return outstanding_ == 0; });
}

void RaftNode::ResetElectionTimerLocked() {
  int64_t lo = config_.election_timeout_min.count();
  int64_t hi = std::max<int64_t>(lo, config_.election_timeout_max.count());
  std::uniform_int_distribution<int64_t> dist(lo, hi);
  election_timer_start_ = Now();
  election_deadline_ = election_timer_start_ + std::chrono::milliseconds(dist(rng_));
}

void RaftNode::TickerLoop() {
  while (running_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    Tick();
  }
}

void RaftNode::Tick() {
  std::lock_guard lock(mu_);
  if (!running_) return;
  auto now = Now();
  if (role_ == Role::kLeader) {
    if (now >= next_heartbeat_) BroadcastAppendLocked();
  } else if (now >= election_deadline_) {
    StartElectionLocked();
  }
}

void RaftNode::BecomeFollowerLocked(uint64_t term, const std::string& leader) {
  if (term > log_.current_term()) log_.SetTermAndVote(term, "");
  role_ = Role::kFollower;
  leader_id_ = leader;
  state_cv_.notify_all();  // wake proposers waiting on a leadership they just lost
}

void RaftNode::StartElectionLocked() {
  role_ = Role::kCandidate;
  leader_id_.clear();
  uint64_t term = log_.current_term() + 1;
  if (!log_.SetTermAndVote(term, config_.id).ok()) return;
  votes_ = 1;
  ResetElectionTimerLocked();
  if (votes_ >= Majority()) {
    BecomeLeaderLocked();
    return;
  }

  RequestVoteRequest req;
  req.group = config_.group;
  req.term = term;
  req.candidate_id = config_.id;
  req.last_log_index = log_.LastIndex();
  req.last_log_term = log_.LastTerm();
  for (const auto& peer : config_.peers) {
    ++outstanding_;
    transport_->RequestVote(peer, req, [this, term](bool ok, const RequestVoteResponse& resp) {
      std::lock_guard lock(mu_);
      if (running_ && ok) {
        if (resp.term > log_.current_term()) {
          BecomeFollowerLocked(resp.term, "");
        } else if (role_ == Role::kCandidate && log_.current_term() == term && resp.vote_granted) {
          if (++votes_ >= Majority()) BecomeLeaderLocked();
        }
      }
      if (--outstanding_ == 0) state_cv_.notify_all();
    });
  }
}

void RaftNode::BecomeLeaderLocked() {
  role_ = Role::kLeader;
  leader_id_ = config_.id;
  for (const auto& peer : config_.peers) {
    next_index_[peer] = log_.LastIndex() + 1;
    match_index_[peer] = 0;
    in_flight_[peer] = false;
    acked_seq_[peer] = 0;
  }
  // Committing a no-op from the new term also commits everything before it
  // (Raft §5.4.2) and lets ReadBarrier know the commit index is current.
  LogEntry noop{log_.current_term(), log_.LastIndex() + 1, ""};
  if (!log_.Append({noop}).ok()) {
    BecomeFollowerLocked(log_.current_term(), "");
    return;
  }
  AdvanceCommitLocked();
  BroadcastAppendLocked();
}

void RaftNode::BroadcastAppendLocked() {
  ++heartbeat_seq_;
  next_heartbeat_ = Now() + config_.heartbeat_interval;
  for (const auto& peer : config_.peers) SendAppendLocked(peer);
}

void RaftNode::SendAppendLocked(const std::string& peer) {
  if (in_flight_[peer]) return;  // one outstanding AppendEntries per peer
  uint64_t next = next_index_[peer];
  AppendEntriesRequest req;
  req.group = config_.group;
  req.term = log_.current_term();
  req.leader_id = config_.id;
  req.prev_log_index = next - 1;
  req.prev_log_term = log_.TermAt(next - 1);
  req.entries = log_.Slice(next, config_.max_entries_per_append);
  req.leader_commit = commit_index_;

  in_flight_[peer] = true;
  uint64_t term = req.term;
  uint64_t seq = heartbeat_seq_;
  uint64_t last_sent = req.prev_log_index + req.entries.size();
  ++outstanding_;
  transport_->AppendEntries(peer, req, [this, peer, term, seq, last_sent](bool ok, const AppendEntriesResponse& resp) {
    OnAppendResponse(peer, term, seq, last_sent, ok, resp);
  });
}

void RaftNode::OnAppendResponse(const std::string& peer, uint64_t sent_term, uint64_t seq, uint64_t last_sent,
                                bool ok, const AppendEntriesResponse& resp) {
  std::lock_guard lock(mu_);
  auto done = [this] {
    if (--outstanding_ == 0) state_cv_.notify_all();
  };
  in_flight_[peer] = false;
  if (!running_ || !ok || role_ != Role::kLeader || log_.current_term() != sent_term) return done();

  if (resp.term > log_.current_term()) {
    BecomeFollowerLocked(resp.term, "");
    ResetElectionTimerLocked();
    return done();
  }

  if (resp.term != log_.current_term()) return done();  // stale or from a stopped node

  // Any answer in our term means the peer still accepts us as leader.
  acked_seq_[peer] = std::max(acked_seq_[peer], seq);
  if (resp.success) {
    match_index_[peer] = std::max(match_index_[peer], std::min(resp.match_index, last_sent));
    next_index_[peer] = match_index_[peer] + 1;
    AdvanceCommitLocked();
  } else {
    next_index_[peer] = std::max<uint64_t>(1, std::min(resp.conflict_index, next_index_[peer] - 1));
  }
  state_cv_.notify_all();
  // Keep the pipe full: more entries to send, or a retry after a conflict.
  if (next_index_[peer] <= log_.LastIndex()) SendAppendLocked(peer);
  done();
}

void RaftNode::AdvanceCommitLocked() {
  for (uint64_t n = log_.LastIndex(); n > commit_index_; --n) {
    // Only entries from the current term are committed by counting replicas.
    if (log_.TermAt(n) != log_.current_term()) break;
    size_t count = 1;
    for (const auto& peer : config_.peers) {
      if (match_index_[peer] >= n) ++count;
    }
    if (count >= Majority()) {
      commit_index_ = n;
      apply_cv_.notify_one();
      break;
    }
  }
}

RequestVoteResponse RaftNode::HandleRequestVote(const RequestVoteRequest& req) {
  std::lock_guard lock(mu_);
  RequestVoteResponse resp;
  if (!running_) return resp;  // not started (log not loaded) or already stopped
  if (req.term > log_.current_term()) BecomeFollowerLocked(req.term, "");
  resp.term = log_.current_term();
  if (req.term < log_.current_term()) return resp;

  bool up_to_date = req.last_log_term > log_.LastTerm() ||
                    (req.last_log_term == log_.LastTerm() && req.last_log_index >= log_.LastIndex());
  bool can_vote = log_.voted_for().empty() || log_.voted_for() == req.candidate_id;
  if (can_vote && up_to_date && log_.SetTermAndVote(req.term, req.candidate_id).ok()) {
    resp.vote_granted = true;
    ResetElectionTimerLocked();
  }
  return resp;
}

AppendEntriesResponse RaftNode::HandleAppendEntries(const AppendEntriesRequest& req) {
  std::lock_guard lock(mu_);
  AppendEntriesResponse resp;
  if (!running_) return resp;  // not started (log not loaded) or already stopped
  if (req.term < log_.current_term()) {
    resp.term = log_.current_term();
    return resp;
  }
  if (req.term > log_.current_term() || role_ != Role::kFollower) BecomeFollowerLocked(req.term, req.leader_id);
  leader_id_ = req.leader_id;
  resp.term = log_.current_term();
  ResetElectionTimerLocked();

  // Consistency check, with a hint so the leader can skip a whole term at once.
  if (req.prev_log_index > log_.LastIndex()) {
    resp.conflict_index = log_.LastIndex() + 1;
    return resp;
  }
  if (log_.TermAt(req.prev_log_index) != req.prev_log_term) {
    uint64_t bad_term = log_.TermAt(req.prev_log_index);
    uint64_t i = req.prev_log_index;
    while (i > 1 && log_.TermAt(i - 1) == bad_term) --i;
    resp.conflict_index = i;
    return resp;
  }

  // Skip entries we already have; truncate at the first conflict; append the rest.
  size_t first_new = req.entries.size();
  for (size_t i = 0; i < req.entries.size(); ++i) {
    const auto& e = req.entries[i];
    if (e.index > log_.LastIndex()) {
      first_new = i;
      break;
    }
    if (log_.TermAt(e.index) != e.term) {
      if (!log_.TruncateFrom(e.index).ok()) return resp;
      first_new = i;
      break;
    }
  }
  if (first_new < req.entries.size()) {
    std::vector<LogEntry> tail(req.entries.begin() + static_cast<long>(first_new), req.entries.end());
    if (!log_.Append(tail).ok()) return resp;
  }

  uint64_t last_new = req.prev_log_index + req.entries.size();
  if (req.leader_commit > commit_index_) {
    commit_index_ = std::min(req.leader_commit, last_new);
    apply_cv_.notify_one();
  }
  resp.success = true;
  resp.match_index = last_new;
  return resp;
}

void RaftNode::ApplierLoop() {
  while (running_) {
    {
      std::unique_lock lock(mu_);
      apply_cv_.wait(lock, [this] { return !running_ || commit_index_ > last_applied_; });
      if (!running_) return;
    }
    ApplyCommitted();
  }
}

size_t RaftNode::ApplyCommitted() {
  std::vector<LogEntry> batch;
  {
    std::lock_guard lock(mu_);
    if (commit_index_ <= last_applied_) return 0;
    batch = log_.Slice(last_applied_ + 1, static_cast<size_t>(commit_index_ - last_applied_));
  }
  // Committed entries never change, so they can be applied without the lock.
  for (const auto& e : batch) {
    if (!e.command.empty()) state_machine_->Apply(e.index, e.command);
  }
  {
    std::lock_guard lock(mu_);
    if (!batch.empty()) last_applied_ = std::max(last_applied_, batch.back().index);
  }
  state_cv_.notify_all();
  return batch.size();
}

Status RaftNode::AppendLocked(const std::string& command, LogEntry* out) {
  if (command.empty()) return Status::InvalidArgument("empty command");
  if (role_ != Role::kLeader) return Status::NotLeader(leader_id_);
  LogEntry entry{log_.current_term(), log_.LastIndex() + 1, command};
  Status s = log_.Append({entry});
  if (!s.ok()) return s;
  AdvanceCommitLocked();  // single-node groups commit immediately
  for (const auto& peer : config_.peers) SendAppendLocked(peer);
  *out = entry;
  return Status::Ok();
}

Status RaftNode::Append(const std::string& command, uint64_t* index, uint64_t* term) {
  std::lock_guard lock(mu_);
  LogEntry entry;
  Status s = AppendLocked(command, &entry);
  if (!s.ok()) return s;
  if (index != nullptr) *index = entry.index;
  if (term != nullptr) *term = entry.term;
  return Status::Ok();
}

Status RaftNode::Propose(const std::string& command, std::chrono::milliseconds timeout, uint64_t* index) {
  std::unique_lock lock(mu_);
  LogEntry entry;
  Status s = AppendLocked(command, &entry);
  if (!s.ok()) return s;
  if (index != nullptr) *index = entry.index;
  uint64_t term = entry.term;

  bool done = state_cv_.wait_for(lock, timeout, [&] {
    return !running_ || last_applied_ >= entry.index || role_ != Role::kLeader || log_.current_term() != term;
  });
  if (last_applied_ >= entry.index) {
    // Applied: it is ours only if the entry at that index is still from our term.
    return log_.TermAt(entry.index) == term ? Status::Ok() : Status::NotLeader(leader_id_);
  }
  if (!done) return Status::Timeout("not committed in time");
  // Leadership changed. The entry may still commit under the new leader,
  // so the client has to treat the outcome as unknown and retry.
  return Status::NotLeader(leader_id_);
}

Status RaftNode::ReadBarrier(std::chrono::milliseconds timeout) {
  auto deadline = Clock::now() + timeout;
  std::unique_lock lock(mu_);
  if (role_ != Role::kLeader) return Status::NotLeader(leader_id_);
  uint64_t term = log_.current_term();
  auto still_leader = [&] { return running_ && role_ == Role::kLeader && log_.current_term() == term; };

  // The commit index is only trustworthy once an entry from this term has committed.
  if (!state_cv_.wait_until(lock, deadline, [&] { return !still_leader() || log_.TermAt(commit_index_) == term; })) {
    return Status::Timeout("leader has not committed in its term yet");
  }
  if (!still_leader()) return Status::NotLeader(leader_id_);
  uint64_t read_index = commit_index_;

  if (!config_.peers.empty()) {
    BroadcastAppendLocked();
    uint64_t seq = heartbeat_seq_;
    auto confirmed = [&] {
      size_t acks = 1;
      for (const auto& peer : config_.peers) {
        if (acked_seq_[peer] >= seq) ++acks;
      }
      return acks >= Majority();
    };
    if (!state_cv_.wait_until(lock, deadline, [&] { return !still_leader() || confirmed(); })) {
      return Status::Timeout("could not confirm leadership");
    }
    if (!still_leader()) return Status::NotLeader(leader_id_);
  }

  if (!state_cv_.wait_until(lock, deadline, [&] { return !running_ || last_applied_ >= read_index; })) {
    return Status::Timeout("apply did not catch up");
  }
  return Status::Ok();
}

RaftStatus RaftNode::GetStatus() const {
  std::lock_guard lock(mu_);
  RaftStatus s;
  s.id = config_.id;
  s.role = role_;
  s.term = log_.current_term();
  s.leader_id = leader_id_;
  s.last_index = log_.LastIndex();
  s.commit_index = commit_index_;
  s.last_applied = last_applied_;
  s.voted_for = log_.voted_for();
  s.election_timer_start = election_timer_start_;
  s.election_deadline = election_deadline_;
  return s;
}

std::vector<LogEntry> RaftNode::LogSlice(uint64_t from, size_t max) const {
  std::lock_guard lock(mu_);
  return log_.Slice(std::max<uint64_t>(from, 1), max);
}

bool RaftNode::IsLeader() const {
  std::lock_guard lock(mu_);
  return role_ == Role::kLeader;
}

}  // namespace kv::raft
