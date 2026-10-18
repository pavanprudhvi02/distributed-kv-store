#pragma once

#include <gtest/gtest.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "raft/raft_node.h"
#include "sim_network.h"
#include "test_util.h"

namespace kv::test {

// State machine that records what it applied, so tests can compare replicas.
class RecordingStateMachine : public raft::StateMachine {
 public:
  void Apply(uint64_t index, const std::string& command) override {
    std::lock_guard lock(mu_);
    applied_.emplace_back(index, command);
  }
  std::vector<std::string> Commands() const {
    std::lock_guard lock(mu_);
    std::vector<std::string> out;
    for (const auto& [_, c] : applied_) out.push_back(c);
    return out;
  }
  size_t Count() const {
    std::lock_guard lock(mu_);
    return applied_.size();
  }
  void Clear() {
    std::lock_guard lock(mu_);
    applied_.clear();
  }

 private:
  mutable std::mutex mu_;
  std::vector<std::pair<uint64_t, std::string>> applied_;
};

// N-node Raft group on a simulated network, with on-disk logs so nodes can
// be crashed and restarted.
class RaftCluster {
 public:
  explicit RaftCluster(int n) {
    for (int i = 1; i <= n; ++i) ids_.push_back("n" + std::to_string(i));
    machines_.resize(n);
    transports_.resize(n);
    nodes_.resize(n);
    for (int i = 0; i < n; ++i) {
      machines_[i] = std::make_unique<RecordingStateMachine>();
      transports_[i] = std::make_unique<SimTransport>(&net_, ids_[i]);
      StartNode(i);
    }
  }

  ~RaftCluster() {
    for (size_t i = 0; i < nodes_.size(); ++i) StopNode(static_cast<int>(i));
  }

  raft::RaftConfig ConfigFor(int i) const {
    raft::RaftConfig c;
    c.id = ids_[i];
    for (size_t j = 0; j < ids_.size(); ++j) {
      if (static_cast<int>(j) != i) c.peers.push_back(ids_[j]);
    }
    c.data_dir = dir_.sub(ids_[i]);
    c.election_timeout_min = std::chrono::milliseconds(150);
    c.election_timeout_max = std::chrono::milliseconds(300);
    c.heartbeat_interval = std::chrono::milliseconds(30);
    c.sync_log = false;  // tests crash processes, not machines
    return c;
  }

  void StartNode(int i) {
    nodes_[i] = std::make_unique<raft::RaftNode>(ConfigFor(i), transports_[i].get(), machines_[i].get());
    ASSERT_TRUE(nodes_[i]->Start().ok());
    net_.Register(ids_[i], 0, nodes_[i].get());
  }

  // Simulates a crash: the node stops and loses its in-memory state.
  void StopNode(int i) {
    if (!nodes_[i]) return;
    net_.Unregister(ids_[i], 0);
    nodes_[i]->Stop();
    nodes_[i].reset();
  }

  // Restart re-applies the log from disk, so the recording starts over.
  void RestartNode(int i) {
    StopNode(i);
    machines_[i]->Clear();
    StartNode(i);
  }

  std::optional<int> Leader() const {
    std::optional<int> leader;
    uint64_t best_term = 0;
    for (size_t i = 0; i < nodes_.size(); ++i) {
      if (!nodes_[i]) continue;
      auto s = nodes_[i]->GetStatus();
      if (s.role == raft::Role::kLeader && s.term >= best_term) {
        best_term = s.term;
        leader = static_cast<int>(i);
      }
    }
    return leader;
  }

  int WaitForLeader(std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    std::optional<int> leader;
    EXPECT_TRUE(Eventually([&] { return (leader = Leader()).has_value(); }, timeout)) << "no leader elected";
    return leader.value_or(-1);
  }

  // Proposes on whoever is leader, retrying through elections.
  Status ProposeAnywhere(const std::string& cmd, std::chrono::milliseconds timeout = std::chrono::seconds(10)) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    Status last = Status::Timeout();
    while (std::chrono::steady_clock::now() < deadline) {
      auto leader = Leader();
      if (leader) {
        last = nodes_[*leader]->Propose(cmd, std::chrono::milliseconds(500));
        if (last.ok()) return last;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return last;
  }

  raft::RaftNode& node(int i) { return *nodes_[i]; }
  bool alive(int i) const { return nodes_[i] != nullptr; }
  RecordingStateMachine& machine(int i) { return *machines_[i]; }
  const std::string& id(int i) const { return ids_[i]; }
  SimNetwork& net() { return net_; }
  int size() const { return static_cast<int>(ids_.size()); }

 private:
  TempDir dir_;
  SimNetwork net_;
  std::vector<std::string> ids_;
  std::vector<std::unique_ptr<RecordingStateMachine>> machines_;
  std::vector<std::unique_ptr<SimTransport>> transports_;
  std::vector<std::unique_ptr<raft::RaftNode>> nodes_;
};

}  // namespace kv::test
