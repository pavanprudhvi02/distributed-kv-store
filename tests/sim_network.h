#pragma once

#include <chrono>
#include <map>
#include <mutex>
#include <random>
#include <set>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/thread_pool.h"
#include "raft/raft_node.h"

namespace kv::test {

// In-process network for Raft tests. Messages are delivered on a thread pool
// with a small random delay, and the test can cut links, partition the
// cluster, take nodes down, or drop a fraction of messages. A message that
// cannot be delivered fails the RPC the way a real timeout would.
class SimNetwork {
 public:
  SimNetwork() : pool_(8), rng_(7) {}
  ~SimNetwork() { pool_.Shutdown(); }

  void Register(const std::string& id, uint32_t group, raft::RaftNode* node) {
    std::unique_lock lock(nodes_mu_);
    nodes_[Key(id, group)] = node;
  }

  // Waits for in-flight deliveries to this node to finish before returning.
  void Unregister(const std::string& id, uint32_t group) {
    std::unique_lock lock(nodes_mu_);
    nodes_.erase(Key(id, group));
  }

  // Nodes in different groups can no longer talk to each other.
  void Partition(const std::vector<std::vector<std::string>>& groups) {
    std::lock_guard lock(mu_);
    side_.clear();
    for (size_t g = 0; g < groups.size(); ++g) {
      for (const auto& id : groups[g]) side_[id] = static_cast<int>(g);
    }
  }

  void Isolate(const std::string& id) {
    std::lock_guard lock(mu_);
    isolated_.insert(id);
  }

  void Heal() {
    std::lock_guard lock(mu_);
    side_.clear();
    isolated_.clear();
    drop_rate_ = 0;
  }

  void SetDropRate(double rate) {
    std::lock_guard lock(mu_);
    drop_rate_ = rate;
  }

  bool CanDeliver(const std::string& from, const std::string& to) {
    std::lock_guard lock(mu_);
    if (isolated_.count(from) || isolated_.count(to)) return false;
    auto a = side_.find(from), b = side_.find(to);
    if (a != side_.end() && b != side_.end() && a->second != b->second) return false;
    if (drop_rate_ > 0 && std::uniform_real_distribution<double>(0, 1)(rng_) < drop_rate_) return false;
    return true;
  }

  template <typename Req, typename Resp, typename Handler, typename Callback>
  void Send(const std::string& from, const std::string& to, uint32_t group, const Req& req, Handler handler,
            Callback cb) {
    pool_.Submit([=, this] {
      std::this_thread::sleep_for(std::chrono::microseconds(200 + Jitter()));
      if (!CanDeliver(from, to)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));  // looks like a timeout
        cb(false, Resp{});
        return;
      }
      Resp resp;
      bool delivered = false;
      {
        std::shared_lock lock(nodes_mu_);
        auto it = nodes_.find(Key(to, group));
        if (it != nodes_.end()) {
          resp = handler(it->second, req);
          delivered = true;
        }
      }
      // The reply can be lost too.
      if (!delivered || !CanDeliver(to, from)) {
        cb(false, Resp{});
        return;
      }
      cb(true, resp);
    });
  }

 private:
  static std::string Key(const std::string& id, uint32_t group) { return id + "#" + std::to_string(group); }
  int Jitter() {
    std::lock_guard lock(mu_);
    return std::uniform_int_distribution<int>(0, 1500)(rng_);
  }

  ThreadPool pool_;
  std::shared_mutex nodes_mu_;
  std::map<std::string, raft::RaftNode*> nodes_;
  std::mutex mu_;
  std::map<std::string, int> side_;
  std::set<std::string> isolated_;
  double drop_rate_ = 0;
  std::mt19937 rng_;
};

class SimTransport : public raft::Transport {
 public:
  SimTransport(SimNetwork* net, std::string self) : net_(net), self_(std::move(self)) {}

  void RequestVote(const std::string& peer, const raft::RequestVoteRequest& req, VoteCallback cb) override {
    net_->Send<raft::RequestVoteRequest, raft::RequestVoteResponse>(
        self_, peer, req.group, req, [](raft::RaftNode* n, const auto& r) { return n->HandleRequestVote(r); },
        std::move(cb));
  }

  void AppendEntries(const std::string& peer, const raft::AppendEntriesRequest& req, AppendCallback cb) override {
    net_->Send<raft::AppendEntriesRequest, raft::AppendEntriesResponse>(
        self_, peer, req.group, req, [](raft::RaftNode* n, const auto& r) { return n->HandleAppendEntries(r); },
        std::move(cb));
  }

 private:
  SimNetwork* net_;
  std::string self_;
};

}  // namespace kv::test
