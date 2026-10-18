#pragma once

#include <chrono>
#include <memory>
#include <string>

#include "common/status.h"
#include "raft/raft_node.h"
#include "storage/lsm.h"

namespace kv {

// One replica of one shard: a Raft node whose state machine is an LSM tree.
// Writes go through the Raft log; reads use a ReadIndex barrier on the leader
// so they never return stale data.
class ShardReplica : public raft::StateMachine {
 public:
  struct Options {
    raft::RaftConfig raft;
    LsmOptions storage;
    std::chrono::milliseconds request_timeout{3000};
  };

  ShardReplica(Options options, raft::Transport* transport);
  ~ShardReplica() override;

  Status Start();
  void Stop();

  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Get(const std::string& key, std::string* value);

  void Apply(uint64_t index, const std::string& command) override;

  raft::RaftNode& raft() { return *raft_; }
  LsmTree& storage() { return *storage_; }

 private:
  Options options_;
  std::unique_ptr<LsmTree> storage_;
  std::unique_ptr<raft::RaftNode> raft_;
};

}  // namespace kv
