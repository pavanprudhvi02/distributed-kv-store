#include "shard/shard_group.h"

#include "raft/command.h"

namespace kv {

ShardReplica::ShardReplica(Options options, raft::Transport* transport) : options_(std::move(options)) {
  raft_ = std::make_unique<raft::RaftNode>(options_.raft, transport, this);
}

ShardReplica::~ShardReplica() { Stop(); }

Status ShardReplica::Start() {
  Status s = LsmTree::Open(options_.storage, &storage_);
  if (!s.ok()) return s;
  return raft_->Start();
}

void ShardReplica::Stop() {
  if (raft_) raft_->Stop();
}

Status ShardReplica::Put(const std::string& key, const std::string& value) {
  if (key.empty()) return Status::InvalidArgument("empty key");
  return raft_->Propose(Command::Put(key, value).Encode(), options_.request_timeout);
}

Status ShardReplica::Delete(const std::string& key) {
  if (key.empty()) return Status::InvalidArgument("empty key");
  return raft_->Propose(Command::Delete(key).Encode(), options_.request_timeout);
}

Status ShardReplica::Get(const std::string& key, std::string* value) {
  Status s = raft_->ReadBarrier(options_.request_timeout);
  if (!s.ok()) return s;
  return storage_->Get(key, value);
}

void ShardReplica::Apply(uint64_t /*index*/, const std::string& command) {
  auto cmd = Command::Decode(command);
  if (!cmd) return;  // unknown entry type: skip rather than wedge the applier
  if (cmd->op == Command::Op::kPut) {
    storage_->Put(cmd->key, cmd->value);
  } else {
    storage_->Delete(cmd->key);
  }
}

}  // namespace kv
