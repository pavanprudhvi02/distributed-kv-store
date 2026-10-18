#include "server/kv_service.h"

#include <iostream>

namespace kv {

namespace {

kvstore::Code ToCode(const Status& s) {
  switch (s.code()) {
    case Status::Code::kOk: return kvstore::OK;
    case Status::Code::kNotFound: return kvstore::NOT_FOUND;
    case Status::Code::kNotLeader: return kvstore::NOT_LEADER;
    case Status::Code::kTimeout: return kvstore::TIMEOUT;
    case Status::Code::kInvalidArgument: return kvstore::INVALID_ARGUMENT;
    default: return kvstore::INTERNAL;
  }
}

}  // namespace

KvNode::KvNode(NodeConfig config) : config_(std::move(config)) {
  for (uint32_t s = 0; s < config_.shards; ++s) ring_.AddShard(s);

  std::map<std::string, std::string> peers;
  for (const auto& [id, addr] : config_.cluster) {
    if (id != config_.id) peers[id] = addr;
  }
  transport_ = std::make_unique<GrpcTransport>(peers);

  for (uint32_t s = 0; s < config_.shards; ++s) {
    ShardReplica::Options o;
    o.raft.id = config_.id;
    o.raft.group = s;
    for (const auto& [id, _] : peers) o.raft.peers.push_back(id);
    std::string base = config_.data_dir + "/shard-" + std::to_string(s);
    o.raft.data_dir = base + "/raft";
    o.raft.sync_log = config_.sync;
    o.storage.dir = base + "/data";
    replicas_.push_back(std::make_unique<ShardReplica>(o, transport_.get()));
  }
}

KvNode::~KvNode() { Shutdown(); }

Status KvNode::Start() {
  // Replicas load their logs before the port opens, so no RPC ever reaches a
  // Raft node that has not read its state from disk yet.
  for (auto& r : replicas_) {
    Status s = r->Start();
    if (!s.ok()) return s;
  }
  grpc::ServerBuilder builder;
  builder.AddListeningPort(config_.listen_address, grpc::InsecureServerCredentials());
  builder.RegisterService(static_cast<kvstore::KeyValue::Service*>(this));
  builder.RegisterService(static_cast<kvstore::Raft::Service*>(this));
  server_ = builder.BuildAndStart();
  if (!server_) return Status::IOError("could not listen on " + config_.listen_address);
  return Status::Ok();
}

void KvNode::Wait() {
  if (server_) server_->Wait();
}

void KvNode::Shutdown() {
  if (server_) {
    server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    server_.reset();
  }
  for (auto& r : replicas_) r->Stop();
}

ShardReplica* KvNode::ReplicaFor(const std::string& key) { return replicas_[ring_.ShardFor(key)].get(); }

template <typename Resp>
void KvNode::Fill(const Status& s, ShardReplica* replica, Resp* resp) {
  resp->set_code(ToCode(s));
  if (s.IsNotLeader()) {
    auto it = config_.cluster.find(replica->raft().GetStatus().leader_id);
    if (it != config_.cluster.end()) resp->set_leader_address(it->second);
  } else if (!s.ok() && !s.IsNotFound()) {
    resp->set_error(s.ToString());
  }
}

grpc::Status KvNode::Put(grpc::ServerContext*, const kvstore::PutRequest* req, kvstore::WriteResponse* resp) {
  auto* replica = ReplicaFor(req->key());
  Fill(replica->Put(req->key(), req->value()), replica, resp);
  return grpc::Status::OK;
}

grpc::Status KvNode::Delete(grpc::ServerContext*, const kvstore::DeleteRequest* req, kvstore::WriteResponse* resp) {
  auto* replica = ReplicaFor(req->key());
  Fill(replica->Delete(req->key()), replica, resp);
  return grpc::Status::OK;
}

grpc::Status KvNode::Get(grpc::ServerContext*, const kvstore::GetRequest* req, kvstore::GetResponse* resp) {
  auto* replica = ReplicaFor(req->key());
  std::string value;
  Status s = replica->Get(req->key(), &value);
  Fill(s, replica, resp);
  if (s.ok()) resp->set_value(value);
  return grpc::Status::OK;
}

grpc::Status KvNode::ClusterStatus(grpc::ServerContext*, const kvstore::StatusRequest*, kvstore::StatusResponse* resp) {
  resp->set_node_id(config_.id);
  resp->set_shard_count(config_.shards);
  for (uint32_t s = 0; s < replicas_.size(); ++s) {
    auto st = replicas_[s]->raft().GetStatus();
    auto* out = resp->add_shards();
    out->set_shard(s);
    out->set_role(raft::RoleName(st.role));
    out->set_term(st.term);
    out->set_leader_id(st.leader_id);
    out->set_last_index(st.last_index);
    out->set_commit_index(st.commit_index);
    out->set_last_applied(st.last_applied);
  }
  return grpc::Status::OK;
}

grpc::Status KvNode::RequestVote(grpc::ServerContext*, const kvstore::VoteRequest* req, kvstore::VoteResponse* resp) {
  if (req->group() >= replicas_.size()) return {grpc::StatusCode::INVALID_ARGUMENT, "unknown group"};
  raft::RequestVoteRequest r;
  r.group = req->group();
  r.term = req->term();
  r.candidate_id = req->candidate_id();
  r.last_log_index = req->last_log_index();
  r.last_log_term = req->last_log_term();
  auto out = replicas_[req->group()]->raft().HandleRequestVote(r);
  resp->set_term(out.term);
  resp->set_vote_granted(out.vote_granted);
  return grpc::Status::OK;
}

grpc::Status KvNode::AppendEntries(grpc::ServerContext*, const kvstore::AppendRequest* req,
                                   kvstore::AppendResponse* resp) {
  if (req->group() >= replicas_.size()) return {grpc::StatusCode::INVALID_ARGUMENT, "unknown group"};
  auto out = replicas_[req->group()]->raft().HandleAppendEntries(GrpcTransport::FromProto(*req));
  resp->set_term(out.term);
  resp->set_success(out.success);
  resp->set_match_index(out.match_index);
  resp->set_conflict_index(out.conflict_index);
  return grpc::Status::OK;
}

}  // namespace kv
