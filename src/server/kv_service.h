#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "kv.grpc.pb.h"
#include "server/grpc_transport.h"
#include "shard/hash_ring.h"
#include "shard/shard_group.h"

namespace kv {

struct NodeConfig {
  std::string id;
  std::string listen_address;
  std::map<std::string, std::string> cluster;  // node id -> address, including this node
  uint32_t shards = 4;
  std::string data_dir;
  bool sync = true;
};

// A storage node: hosts one replica of every shard and serves both the
// client API and the Raft traffic for those shards on one gRPC port.
class KvNode final : public kvstore::KeyValue::Service, public kvstore::Raft::Service {
 public:
  explicit KvNode(NodeConfig config);
  ~KvNode() override;

  Status Start();
  void Wait();
  void Shutdown();

  grpc::Status Put(grpc::ServerContext*, const kvstore::PutRequest*, kvstore::WriteResponse*) override;
  grpc::Status Delete(grpc::ServerContext*, const kvstore::DeleteRequest*, kvstore::WriteResponse*) override;
  grpc::Status Get(grpc::ServerContext*, const kvstore::GetRequest*, kvstore::GetResponse*) override;
  grpc::Status ClusterStatus(grpc::ServerContext*, const kvstore::StatusRequest*, kvstore::StatusResponse*) override;
  grpc::Status RequestVote(grpc::ServerContext*, const kvstore::VoteRequest*, kvstore::VoteResponse*) override;
  grpc::Status AppendEntries(grpc::ServerContext*, const kvstore::AppendRequest*, kvstore::AppendResponse*) override;

 private:
  ShardReplica* ReplicaFor(const std::string& key);
  template <typename Resp>
  void Fill(const Status& s, ShardReplica* replica, Resp* resp);

  NodeConfig config_;
  HashRing ring_;
  std::unique_ptr<GrpcTransport> transport_;
  std::vector<std::unique_ptr<ShardReplica>> replicas_;
  std::unique_ptr<grpc::Server> server_;
};

}  // namespace kv
