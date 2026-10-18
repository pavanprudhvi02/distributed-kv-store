#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <string>

#include <grpcpp/grpcpp.h>

#include "common/thread_pool.h"
#include "kv.grpc.pb.h"
#include "raft/types.h"

namespace kv {

// Raft transport over gRPC. One channel per peer, created lazily and shared
// by every shard's Raft group. RPCs use the async callback API with a
// deadline, and results are handed back on a worker pool so Raft callbacks
// never run inline on the calling thread.
class GrpcTransport : public raft::Transport {
 public:
  explicit GrpcTransport(std::map<std::string, std::string> peer_addresses,
                         std::chrono::milliseconds rpc_timeout = std::chrono::milliseconds(500));
  ~GrpcTransport() override;

  void RequestVote(const std::string& peer, const raft::RequestVoteRequest& req, VoteCallback cb) override;
  void AppendEntries(const std::string& peer, const raft::AppendEntriesRequest& req, AppendCallback cb) override;

  // Conversions shared with the server side.
  static void ToProto(const raft::AppendEntriesRequest& in, kvstore::AppendRequest* out);
  static raft::AppendEntriesRequest FromProto(const kvstore::AppendRequest& in);

 private:
  kvstore::Raft::Stub* StubFor(const std::string& peer);

  std::map<std::string, std::string> addresses_;
  std::chrono::milliseconds rpc_timeout_;
  std::mutex mu_;
  std::map<std::string, std::unique_ptr<kvstore::Raft::Stub>> stubs_;
  ThreadPool callbacks_{2};
};

}  // namespace kv
