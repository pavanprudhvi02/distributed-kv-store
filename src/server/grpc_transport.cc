#include "server/grpc_transport.h"

namespace kv {

GrpcTransport::GrpcTransport(std::map<std::string, std::string> peer_addresses, std::chrono::milliseconds rpc_timeout)
    : addresses_(std::move(peer_addresses)), rpc_timeout_(rpc_timeout) {}

GrpcTransport::~GrpcTransport() { callbacks_.Shutdown(); }

kvstore::Raft::Stub* GrpcTransport::StubFor(const std::string& peer) {
  std::lock_guard lock(mu_);
  auto it = stubs_.find(peer);
  if (it != stubs_.end()) return it->second.get();
  auto addr = addresses_.find(peer);
  if (addr == addresses_.end()) return nullptr;
  grpc::ChannelArguments args;
  // Reconnect quickly after a peer restarts instead of backing off for seconds.
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
  auto channel = grpc::CreateCustomChannel(addr->second, grpc::InsecureChannelCredentials(), args);
  auto [pos, _] = stubs_.emplace(peer, kvstore::Raft::NewStub(channel));
  return pos->second.get();
}

void GrpcTransport::ToProto(const raft::AppendEntriesRequest& in, kvstore::AppendRequest* out) {
  out->set_group(in.group);
  out->set_term(in.term);
  out->set_leader_id(in.leader_id);
  out->set_prev_log_index(in.prev_log_index);
  out->set_prev_log_term(in.prev_log_term);
  out->set_leader_commit(in.leader_commit);
  for (const auto& e : in.entries) {
    auto* pe = out->add_entries();
    pe->set_term(e.term);
    pe->set_index(e.index);
    pe->set_command(e.command);
  }
}

raft::AppendEntriesRequest GrpcTransport::FromProto(const kvstore::AppendRequest& in) {
  raft::AppendEntriesRequest out;
  out.group = in.group();
  out.term = in.term();
  out.leader_id = in.leader_id();
  out.prev_log_index = in.prev_log_index();
  out.prev_log_term = in.prev_log_term();
  out.leader_commit = in.leader_commit();
  out.entries.reserve(in.entries_size());
  for (const auto& e : in.entries()) out.entries.push_back({e.term(), e.index(), e.command()});
  return out;
}

void GrpcTransport::RequestVote(const std::string& peer, const raft::RequestVoteRequest& req, VoteCallback cb) {
  auto* stub = StubFor(peer);
  if (stub == nullptr) {
    callbacks_.Submit([cb] { cb(false, {}); });
    return;
  }
  struct Call {
    grpc::ClientContext ctx;
    kvstore::VoteRequest req;
    kvstore::VoteResponse resp;
  };
  auto call = std::make_shared<Call>();
  call->ctx.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);
  call->req.set_group(req.group);
  call->req.set_term(req.term);
  call->req.set_candidate_id(req.candidate_id);
  call->req.set_last_log_index(req.last_log_index);
  call->req.set_last_log_term(req.last_log_term);
  stub->async()->RequestVote(&call->ctx, &call->req, &call->resp, [this, call, cb](grpc::Status status) {
    callbacks_.Submit([call, cb, ok = status.ok()] {
      raft::RequestVoteResponse r;
      r.term = call->resp.term();
      r.vote_granted = call->resp.vote_granted();
      cb(ok, r);
    });
  });
}

void GrpcTransport::AppendEntries(const std::string& peer, const raft::AppendEntriesRequest& req, AppendCallback cb) {
  auto* stub = StubFor(peer);
  if (stub == nullptr) {
    callbacks_.Submit([cb] { cb(false, {}); });
    return;
  }
  struct Call {
    grpc::ClientContext ctx;
    kvstore::AppendRequest req;
    kvstore::AppendResponse resp;
  };
  auto call = std::make_shared<Call>();
  call->ctx.set_deadline(std::chrono::system_clock::now() + rpc_timeout_);
  ToProto(req, &call->req);
  stub->async()->AppendEntries(&call->ctx, &call->req, &call->resp, [this, call, cb](grpc::Status status) {
    callbacks_.Submit([call, cb, ok = status.ok()] {
      raft::AppendEntriesResponse r;
      r.term = call->resp.term();
      r.success = call->resp.success();
      r.match_index = call->resp.match_index();
      r.conflict_index = call->resp.conflict_index();
      cb(ok, r);
    });
  });
}

}  // namespace kv
