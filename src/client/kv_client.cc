#include "client/kv_client.h"

#include <algorithm>
#include <thread>

namespace kv {

KvClient::KvClient(std::vector<std::string> addresses, int max_attempts)
    : addresses_(std::move(addresses)), max_attempts_(max_attempts) {}

kvstore::KeyValue::Stub* KvClient::StubFor(const std::string& address) {
  std::lock_guard lock(mu_);
  auto& stub = stubs_[address];
  if (!stub) stub = kvstore::KeyValue::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  return stub.get();
}

Status KvClient::ClusterStatus(const std::string& address, kvstore::StatusResponse* out) {
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(2));
  kvstore::StatusRequest req;
  grpc::Status s = StubFor(address)->ClusterStatus(&ctx, req, out);
  return s.ok() ? Status::Ok() : Status::IOError(address + ": " + s.error_message());
}

Status KvClient::Connect() {
  Status last = Status::IOError("no addresses");
  for (int round = 0; round < 10; ++round) {
    for (const auto& addr : addresses_) {
      kvstore::StatusResponse resp;
      last = ClusterStatus(addr, &resp);
      if (last.ok() && resp.shard_count() > 0) {
        shard_count_ = resp.shard_count();
        for (uint32_t s = 0; s < shard_count_; ++s) ring_.AddShard(s);
        return Status::Ok();
      }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
  }
  return last;
}

template <typename Call>
Status KvClient::WithLeader(const std::string& key, Call call) {
  if (shard_count_ == 0) return Status::InvalidArgument("call Connect() first");
  uint32_t shard = ring_.ShardFor(key);
  std::string target;
  {
    std::lock_guard lock(mu_);
    auto it = leader_cache_.find(shard);
    target = it != leader_cache_.end() ? it->second : addresses_[shard % addresses_.size()];
  }

  Status last = Status::Timeout("no leader found");
  for (int attempt = 0; attempt < max_attempts_; ++attempt) {
    kvstore::Code code;
    std::string leader_hint;
    std::string error;
    grpc::Status rpc = call(StubFor(target), &code, &leader_hint, &error);

    if (rpc.ok() && code != kvstore::NOT_LEADER && code != kvstore::TIMEOUT) {
      std::lock_guard lock(mu_);
      leader_cache_[shard] = target;
      switch (code) {
        case kvstore::OK: return Status::Ok();
        case kvstore::NOT_FOUND: return Status::NotFound();
        case kvstore::INVALID_ARGUMENT: return Status::InvalidArgument(error);
        default: return Status::IOError(error);
      }
    }

    if (rpc.ok() && code == kvstore::NOT_LEADER && !leader_hint.empty()) {
      target = leader_hint;  // follow the redirect straight away
      last = Status::NotLeader(leader_hint);
      continue;
    }
    // Node down, election in progress, or request timed out: try the next node after a pause.
    last = rpc.ok() ? (code == kvstore::TIMEOUT ? Status::Timeout(error) : Status::NotLeader())
                    : Status::IOError(target + ": " + rpc.error_message());
    auto pos = std::find(addresses_.begin(), addresses_.end(), target);
    size_t next = pos == addresses_.end() ? 0 : (static_cast<size_t>(pos - addresses_.begin()) + 1) % addresses_.size();
    target = addresses_[next];
    std::this_thread::sleep_for(std::chrono::milliseconds(std::min(50 * (attempt + 1), 500)));
  }
  return last;
}

Status KvClient::Put(const std::string& key, const std::string& value) {
  return WithLeader(key, [&](kvstore::KeyValue::Stub* stub, kvstore::Code* code, std::string* hint, std::string* err) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    kvstore::PutRequest req;
    req.set_key(key);
    req.set_value(value);
    kvstore::WriteResponse resp;
    grpc::Status s = stub->Put(&ctx, req, &resp);
    *code = resp.code();
    *hint = resp.leader_address();
    *err = resp.error();
    return s;
  });
}

Status KvClient::Delete(const std::string& key) {
  return WithLeader(key, [&](kvstore::KeyValue::Stub* stub, kvstore::Code* code, std::string* hint, std::string* err) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    kvstore::DeleteRequest req;
    req.set_key(key);
    kvstore::WriteResponse resp;
    grpc::Status s = stub->Delete(&ctx, req, &resp);
    *code = resp.code();
    *hint = resp.leader_address();
    *err = resp.error();
    return s;
  });
}

Status KvClient::Get(const std::string& key, std::string* value) {
  return WithLeader(key, [&](kvstore::KeyValue::Stub* stub, kvstore::Code* code, std::string* hint, std::string* err) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    kvstore::GetRequest req;
    req.set_key(key);
    kvstore::GetResponse resp;
    grpc::Status s = stub->Get(&ctx, req, &resp);
    *code = resp.code();
    *hint = resp.leader_address();
    *err = resp.error();
    if (s.ok() && resp.code() == kvstore::OK) *value = resp.value();
    return s;
  });
}

}  // namespace kv
