#pragma once

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/status.h"
#include "kv.grpc.pb.h"
#include "shard/hash_ring.h"

namespace kv {

// Client library. Hashes each key to its shard with the same consistent-hash
// ring the servers use, remembers which node led that shard last time, and
// follows NOT_LEADER redirects. Nodes that are down are skipped.
class KvClient {
 public:
  explicit KvClient(std::vector<std::string> addresses, int max_attempts = 30);

  // Learns the shard count from any reachable node.
  Status Connect();

  Status Put(const std::string& key, const std::string& value);
  Status Delete(const std::string& key);
  Status Get(const std::string& key, std::string* value);
  Status ClusterStatus(const std::string& address, kvstore::StatusResponse* out);

  uint32_t shard_count() const { return shard_count_; }
  uint32_t ShardFor(const std::string& key) const { return ring_.ShardFor(key); }

 private:
  kvstore::KeyValue::Stub* StubFor(const std::string& address);
  template <typename Call>
  Status WithLeader(const std::string& key, Call call);

  std::vector<std::string> addresses_;
  int max_attempts_;
  uint32_t shard_count_ = 0;
  HashRing ring_;
  std::mutex mu_;
  std::map<std::string, std::unique_ptr<kvstore::KeyValue::Stub>> stubs_;
  std::map<uint32_t, std::string> leader_cache_;
};

}  // namespace kv
