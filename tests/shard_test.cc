#include <gtest/gtest.h>

#include <map>

#include "shard/hash_ring.h"
#include "shard/shard_group.h"
#include "sim_network.h"
#include "test_util.h"

namespace kv {

using test::Eventually;

// Three nodes, four shards. Every node hosts one replica of every shard, so
// each shard is its own three-member Raft group with its own LSM tree.
class ShardedCluster {
 public:
  static constexpr int kNodes = 3;
  static constexpr uint32_t kShards = 4;

  ShardedCluster() {
    for (uint32_t s = 0; s < kShards; ++s) ring_.AddShard(s);
    for (int n = 0; n < kNodes; ++n) {
      transports_.push_back(std::make_unique<test::SimTransport>(&net_, NodeId(n)));
      replicas_.emplace_back(kShards);
      for (uint32_t s = 0; s < kShards; ++s) Start(n, s);
    }
  }

  ~ShardedCluster() {
    for (int n = 0; n < kNodes; ++n) StopNode(n);
  }

  static std::string NodeId(int n) { return "node" + std::to_string(n); }

  void Start(int n, uint32_t shard) {
    ShardReplica::Options o;
    o.raft.id = NodeId(n);
    o.raft.group = shard;
    for (int p = 0; p < kNodes; ++p) {
      if (p != n) o.raft.peers.push_back(NodeId(p));
    }
    std::string base = dir_.sub(NodeId(n) + "-shard" + std::to_string(shard));
    o.raft.data_dir = base + "/raft";
    o.raft.election_timeout_min = std::chrono::milliseconds(150);
    o.raft.election_timeout_max = std::chrono::milliseconds(300);
    o.raft.heartbeat_interval = std::chrono::milliseconds(30);
    o.raft.sync_log = false;
    o.storage.dir = base + "/data";
    o.storage.memtable_bytes = 32 * 1024;
    o.request_timeout = std::chrono::milliseconds(1500);
    auto& slot = replicas_[n][shard];
    slot = std::make_unique<ShardReplica>(o, transports_[n].get());
    Status started = slot->Start();
    ASSERT_TRUE(started.ok()) << started.ToString();
    net_.Register(NodeId(n), shard, &slot->raft());
  }

  void StopNode(int n) {
    for (uint32_t s = 0; s < kShards; ++s) {
      if (!replicas_[n][s]) continue;
      net_.Unregister(NodeId(n), s);
      replicas_[n][s]->Stop();
      replicas_[n][s].reset();
    }
  }

  void RestartNode(int n) {
    StopNode(n);
    for (uint32_t s = 0; s < kShards; ++s) Start(n, s);
  }

  // What a client library does: hash the key to a shard, then find that shard's leader.
  ShardReplica* LeaderFor(const std::string& key) {
    uint32_t shard = ring_.ShardFor(key);
    for (int n = 0; n < kNodes; ++n) {
      auto& r = replicas_[n][shard];
      if (r && r->raft().IsLeader()) return r.get();
    }
    return nullptr;
  }

  Status Put(const std::string& key, const std::string& value) {
    return Retry([&](ShardReplica* r) { return r->Put(key, value); }, key);
  }
  Status Delete(const std::string& key) {
    return Retry([&](ShardReplica* r) { return r->Delete(key); }, key);
  }
  Status Get(const std::string& key, std::string* value) {
    return Retry([&](ShardReplica* r) { return r->Get(key, value); }, key);
  }

  ShardReplica* replica(int n, uint32_t shard) { return replicas_[n][shard].get(); }
  const HashRing& ring() const { return ring_; }

 private:
  template <typename Op>
  Status Retry(Op op, const std::string& key) {
    Status last = Status::Timeout();
    for (int attempt = 0; attempt < 200; ++attempt) {
      if (ShardReplica* r = LeaderFor(key)) {
        last = op(r);
        if (last.ok() || last.IsNotFound()) return last;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return last;
  }

  test::TempDir dir_;
  test::SimNetwork net_;
  HashRing ring_;
  std::vector<std::unique_ptr<test::SimTransport>> transports_;
  std::vector<std::vector<std::unique_ptr<ShardReplica>>> replicas_;
};

TEST(ShardTest, KeysAreSpreadAcrossShardsAndReadBack) {
  ShardedCluster c;
  std::map<uint32_t, int> per_shard;
  for (int i = 0; i < 400; ++i) {
    std::string key = "user:" + std::to_string(i);
    ASSERT_TRUE(c.Put(key, "v" + std::to_string(i)).ok()) << key;
    per_shard[c.ring().ShardFor(key)]++;
  }
  EXPECT_EQ(per_shard.size(), ShardedCluster::kShards);
  for (const auto& [shard, n] : per_shard) EXPECT_GT(n, 50) << "shard " << shard;

  for (int i = 0; i < 400; i += 7) {
    std::string v;
    ASSERT_TRUE(c.Get("user:" + std::to_string(i), &v).ok());
    EXPECT_EQ(v, "v" + std::to_string(i));
  }
  ASSERT_TRUE(c.Delete("user:7").ok());
  std::string v;
  EXPECT_TRUE(c.Get("user:7", &v).IsNotFound());
}

TEST(ShardTest, EveryReplicaOfAShardStoresTheSameData) {
  ShardedCluster c;
  for (int i = 0; i < 200; ++i) ASSERT_TRUE(c.Put("k" + std::to_string(i), std::to_string(i * i)).ok());
  // Followers apply asynchronously; check their storage directly once caught up.
  for (int i = 0; i < 200; i += 13) {
    std::string key = "k" + std::to_string(i);
    uint32_t shard = c.ring().ShardFor(key);
    for (int n = 0; n < ShardedCluster::kNodes; ++n) {
      ASSERT_TRUE(Eventually([&] {
        std::string v;
        return c.replica(n, shard)->storage().Get(key, &v).ok() && v == std::to_string(i * i);
      })) << key << " on node" << n;
    }
  }
}

TEST(ShardTest, SurvivesANodeCrashAndTheNodeCatchesUpOnRestart) {
  ShardedCluster c;
  for (int i = 0; i < 100; ++i) ASSERT_TRUE(c.Put("a" + std::to_string(i), "1").ok());

  c.StopNode(0);  // every shard loses one replica, possibly its leader
  for (int i = 0; i < 100; ++i) ASSERT_TRUE(c.Put("b" + std::to_string(i), "2").ok());
  std::string v;
  ASSERT_TRUE(c.Get("a42", &v).ok());

  c.RestartNode(0);
  for (int i = 0; i < 100; i += 9) {
    std::string key = "b" + std::to_string(i);
    uint32_t shard = c.ring().ShardFor(key);
    ASSERT_TRUE(Eventually([&] {
      std::string got;
      return c.replica(0, shard)->storage().Get(key, &got).ok() && got == "2";
    })) << key;
  }
}

}  // namespace kv
