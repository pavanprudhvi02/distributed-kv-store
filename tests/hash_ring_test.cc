#include <gtest/gtest.h>

#include <map>

#include "shard/hash_ring.h"

namespace kv {

TEST(HashRingTest, SameKeyAlwaysMapsToSameShard) {
  HashRing a, b;
  for (uint32_t s = 0; s < 4; ++s) {
    a.AddShard(s);
    b.AddShard(3 - s);  // insertion order must not matter
  }
  for (int i = 0; i < 1000; ++i) {
    std::string key = "user:" + std::to_string(i);
    EXPECT_EQ(a.ShardFor(key), b.ShardFor(key));
  }
}

TEST(HashRingTest, SpreadsKeysEvenly) {
  HashRing ring(256);
  const int shards = 8, keys = 100000;
  for (uint32_t s = 0; s < shards; ++s) ring.AddShard(s);
  std::map<uint32_t, int> counts;
  for (int i = 0; i < keys; ++i) counts[ring.ShardFor("key-" + std::to_string(i))]++;
  ASSERT_EQ(counts.size(), static_cast<size_t>(shards));
  for (const auto& [shard, n] : counts) {
    EXPECT_NEAR(n, keys / shards, keys / shards * 0.2) << "shard " << shard;
  }
}

TEST(HashRingTest, AddingAShardOnlyMovesKeysToTheNewShard) {
  HashRing ring;
  for (uint32_t s = 0; s < 4; ++s) ring.AddShard(s);
  const int keys = 20000;
  std::vector<uint32_t> before(keys);
  for (int i = 0; i < keys; ++i) before[i] = ring.ShardFor("k" + std::to_string(i));

  ring.AddShard(4);
  int moved = 0;
  for (int i = 0; i < keys; ++i) {
    uint32_t now = ring.ShardFor("k" + std::to_string(i));
    if (now != before[i]) {
      ++moved;
      EXPECT_EQ(now, 4u) << "a key moved between two existing shards";
    }
  }
  // Ideal is 1/5 of the keys.
  EXPECT_NEAR(moved, keys / 5, keys * 0.05);
}

TEST(HashRingTest, RemovingAShardHandsItsKeysToTheOthers) {
  HashRing ring;
  for (uint32_t s = 0; s < 3; ++s) ring.AddShard(s);
  ring.RemoveShard(1);
  EXPECT_EQ(ring.Shards(), (std::vector<uint32_t>{0, 2}));
  for (int i = 0; i < 1000; ++i) EXPECT_NE(ring.ShardFor(std::to_string(i)), 1u);
}

}  // namespace kv
