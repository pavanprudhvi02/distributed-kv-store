#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

// Consistent hashing with virtual nodes. Each shard owns many points on a
// 64-bit ring; a key belongs to the first point clockwise from its hash.
// Adding or removing a shard only moves the keys next to that shard's
// points (about 1/N of them), instead of reshuffling everything as
// `hash % N` would.
class HashRing {
 public:
  explicit HashRing(int virtual_nodes = 128) : virtual_nodes_(virtual_nodes) {}

  void AddShard(uint32_t shard);
  void RemoveShard(uint32_t shard);
  uint32_t ShardFor(std::string_view key) const;  // requires at least one shard
  std::vector<uint32_t> Shards() const;
  bool empty() const { return ring_.empty(); }

  static uint64_t Hash(std::string_view data);

 private:
  int virtual_nodes_;
  std::map<uint64_t, uint32_t> ring_;
};

}  // namespace kv
