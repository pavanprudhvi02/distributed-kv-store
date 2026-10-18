#include "shard/hash_ring.h"

#include <set>
#include <stdexcept>

namespace kv {

uint64_t HashRing::Hash(std::string_view data) {
  // FNV-1a with a final avalanche step (splitmix64) for an even spread on the ring.
  uint64_t h = 1469598103934665603ULL;
  for (unsigned char c : data) {
    h ^= c;
    h *= 1099511628211ULL;
  }
  h += 0x9e3779b97f4a7c15ULL;
  h = (h ^ (h >> 30)) * 0xbf58476d1ce4e5b9ULL;
  h = (h ^ (h >> 27)) * 0x94d049bb133111ebULL;
  return h ^ (h >> 31);
}

void HashRing::AddShard(uint32_t shard) {
  for (int v = 0; v < virtual_nodes_; ++v) {
    ring_[Hash("shard-" + std::to_string(shard) + "#" + std::to_string(v))] = shard;
  }
}

void HashRing::RemoveShard(uint32_t shard) {
  for (auto it = ring_.begin(); it != ring_.end();) {
    it = (it->second == shard) ? ring_.erase(it) : std::next(it);
  }
}

uint32_t HashRing::ShardFor(std::string_view key) const {
  if (ring_.empty()) throw std::logic_error("hash ring has no shards");
  auto it = ring_.lower_bound(Hash(key));
  if (it == ring_.end()) it = ring_.begin();  // wrap around
  return it->second;
}

std::vector<uint32_t> HashRing::Shards() const {
  std::set<uint32_t> s;
  for (const auto& [_, shard] : ring_) s.insert(shard);
  return {s.begin(), s.end()};
}

}  // namespace kv
