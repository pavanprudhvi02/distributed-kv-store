#include "storage/bloom.h"

#include <algorithm>
#include <cmath>

namespace kv {

uint64_t BloomFilter::Hash(std::string_view key) {
  // 64-bit FNV-1a followed by a murmur-style finalizer for better bit spread.
  uint64_t h = 1469598103934665603ULL;
  for (unsigned char c : key) {
    h ^= c;
    h *= 1099511628211ULL;
  }
  h ^= h >> 33;
  h *= 0xff51afd7ed558ccdULL;
  h ^= h >> 33;
  return h;
}

std::string BloomFilter::Build(const std::vector<std::string>& keys, int bits_per_key) {
  // k = ln(2) * bits/key minimises the false positive rate.
  int k = std::clamp(static_cast<int>(std::round(bits_per_key * 0.69)), 1, 30);
  size_t bits = std::max<size_t>(64, keys.size() * static_cast<size_t>(bits_per_key));
  size_t bytes = (bits + 7) / 8;
  bits = bytes * 8;

  std::string filter(bytes, '\0');
  for (const auto& key : keys) {
    uint64_t h = Hash(key);
    uint64_t delta = (h >> 17) | (h << 47);  // double hashing: h_i = h + i * delta
    for (int i = 0; i < k; ++i) {
      size_t bit = h % bits;
      filter[bit / 8] = static_cast<char>(filter[bit / 8] | (1 << (bit % 8)));
      h += delta;
    }
  }
  filter.push_back(static_cast<char>(k));
  return filter;
}

bool BloomFilter::MayContain(std::string_view filter, std::string_view key) {
  if (filter.size() < 2) return true;
  int k = static_cast<unsigned char>(filter.back());
  size_t bits = (filter.size() - 1) * 8;
  uint64_t h = Hash(key);
  uint64_t delta = (h >> 17) | (h << 47);
  for (int i = 0; i < k; ++i) {
    size_t bit = h % bits;
    if ((filter[bit / 8] & (1 << (bit % 8))) == 0) return false;
    h += delta;
  }
  return true;
}

}  // namespace kv
