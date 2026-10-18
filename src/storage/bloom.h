#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace kv {

// Bloom filter stored alongside each SSTable so most lookups for absent
// keys skip the table without touching its data blocks.
class BloomFilter {
 public:
  // Builds a filter for `keys` using roughly `bits_per_key` bits each.
  static std::string Build(const std::vector<std::string>& keys, int bits_per_key = 10);

  // `filter` is the serialized form produced by Build.
  static bool MayContain(std::string_view filter, std::string_view key);

 private:
  static uint64_t Hash(std::string_view key);
};

}  // namespace kv
