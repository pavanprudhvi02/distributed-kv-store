#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace kv {

// CRC-32 (IEEE 802.3), used to detect torn or corrupted records on disk.
inline uint32_t Crc32(const void* data, size_t n, uint32_t crc = 0) {
  static const std::array<uint32_t, 256> table = [] {
    std::array<uint32_t, 256> t{};
    for (uint32_t i = 0; i < 256; ++i) {
      uint32_t c = i;
      for (int k = 0; k < 8; ++k) c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : c >> 1;
      t[i] = c;
    }
    return t;
  }();
  const auto* p = static_cast<const unsigned char*>(data);
  crc = ~crc;
  for (size_t i = 0; i < n; ++i) crc = table[(crc ^ p[i]) & 0xff] ^ (crc >> 8);
  return ~crc;
}

}  // namespace kv
