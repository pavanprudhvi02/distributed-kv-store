#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace kv {

// Little-endian fixed-width and LEB128 varint helpers used by the WAL,
// SSTable and Raft log formats.

inline void PutFixed32(std::string* dst, uint32_t v) {
  for (int i = 0; i < 4; ++i) dst->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

inline void PutFixed64(std::string* dst, uint64_t v) {
  for (int i = 0; i < 8; ++i) dst->push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

inline uint32_t DecodeFixed32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

inline uint64_t DecodeFixed64(const char* p) {
  uint64_t v = 0;
  for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

inline void PutVarint64(std::string* dst, uint64_t v) {
  while (v >= 0x80) {
    dst->push_back(static_cast<char>(v | 0x80));
    v >>= 7;
  }
  dst->push_back(static_cast<char>(v));
}

// Reads a varint from the front of `in` and advances it. Returns false on truncation.
inline bool GetVarint64(std::string_view* in, uint64_t* v) {
  uint64_t result = 0;
  for (int shift = 0; shift <= 63 && !in->empty(); shift += 7) {
    auto byte = static_cast<unsigned char>(in->front());
    in->remove_prefix(1);
    result |= static_cast<uint64_t>(byte & 0x7f) << shift;
    if ((byte & 0x80) == 0) {
      *v = result;
      return true;
    }
  }
  return false;
}

inline void PutLengthPrefixed(std::string* dst, std::string_view s) {
  PutVarint64(dst, s.size());
  dst->append(s.data(), s.size());
}

inline bool GetLengthPrefixed(std::string_view* in, std::string_view* out) {
  uint64_t len = 0;
  if (!GetVarint64(in, &len) || in->size() < len) return false;
  *out = in->substr(0, len);
  in->remove_prefix(len);
  return true;
}

}  // namespace kv
