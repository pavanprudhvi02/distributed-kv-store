#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace kv {

// A key-value mutation as stored in the Raft log.
struct Command {
  enum class Op : uint8_t { kPut = 1, kDelete = 2 };
  Op op = Op::kPut;
  std::string key;
  std::string value;

  std::string Encode() const;
  static std::optional<Command> Decode(std::string_view data);

  static Command Put(std::string key, std::string value) { return {Op::kPut, std::move(key), std::move(value)}; }
  static Command Delete(std::string key) { return {Op::kDelete, std::move(key), {}}; }
};

}  // namespace kv
