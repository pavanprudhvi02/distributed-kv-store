#include "raft/command.h"

#include "common/coding.h"

namespace kv {

std::string Command::Encode() const {
  std::string out;
  out.push_back(static_cast<char>(op));
  PutLengthPrefixed(&out, key);
  PutLengthPrefixed(&out, value);
  return out;
}

std::optional<Command> Command::Decode(std::string_view data) {
  if (data.empty()) return std::nullopt;
  Command c;
  auto op = static_cast<uint8_t>(data.front());
  if (op != static_cast<uint8_t>(Op::kPut) && op != static_cast<uint8_t>(Op::kDelete)) return std::nullopt;
  c.op = static_cast<Op>(op);
  data.remove_prefix(1);
  std::string_view k, v;
  if (!GetLengthPrefixed(&data, &k) || !GetLengthPrefixed(&data, &v)) return std::nullopt;
  c.key.assign(k);
  c.value.assign(v);
  return c;
}

}  // namespace kv
