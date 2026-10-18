#include "storage/memtable.h"

#include <mutex>

namespace kv {

namespace {
constexpr size_t kEntryOverhead = 32;  // rough per-node cost of std::map
}

void MemTable::Put(std::string_view key, std::string_view value) {
  std::unique_lock lock(mu_);
  auto it = table_.find(key);
  if (it == table_.end()) {
    table_.emplace(std::string(key), Entry{false, std::string(value)});
    bytes_ += key.size() + value.size() + kEntryOverhead;
  } else {
    bytes_ = bytes_ - it->second.value.size() + value.size();
    it->second = Entry{false, std::string(value)};
  }
}

void MemTable::Delete(std::string_view key) {
  std::unique_lock lock(mu_);
  auto it = table_.find(key);
  if (it == table_.end()) {
    table_.emplace(std::string(key), Entry{true, {}});
    bytes_ += key.size() + kEntryOverhead;
  } else {
    bytes_ -= it->second.value.size();
    it->second = Entry{true, {}};
  }
}

std::optional<Entry> MemTable::Get(std::string_view key) const {
  std::shared_lock lock(mu_);
  auto it = table_.find(key);
  if (it == table_.end()) return std::nullopt;
  return it->second;
}

size_t MemTable::ApproximateBytes() const {
  std::shared_lock lock(mu_);
  return bytes_;
}

size_t MemTable::size() const {
  std::shared_lock lock(mu_);
  return table_.size();
}

std::map<std::string, Entry, std::less<>> MemTable::Snapshot() const {
  std::shared_lock lock(mu_);
  return table_;
}

}  // namespace kv
