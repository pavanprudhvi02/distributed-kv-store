#include "storage/sstable.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>

#include "common/coding.h"
#include "common/crc32.h"
#include "storage/bloom.h"
#include "storage/wal.h"

namespace kv {

namespace {

Status WriteAll(int fd, const std::string& data) {
  size_t done = 0;
  while (done < data.size()) {
    ssize_t n = ::write(fd, data.data() + done, data.size() - done);
    if (n < 0) {
      if (errno == EINTR) continue;
      return Status::IOError(std::string("write: ") + std::strerror(errno));
    }
    done += static_cast<size_t>(n);
  }
  return Status::Ok();
}

void EncodeRecord(std::string* dst, std::string_view key, const Entry& e) {
  dst->push_back(static_cast<char>(e.deleted ? RecordType::kDelete : RecordType::kPut));
  PutLengthPrefixed(dst, key);
  PutLengthPrefixed(dst, e.deleted ? std::string_view{} : std::string_view{e.value});
}

// Decodes one record from the front of `in`. Returns false if `in` is too short.
bool DecodeRecord(std::string_view* in, std::string_view* key, Entry* e) {
  if (in->empty()) return false;
  auto type = static_cast<RecordType>(in->front());
  std::string_view rest = in->substr(1);
  std::string_view value;
  if (!GetLengthPrefixed(&rest, key) || !GetLengthPrefixed(&rest, &value)) return false;
  e->deleted = type == RecordType::kDelete;
  e->value.assign(value.data(), value.size());
  *in = rest;
  return true;
}

}  // namespace

Status SSTable::Write(const std::string& path, const EntrySource& next, uint64_t* file_size) {
  std::string tmp = path + ".tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0) return Status::IOError("open " + tmp + ": " + std::strerror(errno));

  std::string block;
  std::string index;
  std::vector<std::string> keys;
  uint64_t offset = 0;
  uint64_t count = 0;
  uint32_t crc = 0;
  Status s;

  std::string key;
  Entry entry;
  while (next(&key, &entry)) {
    if (count % kIndexInterval == 0) {
      PutLengthPrefixed(&index, key);
      PutFixed64(&index, offset + block.size());
    }
    EncodeRecord(&block, key, entry);
    keys.push_back(key);
    ++count;
    if (block.size() >= 64 * 1024) {
      crc = Crc32(block.data(), block.size(), crc);
      if (!(s = WriteAll(fd, block)).ok()) break;
      offset += block.size();
      block.clear();
    }
  }

  if (s.ok()) {
    std::string tail = block;
    uint64_t index_offset = offset + block.size();
    std::string index_section;
    PutFixed64(&index_section, count);
    index_section += index;
    std::string bloom = BloomFilter::Build(keys);
    uint64_t bloom_offset = index_offset + index_section.size();
    tail += index_section;
    tail += bloom;
    crc = Crc32(tail.data(), tail.size(), crc);

    PutFixed64(&tail, index_offset);
    PutFixed64(&tail, index_section.size());
    PutFixed64(&tail, bloom_offset);
    PutFixed64(&tail, bloom.size());
    PutFixed32(&tail, crc);
    PutFixed32(&tail, kMagic);
    s = WriteAll(fd, tail);
    offset += tail.size();
  }
  if (s.ok() && ::fsync(fd) != 0) s = Status::IOError("fsync sstable");
  ::close(fd);
  if (s.ok() && std::rename(tmp.c_str(), path.c_str()) != 0) s = Status::IOError("rename sstable");
  if (!s.ok()) {
    ::unlink(tmp.c_str());
    return s;
  }
  if (file_size != nullptr) *file_size = offset;
  return Status::Ok();
}

Status SSTable::Write(const std::string& path, const std::map<std::string, Entry, std::less<>>& entries) {
  auto it = entries.begin();
  return Write(path, [&](std::string* key, Entry* entry) {
    if (it == entries.end()) return false;
    *key = it->first;
    *entry = it->second;
    ++it;
    return true;
  });
}

Status SSTable::Open(const std::string& path, std::shared_ptr<SSTable>* table) {
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd < 0) return Status::IOError("open " + path + ": " + std::strerror(errno));
  std::shared_ptr<SSTable> t(new SSTable());
  t->path_ = path;
  t->fd_ = fd;

  off_t size = ::lseek(fd, 0, SEEK_END);
  if (size < static_cast<off_t>(kFooterSize)) return Status::Corruption(path + ": file too small");
  std::string footer;
  if (!t->ReadAt(size - kFooterSize, kFooterSize, &footer)) return Status::IOError(path + ": read footer");
  if (DecodeFixed32(footer.data() + 36) != kMagic) return Status::Corruption(path + ": bad magic");

  uint64_t index_off = DecodeFixed64(footer.data());
  uint64_t index_size = DecodeFixed64(footer.data() + 8);
  uint64_t bloom_off = DecodeFixed64(footer.data() + 16);
  uint64_t bloom_size = DecodeFixed64(footer.data() + 24);
  uint32_t expected_crc = DecodeFixed32(footer.data() + 32);
  if (bloom_off + bloom_size + kFooterSize != static_cast<uint64_t>(size) || index_off + index_size != bloom_off) {
    return Status::Corruption(path + ": bad footer offsets");
  }

  // Verify the whole file once on open; afterwards reads trust the content.
  std::string body;
  if (!t->ReadAt(0, static_cast<size_t>(bloom_off + bloom_size), &body)) return Status::IOError(path + ": read");
  if (Crc32(body.data(), body.size()) != expected_crc) return Status::Corruption(path + ": checksum mismatch");

  std::string_view index(body.data() + index_off, index_size);
  if (index.size() < 8) return Status::Corruption(path + ": bad index");
  t->entry_count_ = DecodeFixed64(index.data());
  index.remove_prefix(8);
  while (!index.empty()) {
    std::string_view key;
    if (!GetLengthPrefixed(&index, &key) || index.size() < 8) return Status::Corruption(path + ": bad index entry");
    t->index_.emplace_back(std::string(key), DecodeFixed64(index.data()));
    index.remove_prefix(8);
  }
  t->bloom_.assign(body.data() + bloom_off, bloom_size);
  t->index_offset_ = index_off;
  *table = std::move(t);
  return Status::Ok();
}

SSTable::~SSTable() {
  if (fd_ >= 0) ::close(fd_);
}

bool SSTable::ReadAt(uint64_t offset, size_t n, std::string* out) const {
  out->resize(n);
  size_t done = 0;
  while (done < n) {
    ssize_t r = ::pread(fd_, out->data() + done, n - done, static_cast<off_t>(offset + done));
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) return false;
    done += static_cast<size_t>(r);
  }
  return true;
}

std::optional<Entry> SSTable::Get(std::string_view key) const {
  if (index_.empty() || !BloomFilter::MayContain(bloom_, key)) return std::nullopt;

  // Find the last index entry with key <= target; the record, if present,
  // lies between it and the next index entry.
  auto it = std::upper_bound(index_.begin(), index_.end(), key,
                             [](std::string_view k, const auto& e) { return k < e.first; });
  if (it == index_.begin()) return std::nullopt;
  --it;
  uint64_t start = it->second;
  uint64_t end = (it + 1 == index_.end()) ? index_offset_ : (it + 1)->second;

  std::string block;
  if (!ReadAt(start, static_cast<size_t>(end - start), &block)) return std::nullopt;
  std::string_view in(block);
  std::string_view k;
  Entry e;
  while (DecodeRecord(&in, &k, &e)) {
    if (k == key) return e;
    if (k > key) break;
  }
  return std::nullopt;
}

SSTable::Iterator::Iterator(const SSTable* table) : table_(table) { Next(); }

void SSTable::Iterator::Next() {
  valid_ = false;
  if (pos_ >= table_->index_offset_) return;
  for (int attempt = 0; attempt < 3; ++attempt) {
    std::string_view in;
    if (pos_ >= buffer_start_ && pos_ - buffer_start_ <= buffer_.size()) {
      in = std::string_view(buffer_);
      in.remove_prefix(static_cast<size_t>(pos_ - buffer_start_));
    }
    size_t before = in.size();
    std::string_view k;
    if (DecodeRecord(&in, &k, &entry_)) {
      key_.assign(k.data(), k.size());
      pos_ += before - in.size();
      valid_ = true;
      return;
    }
    if (attempt == 2) return;
    // Refill from the current record. A record larger than the normal chunk
    // gets a second refill covering the rest of the data section.
    uint64_t remaining = table_->index_offset_ - pos_;
    size_t n = static_cast<size_t>(attempt == 0 ? std::min<uint64_t>(256 * 1024, remaining) : remaining);
    if (!table_->ReadAt(pos_, n, &buffer_)) return;
    buffer_start_ = pos_;
  }
}

}  // namespace kv
