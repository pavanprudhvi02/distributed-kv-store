#include "storage/wal.h"

#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <iterator>

#include "common/coding.h"
#include "common/crc32.h"

namespace kv {

WriteAheadLog::~WriteAheadLog() { Close(); }

Status WriteAheadLog::Open(const std::string& path) {
  Close();
  file_ = std::fopen(path.c_str(), "ab");
  if (file_ == nullptr) return Status::IOError("open " + path + ": " + std::strerror(errno));
  path_ = path;
  return Status::Ok();
}

Status WriteAheadLog::Append(RecordType type, std::string_view key, std::string_view value, bool sync) {
  if (file_ == nullptr) return Status::IOError("wal not open");
  std::string payload;
  payload.push_back(static_cast<char>(type));
  PutLengthPrefixed(&payload, key);
  PutLengthPrefixed(&payload, value);

  std::string record;
  PutFixed32(&record, Crc32(payload.data(), payload.size()));
  PutFixed32(&record, static_cast<uint32_t>(payload.size()));
  record += payload;

  if (std::fwrite(record.data(), 1, record.size(), file_) != record.size()) {
    return Status::IOError("wal write failed");
  }
  if (std::fflush(file_) != 0) return Status::IOError("wal flush failed");
  return sync ? Sync() : Status::Ok();
}

Status WriteAheadLog::Sync() {
  if (file_ == nullptr) return Status::IOError("wal not open");
  if (::fsync(::fileno(file_)) != 0) return Status::IOError("wal fsync failed");
  return Status::Ok();
}

void WriteAheadLog::Close() {
  if (file_ != nullptr) {
    std::fclose(file_);
    file_ = nullptr;
  }
}

Status WriteAheadLog::Replay(const std::string& path, const Visitor& visit, size_t* valid_records) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return Status::NotFound(path);
  std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());

  size_t pos = 0;
  size_t count = 0;
  while (pos + 8 <= data.size()) {
    uint32_t crc = DecodeFixed32(data.data() + pos);
    uint32_t len = DecodeFixed32(data.data() + pos + 4);
    if (pos + 8 + len > data.size()) break;  // torn tail
    std::string_view payload(data.data() + pos + 8, len);
    if (Crc32(payload.data(), payload.size()) != crc) break;  // corrupt tail

    if (payload.empty()) break;
    auto type = static_cast<RecordType>(payload.front());
    payload.remove_prefix(1);
    std::string_view key, value;
    if (!GetLengthPrefixed(&payload, &key) || !GetLengthPrefixed(&payload, &value)) break;
    visit(type, key, value);
    ++count;
    pos += 8 + len;
  }
  if (valid_records != nullptr) *valid_records = count;
  return Status::Ok();
}

}  // namespace kv
