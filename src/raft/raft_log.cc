#include "raft/raft_log.h"

#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iterator>

#include "common/coding.h"
#include "common/crc32.h"

namespace kv::raft {

namespace fs = std::filesystem;

RaftLog::~RaftLog() {
  if (log_file_ != nullptr) std::fclose(log_file_);
}

Status RaftLog::Open(const std::string& dir, bool sync) {
  dir_ = dir;
  sync_ = sync;
  if (dir_.empty()) return Status::Ok();
  std::error_code ec;
  fs::create_directories(dir_, ec);
  if (ec) return Status::IOError("create " + dir_ + ": " + ec.message());

  // Metadata: "<term>\n<votedFor>\n"
  {
    std::ifstream meta(dir_ + "/raft.meta");
    if (meta) {
      meta >> current_term_;
      meta.ignore();
      std::getline(meta, voted_for_);
    }
  }

  // Log records: [crc32][len][term varint][index varint][command length-prefixed]
  std::string path = dir_ + "/raft.log";
  size_t valid_bytes = 0;
  {
    std::ifstream in(path, std::ios::binary);
    std::string data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    size_t pos = 0;
    while (pos + 8 <= data.size()) {
      uint32_t crc = DecodeFixed32(data.data() + pos);
      uint32_t len = DecodeFixed32(data.data() + pos + 4);
      if (pos + 8 + len > data.size()) break;
      std::string_view payload(data.data() + pos + 8, len);
      if (Crc32(payload.data(), payload.size()) != crc) break;
      LogEntry e;
      std::string_view cmd;
      if (!GetVarint64(&payload, &e.term) || !GetVarint64(&payload, &e.index) || !GetLengthPrefixed(&payload, &cmd)) {
        break;
      }
      if (e.index != entries_.size()) return Status::Corruption("raft log index gap: expected " + std::to_string(entries_.size()) + ", found " +
                                  std::to_string(e.index) + " at offset " + std::to_string(pos));
      e.command.assign(cmd.data(), cmd.size());
      entries_.push_back(std::move(e));
      pos += 8 + len;
      valid_bytes = pos;
    }
  }
  // Drop a torn tail so new appends start on a record boundary.
  if (fs::exists(path) && fs::file_size(path) != valid_bytes) fs::resize_file(path, valid_bytes);

  log_file_ = std::fopen(path.c_str(), "ab");
  if (log_file_ == nullptr) return Status::IOError("open " + path);
  return Status::Ok();
}

Status RaftLog::SetTermAndVote(uint64_t term, const std::string& voted_for) {
  if (term == current_term_ && voted_for == voted_for_) return Status::Ok();
  current_term_ = term;
  voted_for_ = voted_for;
  if (dir_.empty()) return Status::Ok();

  std::string tmp = dir_ + "/raft.meta.tmp";
  std::FILE* f = std::fopen(tmp.c_str(), "wb");
  if (f == nullptr) return Status::IOError("open " + tmp);
  std::string body = std::to_string(term) + "\n" + voted_for + "\n";
  bool ok = std::fwrite(body.data(), 1, body.size(), f) == body.size() && std::fflush(f) == 0;
  if (ok && sync_) ok = ::fsync(::fileno(f)) == 0;
  std::fclose(f);
  if (!ok) return Status::IOError("write raft meta");
  std::error_code ec;
  fs::rename(tmp, dir_ + "/raft.meta", ec);
  if (ec) return Status::IOError("rename raft meta: " + ec.message());
  return Status::Ok();
}

std::vector<LogEntry> RaftLog::Slice(uint64_t from, size_t max) const {
  std::vector<LogEntry> out;
  for (uint64_t i = from; i <= LastIndex() && out.size() < max; ++i) out.push_back(entries_[i]);
  return out;
}

Status RaftLog::WriteRecords(std::FILE* f, const std::vector<LogEntry>& entries, size_t from) {
  std::string buf;
  for (size_t i = from; i < entries.size(); ++i) {
    std::string payload;
    PutVarint64(&payload, entries[i].term);
    PutVarint64(&payload, entries[i].index);
    PutLengthPrefixed(&payload, entries[i].command);
    PutFixed32(&buf, Crc32(payload.data(), payload.size()));
    PutFixed32(&buf, static_cast<uint32_t>(payload.size()));
    buf += payload;
  }
  if (std::fwrite(buf.data(), 1, buf.size(), f) != buf.size() || std::fflush(f) != 0) {
    return Status::IOError("write raft log");
  }
  return Status::Ok();
}

Status RaftLog::SyncFile(std::FILE* f) {
  if (sync_ && ::fsync(::fileno(f)) != 0) return Status::IOError("fsync raft log");
  return Status::Ok();
}

Status RaftLog::Append(const std::vector<LogEntry>& entries) {
  for (const auto& e : entries) {
    if (e.index != entries_.size()) return Status::InvalidArgument("non-contiguous append");
    entries_.push_back(e);
  }
  if (log_file_ == nullptr) return Status::Ok();
  Status s = WriteRecords(log_file_, entries, 0);
  return s.ok() ? SyncFile(log_file_) : s;
}

Status RaftLog::TruncateFrom(uint64_t index) {
  if (index == 0 || index > LastIndex()) return Status::Ok();
  entries_.resize(index);
  if (log_file_ == nullptr) return Status::Ok();

  // Rewrite the surviving prefix to a new file, then swap it in.
  std::string path = dir_ + "/raft.log";
  std::string tmp = path + ".tmp";
  std::FILE* f = std::fopen(tmp.c_str(), "wb");
  if (f == nullptr) return Status::IOError("open " + tmp);
  Status s = WriteRecords(f, entries_, 1);
  if (s.ok()) s = SyncFile(f);
  std::fclose(f);
  if (!s.ok()) return s;
  std::fclose(log_file_);
  std::error_code ec;
  fs::rename(tmp, path, ec);
  if (ec) return Status::IOError("rename raft log: " + ec.message());
  log_file_ = std::fopen(path.c_str(), "ab");
  return log_file_ ? Status::Ok() : Status::IOError("reopen raft log");
}

}  // namespace kv::raft
