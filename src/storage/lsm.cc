#include "storage/lsm.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <queue>

namespace kv {

namespace fs = std::filesystem;

LsmTree::LsmTree(const LsmOptions& options) : options_(options) {}

LsmTree::~LsmTree() {
  WaitForBackgroundWork();
  pool_.Shutdown();
}

std::string LsmTree::TablePath(uint64_t number) const {
  return options_.dir + "/" + std::to_string(number) + ".sst";
}

std::string LsmTree::WalPath(uint64_t number) const {
  return options_.dir + "/" + std::to_string(number) + ".wal";
}

Status LsmTree::Open(const LsmOptions& options, std::unique_ptr<LsmTree>* db) {
  if (options.dir.empty()) return Status::InvalidArgument("dir is required");
  std::error_code ec;
  fs::create_directories(options.dir, ec);
  if (ec) return Status::IOError("create " + options.dir + ": " + ec.message());
  std::unique_ptr<LsmTree> tree(new LsmTree(options));
  Status s = tree->Recover();
  if (!s.ok()) return s;
  *db = std::move(tree);
  return Status::Ok();
}

Status LsmTree::Recover() {
  auto version = std::make_shared<Version>();

  // 1. Live tables come from the manifest (one file number per line, newest first).
  std::ifstream manifest(options_.dir + "/MANIFEST");
  uint64_t number;
  while (manifest >> number) {
    std::shared_ptr<SSTable> table;
    Status s = SSTable::Open(TablePath(number), &table);
    if (!s.ok()) return s;
    version->tables.push_back(table);
    version->numbers.push_back(number);
    next_file_number_ = std::max(next_file_number_, number + 1);
  }

  // 2. Anything else on disk is either a WAL to replay or garbage from a crash.
  std::vector<uint64_t> wals;
  for (const auto& f : fs::directory_iterator(options_.dir)) {
    const auto name = f.path().filename().string();
    const auto ext = f.path().extension().string();
    if (ext == ".wal") {
      uint64_t n = std::stoull(f.path().stem().string());
      wals.push_back(n);
      next_file_number_ = std::max(next_file_number_, n + 1);
    } else if (ext == ".sst" || ext == ".tmp") {
      uint64_t n = std::strtoull(f.path().stem().string().c_str(), nullptr, 10);
      next_file_number_ = std::max(next_file_number_, n + 1);
      if (std::find(version->numbers.begin(), version->numbers.end(), n) == version->numbers.end()) {
        fs::remove(f.path());  // not in the manifest: an unfinished flush or compaction
      }
    }
  }
  std::sort(wals.begin(), wals.end());

  mem_ = std::make_shared<MemTable>();
  for (uint64_t n : wals) {
    Status s = WriteAheadLog::Replay(WalPath(n), [this](RecordType type, std::string_view k, std::string_view v) {
      if (type == RecordType::kPut) {
        mem_->Put(k, v);
      } else {
        mem_->Delete(k);
      }
    });
    if (!s.ok() && !s.IsNotFound()) return s;
  }

  // 3. Persist whatever the WALs held as a table, then start a fresh WAL.
  if (!mem_->empty()) {
    uint64_t table_number = next_file_number_++;
    Status s = SSTable::Write(TablePath(table_number), mem_->Snapshot());
    if (!s.ok()) return s;
    std::shared_ptr<SSTable> table;
    if (!(s = SSTable::Open(TablePath(table_number), &table)).ok()) return s;
    version->tables.insert(version->tables.begin(), table);
    version->numbers.insert(version->numbers.begin(), table_number);
    if (!(s = WriteManifest(*version)).ok()) return s;
    mem_ = std::make_shared<MemTable>();
  }
  for (uint64_t n : wals) fs::remove(WalPath(n));

  wal_ = std::make_unique<WriteAheadLog>();
  Status s = wal_->Open(WalPath(next_file_number_++));
  if (!s.ok()) return s;
  version_ = version;
  stats_.sstables = version->tables.size();
  MaybeScheduleCompaction();
  return Status::Ok();
}

Status LsmTree::WriteManifest(const Version& v) {
  std::string tmp = options_.dir + "/MANIFEST.tmp";
  {
    std::ofstream out(tmp, std::ios::trunc);
    for (uint64_t n : v.numbers) out << n << "\n";
    out.flush();
    if (!out) return Status::IOError("write manifest");
  }
  std::error_code ec;
  fs::rename(tmp, options_.dir + "/MANIFEST", ec);
  if (ec) return Status::IOError("rename manifest: " + ec.message());
  return Status::Ok();
}

Status LsmTree::Put(std::string_view key, std::string_view value) { return Write(RecordType::kPut, key, value); }

Status LsmTree::Delete(std::string_view key) { return Write(RecordType::kDelete, key, {}); }

Status LsmTree::Write(RecordType type, std::string_view key, std::string_view value) {
  if (key.empty()) return Status::InvalidArgument("empty key");
  std::unique_lock lock(mu_);
  if (!bg_error_.ok()) return bg_error_;
  Status s = wal_->Append(type, key, value, options_.sync_writes);
  if (!s.ok()) return s;
  if (type == RecordType::kPut) {
    mem_->Put(key, value);
  } else {
    mem_->Delete(key);
  }
  if (mem_->ApproximateBytes() >= options_.memtable_bytes) return RotateMemtableLocked(lock);
  return Status::Ok();
}

Status LsmTree::RotateMemtableLocked(std::unique_lock<std::mutex>& lock) {
  // Backpressure: only one immutable memtable may wait for its flush.
  bg_cv_.wait(lock, [this] { return imm_ == nullptr || !bg_error_.ok(); });
  if (!bg_error_.ok()) return bg_error_;
  if (mem_->empty()) return Status::Ok();

  imm_ = mem_;
  imm_wal_path_ = wal_->path();
  mem_ = std::make_shared<MemTable>();
  wal_ = std::make_unique<WriteAheadLog>();
  Status s = wal_->Open(WalPath(next_file_number_++));
  if (!s.ok()) return s;
  flush_scheduled_ = true;
  pool_.Submit([this] { BackgroundFlush(); });
  return Status::Ok();
}

void LsmTree::BackgroundFlush() {
  std::shared_ptr<MemTable> imm;
  uint64_t number;
  {
    std::lock_guard lock(mu_);
    imm = imm_;
    number = next_file_number_++;
  }
  Status s = SSTable::Write(TablePath(number), imm->Snapshot());
  std::shared_ptr<SSTable> table;
  if (s.ok()) s = SSTable::Open(TablePath(number), &table);

  std::lock_guard lock(mu_);
  if (s.ok()) {
    auto next = std::make_shared<Version>(*version_);
    next->tables.insert(next->tables.begin(), table);
    next->numbers.insert(next->numbers.begin(), number);
    s = WriteManifest(*next);
    if (s.ok()) {
      version_ = next;
      std::error_code ec;
      fs::remove(imm_wal_path_, ec);  // its contents now live in the table
      imm_.reset();
      ++stats_.flushes;
      stats_.sstables = next->tables.size();
    }
  }
  if (!s.ok()) bg_error_ = s;
  flush_scheduled_ = false;
  bg_cv_.notify_all();
  if (s.ok()) MaybeScheduleCompaction();
}

// Called with mu_ held.
void LsmTree::MaybeScheduleCompaction() {
  if (compaction_scheduled_ || version_->tables.size() < options_.compaction_trigger) return;
  compaction_scheduled_ = true;
  pool_.Submit([this] { BackgroundCompaction(); });
}

void LsmTree::BackgroundCompaction() {
  std::shared_ptr<const Version> base;
  uint64_t number;
  {
    std::lock_guard lock(mu_);
    base = version_;
    number = next_file_number_++;
  }

  // K-way merge over every table. On equal keys the newest table wins.
  // Because all tables take part, tombstones have nothing left to shadow and are dropped.
  struct Cursor {
    std::unique_ptr<SSTable::Iterator> it;
    size_t age;  // 0 = newest
  };
  auto cmp = [](const Cursor* a, const Cursor* b) {
    if (a->it->key() != b->it->key()) return a->it->key() > b->it->key();
    return a->age > b->age;
  };
  std::vector<Cursor> cursors;
  cursors.reserve(base->tables.size());
  for (size_t i = 0; i < base->tables.size(); ++i) cursors.push_back({base->tables[i]->NewIterator(), i});
  std::priority_queue<Cursor*, std::vector<Cursor*>, decltype(cmp)> heap(cmp);
  for (auto& c : cursors) {
    if (c.it->Valid()) heap.push(&c);
  }

  auto next = [&](std::string* key, Entry* entry) -> bool {
    while (!heap.empty()) {
      Cursor* top = heap.top();
      heap.pop();
      std::string k = top->it->key();
      Entry e = top->it->entry();
      top->it->Next();
      if (top->it->Valid()) heap.push(top);
      // Skip older versions of the same key.
      while (!heap.empty() && heap.top()->it->key() == k) {
        Cursor* dup = heap.top();
        heap.pop();
        dup->it->Next();
        if (dup->it->Valid()) heap.push(dup);
      }
      if (e.deleted) continue;
      *key = std::move(k);
      *entry = std::move(e);
      return true;
    }
    return false;
  };

  Status s = SSTable::Write(TablePath(number), next);
  std::shared_ptr<SSTable> merged;
  if (s.ok()) s = SSTable::Open(TablePath(number), &merged);

  std::lock_guard lock(mu_);
  if (s.ok()) {
    // Tables flushed while we were merging are newer; keep them in front.
    auto nv = std::make_shared<Version>();
    for (size_t i = 0; i < version_->tables.size(); ++i) {
      uint64_t n = version_->numbers[i];
      if (std::find(base->numbers.begin(), base->numbers.end(), n) == base->numbers.end()) {
        nv->tables.push_back(version_->tables[i]);
        nv->numbers.push_back(n);
      }
    }
    if (merged->entry_count() > 0) {
      nv->tables.push_back(merged);
      nv->numbers.push_back(number);
    }
    s = WriteManifest(*nv);
    if (s.ok()) {
      version_ = nv;
      for (uint64_t n : base->numbers) {
        std::error_code ec;
        fs::remove(TablePath(n), ec);  // open readers keep their fd until released
      }
      if (merged->entry_count() == 0) {
        std::error_code ec;
        fs::remove(TablePath(number), ec);
      }
      ++stats_.compactions;
      stats_.sstables = nv->tables.size();
    }
  }
  if (!s.ok()) bg_error_ = s;
  compaction_scheduled_ = false;
  bg_cv_.notify_all();
  if (s.ok()) MaybeScheduleCompaction();
}

Status LsmTree::Get(std::string_view key, std::string* value) const {
  std::shared_ptr<MemTable> mem, imm;
  std::shared_ptr<const Version> version;
  {
    std::lock_guard lock(mu_);
    mem = mem_;
    imm = imm_;
    version = version_;
  }
  auto found = [&](const Entry& e) {
    if (e.deleted) return Status::NotFound();
    *value = e.value;
    return Status::Ok();
  };
  if (auto e = mem->Get(key)) return found(*e);
  if (imm) {
    if (auto e = imm->Get(key)) return found(*e);
  }
  for (const auto& table : version->tables) {
    if (auto e = table->Get(key)) return found(*e);
  }
  return Status::NotFound();
}

Status LsmTree::Flush() {
  std::unique_lock lock(mu_);
  Status s = RotateMemtableLocked(lock);
  if (!s.ok()) return s;
  bg_cv_.wait(lock, [this] { return !flush_scheduled_; });
  return bg_error_;
}

void LsmTree::WaitForBackgroundWork() {
  std::unique_lock lock(mu_);
  bg_cv_.wait(lock, [this] { return !flush_scheduled_ && !compaction_scheduled_; });
}

LsmStats LsmTree::Stats() const {
  std::lock_guard lock(mu_);
  return stats_;
}

}  // namespace kv
