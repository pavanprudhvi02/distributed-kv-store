#include <gtest/gtest.h>

#include <fstream>

#include "storage/bloom.h"
#include "storage/memtable.h"
#include "storage/sstable.h"
#include "storage/wal.h"
#include "test_util.h"

namespace kv {

TEST(BloomFilterTest, NoFalseNegativesAndFewFalsePositives) {
  std::vector<std::string> keys;
  for (int i = 0; i < 5000; ++i) keys.push_back("key-" + std::to_string(i));
  std::string filter = BloomFilter::Build(keys);
  for (const auto& k : keys) ASSERT_TRUE(BloomFilter::MayContain(filter, k));

  int false_positives = 0;
  for (int i = 0; i < 10000; ++i) {
    if (BloomFilter::MayContain(filter, "absent-" + std::to_string(i))) ++false_positives;
  }
  EXPECT_LT(false_positives, 300);  // ~1% expected at 10 bits per key
}

TEST(WalTest, ReplaysRecordsInOrder) {
  test::TempDir dir;
  std::string path = dir.sub("a.wal");
  {
    WriteAheadLog wal;
    ASSERT_TRUE(wal.Open(path).ok());
    ASSERT_TRUE(wal.Append(RecordType::kPut, "a", "1", false).ok());
    ASSERT_TRUE(wal.Append(RecordType::kPut, "b", std::string(1000, 'x'), true).ok());
    ASSERT_TRUE(wal.Append(RecordType::kDelete, "a", "", false).ok());
  }
  std::vector<std::string> seen;
  size_t n = 0;
  ASSERT_TRUE(WriteAheadLog::Replay(
                  path,
                  [&](RecordType t, std::string_view k, std::string_view v) {
                    seen.push_back((t == RecordType::kPut ? "put:" : "del:") + std::string(k) + ":" +
                                   std::to_string(v.size()));
                  },
                  &n)
                  .ok());
  EXPECT_EQ(n, 3u);
  EXPECT_EQ(seen, (std::vector<std::string>{"put:a:1", "put:b:1000", "del:a:0"}));
}

TEST(WalTest, IgnoresTornAndCorruptTail) {
  test::TempDir dir;
  std::string path = dir.sub("torn.wal");
  {
    WriteAheadLog wal;
    ASSERT_TRUE(wal.Open(path).ok());
    for (int i = 0; i < 10; ++i) ASSERT_TRUE(wal.Append(RecordType::kPut, "k" + std::to_string(i), "v", false).ok());
  }
  // Simulate a crash halfway through an append, then a flipped bit before it.
  std::filesystem::resize_file(path, std::filesystem::file_size(path) - 3);
  size_t n = 0;
  ASSERT_TRUE(WriteAheadLog::Replay(path, [](auto, auto, auto) {}, &n).ok());
  EXPECT_EQ(n, 9u);

  {
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(20);
    f.put('\x7f');
  }
  ASSERT_TRUE(WriteAheadLog::Replay(path, [](auto, auto, auto) {}, &n).ok());
  EXPECT_LT(n, 9u);
}

TEST(MemTableTest, TracksValuesAndTombstones) {
  MemTable mem;
  mem.Put("a", "1");
  mem.Put("a", "2");
  mem.Delete("b");
  EXPECT_EQ(mem.Get("a")->value, "2");
  EXPECT_TRUE(mem.Get("b")->deleted);
  EXPECT_FALSE(mem.Get("c").has_value());
  EXPECT_EQ(mem.size(), 2u);
}

TEST(SSTableTest, PointLookupsAcrossIndexIntervals) {
  test::TempDir dir;
  std::map<std::string, Entry, std::less<>> entries;
  for (int i = 0; i < 1000; ++i) {
    char key[16];
    std::snprintf(key, sizeof(key), "key%05d", i);
    entries[key] = Entry{i % 10 == 0, i % 10 == 0 ? "" : "value-" + std::to_string(i)};
  }
  std::string path = dir.sub("1.sst");
  ASSERT_TRUE(SSTable::Write(path, entries).ok());

  std::shared_ptr<SSTable> table;
  ASSERT_TRUE(SSTable::Open(path, &table).ok());
  EXPECT_EQ(table->entry_count(), 1000u);
  EXPECT_EQ(table->Get("key00007")->value, "value-7");
  EXPECT_EQ(table->Get("key00999")->value, "value-999");
  EXPECT_TRUE(table->Get("key00010")->deleted);
  EXPECT_FALSE(table->Get("key01000").has_value());
  EXPECT_FALSE(table->Get("aaa").has_value());
  EXPECT_FALSE(table->Get("key00007x").has_value());

  size_t n = 0;
  std::string prev;
  for (auto it = table->NewIterator(); it->Valid(); it->Next()) {
    EXPECT_LT(prev, it->key());
    prev = it->key();
    ++n;
  }
  EXPECT_EQ(n, 1000u);
}

TEST(SSTableTest, IteratesRecordsLargerThanTheReadBuffer) {
  test::TempDir dir;
  std::map<std::string, Entry, std::less<>> entries;
  entries["a"] = Entry{false, std::string(600 * 1024, 'a')};
  entries["b"] = Entry{false, "small"};
  entries["c"] = Entry{false, std::string(300 * 1024, 'c')};
  ASSERT_TRUE(SSTable::Write(dir.sub("big.sst"), entries).ok());
  std::shared_ptr<SSTable> table;
  ASSERT_TRUE(SSTable::Open(dir.sub("big.sst"), &table).ok());
  std::vector<size_t> sizes;
  for (auto it = table->NewIterator(); it->Valid(); it->Next()) sizes.push_back(it->entry().value.size());
  EXPECT_EQ(sizes, (std::vector<size_t>{600 * 1024, 5, 300 * 1024}));
}

TEST(SSTableTest, DetectsCorruption) {
  test::TempDir dir;
  std::map<std::string, Entry, std::less<>> entries{{"a", {false, "1"}}, {"b", {false, "2"}}};
  std::string path = dir.sub("c.sst");
  ASSERT_TRUE(SSTable::Write(path, entries).ok());
  {
    std::fstream f(path, std::ios::in | std::ios::out | std::ios::binary);
    f.seekp(3);
    f.put('Z');
  }
  std::shared_ptr<SSTable> table;
  Status s = SSTable::Open(path, &table);
  EXPECT_EQ(s.code(), Status::Code::kCorruption) << s.ToString();
}

}  // namespace kv
