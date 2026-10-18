#include <gtest/gtest.h>

#include <map>
#include <random>
#include <thread>

#include "storage/lsm.h"
#include "test_util.h"

namespace kv {

namespace {
LsmOptions SmallOptions(const std::string& dir) {
  LsmOptions o;
  o.dir = dir;
  o.memtable_bytes = 16 * 1024;  // tiny, so tests exercise flushes and compactions
  o.compaction_trigger = 3;
  return o;
}
}  // namespace

TEST(LsmTreeTest, ReadsYourWritesAndDeletes) {
  test::TempDir dir;
  std::unique_ptr<LsmTree> db;
  ASSERT_TRUE(LsmTree::Open(SmallOptions(dir.path()), &db).ok());
  ASSERT_TRUE(db->Put("user:1", "ada").ok());
  ASSERT_TRUE(db->Put("user:2", "grace").ok());
  ASSERT_TRUE(db->Delete("user:1").ok());

  std::string v;
  EXPECT_TRUE(db->Get("user:1", &v).IsNotFound());
  ASSERT_TRUE(db->Get("user:2", &v).ok());
  EXPECT_EQ(v, "grace");
  EXPECT_TRUE(db->Put("", "x").code() == Status::Code::kInvalidArgument);
}

TEST(LsmTreeTest, MatchesAReferenceMapThroughFlushesAndCompactions) {
  test::TempDir dir;
  std::unique_ptr<LsmTree> db;
  ASSERT_TRUE(LsmTree::Open(SmallOptions(dir.path()), &db).ok());

  std::map<std::string, std::string> model;
  std::mt19937 rng(42);
  for (int i = 0; i < 20000; ++i) {
    std::string key = "k" + std::to_string(rng() % 2000);
    if (rng() % 5 == 0) {
      ASSERT_TRUE(db->Delete(key).ok());
      model.erase(key);
    } else {
      std::string value = "v" + std::to_string(i) + std::string(rng() % 64, 'x');
      ASSERT_TRUE(db->Put(key, value).ok());
      model[key] = value;
    }
  }
  db->WaitForBackgroundWork();
  LsmStats stats = db->Stats();
  EXPECT_GT(stats.flushes, 5u);
  EXPECT_GT(stats.compactions, 0u);
  EXPECT_LT(stats.sstables, 3u + 1);

  for (int i = 0; i < 2000; ++i) {
    std::string key = "k" + std::to_string(i);
    std::string v;
    Status s = db->Get(key, &v);
    auto it = model.find(key);
    if (it == model.end()) {
      EXPECT_TRUE(s.IsNotFound()) << key;
    } else {
      ASSERT_TRUE(s.ok()) << key << " " << s.ToString();
      EXPECT_EQ(v, it->second) << key;
    }
  }
}

TEST(LsmTreeTest, RecoversUnflushedWritesFromTheWal) {
  test::TempDir dir;
  LsmOptions opts = SmallOptions(dir.path());
  opts.memtable_bytes = 1 << 30;  // never flush: everything lives only in the WAL
  {
    std::unique_ptr<LsmTree> db;
    ASSERT_TRUE(LsmTree::Open(opts, &db).ok());
    for (int i = 0; i < 500; ++i) ASSERT_TRUE(db->Put("key" + std::to_string(i), "v" + std::to_string(i)).ok());
    ASSERT_TRUE(db->Delete("key7").ok());
  }
  std::unique_ptr<LsmTree> db;
  ASSERT_TRUE(LsmTree::Open(opts, &db).ok());
  std::string v;
  ASSERT_TRUE(db->Get("key499", &v).ok());
  EXPECT_EQ(v, "v499");
  EXPECT_TRUE(db->Get("key7", &v).IsNotFound());
}

TEST(LsmTreeTest, RecoversTablesAndWalTogether) {
  test::TempDir dir;
  LsmOptions opts = SmallOptions(dir.path());
  {
    std::unique_ptr<LsmTree> db;
    ASSERT_TRUE(LsmTree::Open(opts, &db).ok());
    for (int i = 0; i < 3000; ++i) ASSERT_TRUE(db->Put("key" + std::to_string(i), std::string(40, 'a' + i % 26)).ok());
    ASSERT_TRUE(db->Put("key0", "latest").ok());
    db->WaitForBackgroundWork();
  }
  std::unique_ptr<LsmTree> db;
  ASSERT_TRUE(LsmTree::Open(opts, &db).ok());
  std::string v;
  ASSERT_TRUE(db->Get("key0", &v).ok());
  EXPECT_EQ(v, "latest");
  ASSERT_TRUE(db->Get("key2999", &v).ok());
  EXPECT_EQ(v, std::string(40, 'a' + 2999 % 26));
}

TEST(LsmTreeTest, ConcurrentReadersAndWriters) {
  test::TempDir dir;
  std::unique_ptr<LsmTree> db;
  ASSERT_TRUE(LsmTree::Open(SmallOptions(dir.path()), &db).ok());

  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < 2000; ++i) {
        std::string key = "t" + std::to_string(t) + "-" + std::to_string(i);
        if (!db->Put(key, key).ok()) failed = true;
        std::string v;
        if (!db->Get(key, &v).ok() || v != key) failed = true;
      }
    });
  }
  for (auto& th : threads) th.join();
  EXPECT_FALSE(failed.load());
  db->WaitForBackgroundWork();
  std::string v;
  ASSERT_TRUE(db->Get("t3-1999", &v).ok());
}

}  // namespace kv
