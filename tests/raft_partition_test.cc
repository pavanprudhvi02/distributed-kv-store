#include <gtest/gtest.h>

#include <set>

#include "raft_cluster.h"

namespace kv {

using test::Eventually;
using test::RaftCluster;

namespace {

// Every replica's applied sequence must be a prefix of the longest one:
// replicas can lag, but they can never disagree.
void ExpectNoDivergence(RaftCluster& c) {
  std::vector<std::string> longest;
  for (int i = 0; i < c.size(); ++i) {
    auto cmds = c.machine(i).Commands();
    if (cmds.size() > longest.size()) longest = cmds;
  }
  for (int i = 0; i < c.size(); ++i) {
    auto cmds = c.machine(i).Commands();
    ASSERT_TRUE(std::equal(cmds.begin(), cmds.end(), longest.begin())) << c.id(i) << " diverged";
  }
}

std::vector<std::string> Others(RaftCluster& c, const std::set<int>& exclude) {
  std::vector<std::string> out;
  for (int i = 0; i < c.size(); ++i) {
    if (!exclude.count(i)) out.push_back(c.id(i));
  }
  return out;
}

}  // namespace

TEST(RaftPartitionTest, MinorityLeaderCannotCommitAndStepsDownOnHeal) {
  RaftCluster c(5);
  int old_leader = c.WaitForLeader();
  ASSERT_TRUE(c.node(old_leader).Propose("committed-before", std::chrono::seconds(5)).ok());
  int buddy = (old_leader + 1) % 5;

  // Old leader keeps one follower; the other three form a majority.
  c.net().Partition({{c.id(old_leader), c.id(buddy)}, Others(c, {old_leader, buddy})});

  // The minority side cannot reach a quorum, so this write must not succeed.
  Status lost = c.node(old_leader).Propose("never-committed", std::chrono::milliseconds(800));
  EXPECT_FALSE(lost.ok()) << lost.ToString();

  // The majority elects a new leader in a higher term and keeps serving.
  int new_leader = -1;
  ASSERT_TRUE(Eventually([&] {
    for (int i = 0; i < 5; ++i) {
      if (i == old_leader || i == buddy) continue;
      if (c.node(i).IsLeader()) new_leader = i;
    }
    return new_leader >= 0;
  }));
  EXPECT_GT(c.node(new_leader).GetStatus().term, c.node(old_leader).GetStatus().term);
  ASSERT_TRUE(c.node(new_leader).Propose("committed-during", std::chrono::seconds(5)).ok());

  c.net().Heal();

  // The old leader learns about the newer term, steps down, and its
  // uncommitted entry is replaced by the majority's log.
  ASSERT_TRUE(Eventually([&] { return !c.node(old_leader).IsLeader(); }));
  ASSERT_TRUE(c.ProposeAnywhere("committed-after").ok());
  ASSERT_TRUE(Eventually([&] {
    for (int i = 0; i < 5; ++i) {
      if (c.machine(i).Count() < 3) return false;
    }
    return true;
  }));
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(c.machine(i).Commands(),
              (std::vector<std::string>{"committed-before", "committed-during", "committed-after"}))
        << c.id(i);
  }
}

TEST(RaftPartitionTest, IsolatedLeaderRefusesReads) {
  RaftCluster c(3);
  int leader = c.WaitForLeader();
  ASSERT_TRUE(c.node(leader).ReadBarrier(std::chrono::seconds(2)).ok());

  c.net().Isolate(c.id(leader));
  // It still believes it is leader, but it cannot prove it, so a read
  // (which could be stale) must fail instead of answering.
  Status s = c.node(leader).ReadBarrier(std::chrono::milliseconds(500));
  EXPECT_FALSE(s.ok());

  // Meanwhile the other two elect someone else and accept writes.
  ASSERT_TRUE(Eventually([&] {
    for (int i = 0; i < 3; ++i) {
      if (i != leader && c.node(i).IsLeader()) return true;
    }
    return false;
  }));
  c.net().Heal();
  ASSERT_TRUE(Eventually([&] { return !c.node(leader).IsLeader() || c.Leader() == leader; }));
}

TEST(RaftPartitionTest, IsolatedFollowerRejoinsAndRepairsItsLog) {
  RaftCluster c(5);
  int leader = c.WaitForLeader();
  int follower = (leader + 2) % 5;
  c.net().Isolate(c.id(follower));
  // While cut off it keeps timing out and bumping its term.
  std::this_thread::sleep_for(std::chrono::milliseconds(700));
  for (int i = 0; i < 50; ++i) ASSERT_TRUE(c.ProposeAnywhere("p" + std::to_string(i)).ok());
  c.net().Heal();

  // Its inflated term forces a new election when it reconnects (there is no
  // pre-vote phase), so a proposal may be retried and land twice. What must
  // hold is that the follower ends up with exactly the leader's sequence.
  ASSERT_TRUE(c.ProposeAnywhere("after-heal").ok());
  ASSERT_TRUE(Eventually([&] {
    auto leader = c.Leader();
    if (!leader) return false;
    auto mine = c.machine(follower).Commands();
    return !mine.empty() && mine.back() == "after-heal" && mine == c.machine(*leader).Commands();
  }));
  auto cmds = c.machine(follower).Commands();
  EXPECT_GE(cmds.size(), 51u);
  EXPECT_EQ(cmds.front(), "p0");
}

TEST(RaftPartitionTest, NoAcknowledgedWriteIsLostUnderChurn) {
  RaftCluster c(5);
  c.WaitForLeader();
  std::vector<std::string> acknowledged;
  std::mt19937 rng(1234);

  for (int round = 0; round < 6; ++round) {
    // Random partition: a random majority/minority split, plus message loss.
    std::vector<int> order{0, 1, 2, 3, 4};
    std::shuffle(order.begin(), order.end(), rng);
    std::vector<std::string> a, b;
    for (int i = 0; i < 5; ++i) (i < 3 ? a : b).push_back(c.id(order[i]));
    c.net().Partition({a, b});
    c.net().SetDropRate(0.1);

    for (int i = 0; i < 15; ++i) {
      std::string cmd = "r" + std::to_string(round) + "-" + std::to_string(i);
      if (c.ProposeAnywhere(cmd, std::chrono::seconds(3)).ok()) acknowledged.push_back(cmd);
    }
    c.net().Heal();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  ASSERT_TRUE(c.ProposeAnywhere("final").ok());
  acknowledged.push_back("final");
  ASSERT_TRUE(Eventually([&] {
    for (int i = 0; i < 5; ++i) {
      auto cmds = c.machine(i).Commands();
      if (cmds.empty() || cmds.back() != "final") return false;
    }
    return true;
  }, std::chrono::seconds(15)));

  ExpectNoDivergence(c);
  auto applied = c.machine(0).Commands();
  std::set<std::string> applied_set(applied.begin(), applied.end());
  for (const auto& cmd : acknowledged) EXPECT_TRUE(applied_set.count(cmd)) << "lost acknowledged write " << cmd;
  EXPECT_GT(acknowledged.size(), 40u);
}

}  // namespace kv
