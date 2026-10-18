#include <gtest/gtest.h>

#include "raft_cluster.h"

namespace kv {

using test::Eventually;
using test::RaftCluster;

namespace {
bool AllApplied(RaftCluster& c, size_t n) {
  for (int i = 0; i < c.size(); ++i) {
    if (c.alive(i) && c.machine(i).Count() < n) return false;
  }
  return true;
}
}  // namespace

TEST(RaftTest, ElectsExactlyOneLeaderPerTerm) {
  RaftCluster c(3);
  int leader = c.WaitForLeader();
  ASSERT_GE(leader, 0);
  // Let a few heartbeats pass; leadership should be stable with no failures.
  std::this_thread::sleep_for(std::chrono::milliseconds(400));
  int leaders = 0;
  uint64_t term = c.node(leader).GetStatus().term;
  for (int i = 0; i < 3; ++i) {
    auto s = c.node(i).GetStatus();
    if (s.role == raft::Role::kLeader) ++leaders;
    EXPECT_EQ(s.term, term);
    EXPECT_EQ(s.leader_id, c.id(leader));
  }
  EXPECT_EQ(leaders, 1);
}

TEST(RaftTest, ReplicatesCommandsInTheSameOrderEverywhere) {
  RaftCluster c(3);
  int leader = c.WaitForLeader();
  for (int i = 0; i < 100; ++i) {
    ASSERT_TRUE(c.node(leader).Propose("cmd-" + std::to_string(i), std::chrono::seconds(5)).ok());
  }
  ASSERT_TRUE(Eventually([&] { return AllApplied(c, 100); }));
  auto expected = c.machine(leader).Commands();
  for (int i = 0; i < 3; ++i) EXPECT_EQ(c.machine(i).Commands(), expected) << c.id(i);
  EXPECT_EQ(expected.front(), "cmd-0");
  EXPECT_EQ(expected.back(), "cmd-99");
}

TEST(RaftTest, FollowersRedirectToTheLeader) {
  RaftCluster c(3);
  int leader = c.WaitForLeader();
  int follower = (leader + 1) % 3;
  ASSERT_TRUE(Eventually([&] { return c.node(follower).GetStatus().leader_id == c.id(leader); }));
  Status s = c.node(follower).Propose("x", std::chrono::seconds(1));
  EXPECT_TRUE(s.IsNotLeader());
  EXPECT_EQ(s.message(), c.id(leader));
  EXPECT_TRUE(c.node(follower).ReadBarrier(std::chrono::seconds(1)).IsNotLeader());
}

TEST(RaftTest, NewLeaderKeepsCommittedEntriesAfterLeaderCrash) {
  RaftCluster c(3);
  int leader = c.WaitForLeader();
  uint64_t old_term = c.node(leader).GetStatus().term;
  for (int i = 0; i < 20; ++i) ASSERT_TRUE(c.node(leader).Propose("before-" + std::to_string(i), std::chrono::seconds(5)).ok());

  c.StopNode(leader);
  int next = c.WaitForLeader();
  ASSERT_NE(next, leader);
  EXPECT_GT(c.node(next).GetStatus().term, old_term);
  ASSERT_TRUE(c.node(next).Propose("after", std::chrono::seconds(5)).ok());

  auto cmds = c.machine(next).Commands();
  ASSERT_EQ(cmds.size(), 21u);
  EXPECT_EQ(cmds[19], "before-19");
  EXPECT_EQ(cmds[20], "after");
}

TEST(RaftTest, RestartedNodesRecoverTheirLogFromDisk) {
  RaftCluster c(3);
  c.WaitForLeader();
  for (int i = 0; i < 30; ++i) ASSERT_TRUE(c.ProposeAnywhere("v" + std::to_string(i)).ok());
  ASSERT_TRUE(Eventually([&] { return AllApplied(c, 30); }));
  auto expected = c.machine(0).Commands();

  // Crash every node, then bring them all back.
  for (int i = 0; i < 3; ++i) c.StopNode(i);
  for (int i = 0; i < 3; ++i) {
    c.machine(i).Clear();
    c.StartNode(i);
  }
  c.WaitForLeader();
  ASSERT_TRUE(c.ProposeAnywhere("after-restart").ok());
  ASSERT_TRUE(Eventually([&] { return AllApplied(c, 31); }));
  for (int i = 0; i < 3; ++i) {
    auto cmds = c.machine(i).Commands();
    ASSERT_EQ(cmds.size(), 31u);
    EXPECT_TRUE(std::equal(expected.begin(), expected.end(), cmds.begin()));
  }
}

TEST(RaftTest, LaggingFollowerCatchesUp) {
  RaftCluster c(3);
  int leader = c.WaitForLeader();
  int lagging = (leader + 1) % 3;
  c.StopNode(lagging);
  for (int i = 0; i < 600; ++i) ASSERT_TRUE(c.node(leader).Propose("e" + std::to_string(i), std::chrono::seconds(5)).ok());

  c.machine(lagging).Clear();
  c.StartNode(lagging);  // needs several AppendEntries batches (256 entries each)
  ASSERT_TRUE(Eventually([&] { return c.machine(lagging).Count() == 600; }));
  EXPECT_EQ(c.machine(lagging).Commands(), c.machine(leader).Commands());
}

TEST(RaftTest, SingleNodeGroupCommitsOnItsOwn) {
  RaftCluster c(1);
  int leader = c.WaitForLeader();
  ASSERT_EQ(leader, 0);
  ASSERT_TRUE(c.node(0).Propose("solo", std::chrono::seconds(2)).ok());
  ASSERT_TRUE(c.node(0).ReadBarrier(std::chrono::seconds(2)).ok());
  EXPECT_EQ(c.machine(0).Commands(), std::vector<std::string>{"solo"});
}

TEST(RaftTest, ReadBarrierSeesEveryAcknowledgedWrite) {
  RaftCluster c(3);
  int leader = c.WaitForLeader();
  for (int i = 0; i < 10; ++i) {
    ASSERT_TRUE(c.node(leader).Propose("w" + std::to_string(i), std::chrono::seconds(5)).ok());
    ASSERT_TRUE(c.node(leader).ReadBarrier(std::chrono::seconds(5)).ok());
    EXPECT_EQ(c.machine(leader).Count(), static_cast<size_t>(i + 1));
  }
}

}  // namespace kv
