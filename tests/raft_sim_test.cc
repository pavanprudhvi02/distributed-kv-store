// The browser demo's single-threaded simulation, tested natively.
#include <gtest/gtest.h>

#include "../web/raft_sim.h"
#include "test_util.h"

namespace kv {

namespace {
sim::Simulation::Options Opts(const test::TempDir& dir) {
  sim::Simulation::Options o;
  o.data_root = dir.sub("sim");
  return o;
}

int StepUntilLeader(sim::Simulation& s, int max_ms = 20000) {
  for (int t = 0; t < max_ms && s.CurrentLeader() < 0; t += 50) s.Step(50);
  return s.CurrentLeader();
}
}  // namespace

TEST(RaftSimTest, ElectsALeaderAndReplicatesWrites) {
  test::TempDir dir;
  sim::Simulation s(Opts(dir));
  int leader = StepUntilLeader(s);
  ASSERT_GE(leader, 0);
  EXPECT_NE(s.Put("color", "blue", -1).find("\"ok\":true"), std::string::npos);
  s.Step(3000);
  EXPECT_EQ(s.WriteStatus(0), "committed");
  for (int i = 0; i < 5; ++i) EXPECT_EQ(s.Data(i).at("color"), "blue") << i;
  EXPECT_NE(s.Get("color").find("\"value\":\"blue\""), std::string::npos);
  EXPECT_NE(s.State().find("\"role\":\"leader\""), std::string::npos);
}

TEST(RaftSimTest, MinorityLeaderWriteIsLostAndMajorityContinues) {
  test::TempDir dir;
  sim::Simulation s(Opts(dir));
  int old_leader = StepUntilLeader(s);
  ASSERT_GE(old_leader, 0);
  s.PartitionLeader();
  // The old leader still thinks it leads; a write sent to it cannot commit.
  s.Put("stale", "1", old_leader);
  s.Step(8000);
  int new_leader = s.CurrentLeader();
  ASSERT_GE(new_leader, 0);
  ASSERT_NE(new_leader, old_leader);
  EXPECT_GT(s.NodeStatus(new_leader).term, s.NodeStatus(old_leader).term);
  s.Put("fresh", "2", -1);
  s.Step(3000);
  EXPECT_EQ(s.WriteStatus(1), "committed");
  EXPECT_NE(s.WriteStatus(0), "committed") << "a minority can never commit";

  s.Heal();
  s.Step(6000);
  EXPECT_EQ(s.WriteStatus(0), "lost");
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(s.Data(i).count("stale"), 0u) << i;
    EXPECT_EQ(s.Data(i).at("fresh"), "2") << i;
  }
}

TEST(RaftSimTest, CrashedLeaderIsReplacedAndRestartedNodeCatchesUp) {
  test::TempDir dir;
  sim::Simulation s(Opts(dir));
  int leader = StepUntilLeader(s);
  s.Put("a", "1", -1);
  s.Step(2000);
  s.KillNode(leader);
  EXPECT_FALSE(s.Alive(leader));
  s.Step(8000);
  int next = s.CurrentLeader();
  ASSERT_GE(next, 0);
  ASSERT_NE(next, leader);
  s.Put("b", "2", -1);
  s.Step(2000);
  s.RestartNode(leader);
  s.Step(4000);
  EXPECT_EQ(s.Data(leader).at("a"), "1");
  EXPECT_EQ(s.Data(leader).at("b"), "2");
}

}  // namespace kv
