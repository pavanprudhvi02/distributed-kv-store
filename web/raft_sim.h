#pragma once

// A whole Raft cluster in one thread, driven by virtual time, for the browser
// demo. The nodes are the real kv::raft::RaftNode from src/raft, started with
// background_threads = false and a virtual clock; this file only supplies the
// network (messages with latency that can be cut or dropped) and a simple
// in-memory key-value state machine per node. Free of Emscripten headers so it
// can be unit-tested natively.

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <deque>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "raft/command.h"
#include "raft/raft_node.h"

namespace kv::sim {

inline std::string JsonEscape(const std::string& s) {
  std::string out;
  for (char c : s) {
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          char buf[8];
          std::snprintf(buf, sizeof(buf), "\\u%04x", c);
          out += buf;
        } else {
          out += c;
        }
    }
  }
  return out;
}

// Cluster shape, timing and network latency for the simulation, in virtual ms.
struct SimOptions {
  int nodes = 5;
  std::string data_root = "/sim";
  int election_min_ms = 1500;
  int election_max_ms = 3000;
  int heartbeat_ms = 450;
  int latency_min_ms = 120;
  int latency_max_ms = 280;
  uint64_t seed = 7;
};

class Simulation {
 public:
  using Clock = std::chrono::steady_clock;

  using Options = SimOptions;

  explicit Simulation(Options o = Options()) : opt_(o), rng_(o.seed) {
    std::error_code ec;
    std::filesystem::remove_all(opt_.data_root, ec);
    for (int i = 0; i < opt_.nodes; ++i) {
      Node n;
      n.id = "n" + std::to_string(i + 1);
      nodes_.push_back(std::move(n));
    }
    for (int i = 0; i < opt_.nodes; ++i) StartNode(i);
    Log("Cluster of " + std::to_string(opt_.nodes) + " nodes started. Waiting for the first election.");
  }

  ~Simulation() {
    for (int i = 0; i < static_cast<int>(nodes_.size()); ++i) {
      if (nodes_[i].raft) KillNode(i, /*quiet=*/true);
    }
  }

  // ---- driving -------------------------------------------------------

  // Advances virtual time by `ms`, in 5 ms ticks.
  void Step(int ms) {
    for (int t = 0; t < ms; t += 5) {
      now_ms_ += 5;
      Deliver();
      for (auto& n : nodes_) {
        if (!n.raft) continue;
        n.raft->Tick();
        n.raft->ApplyCommitted();
      }
      ObserveTransitions();
      ResolveWrites();
      // Forget messages whose animation is over.
      msgs_.erase(std::remove_if(msgs_.begin(), msgs_.end(),
                                 [&](const Msg& m) { return m.done && now_ms_ > m.deliver + 600; }),
                  msgs_.end());
    }
  }

  // ---- fault injection ----------------------------------------------------

  void Crash(int i) { KillNode(i, false); }

  void KillNode(int i, bool quiet = false) {
    Node& n = nodes_[i];
    if (!n.raft) return;
    // Fail every RPC this node is still waiting on, so Stop() has nothing pending.
    for (auto& m : msgs_) {
      if (!m.done && m.owner == n.id) {
        m.done = true;
        m.dropped = true;
        m.fail();
      }
    }
    n.raft->Stop();
    n.raft.reset();
    n.transport.reset();
    if (!quiet) Log(n.id + " crashed. Its log and vote stay on disk; its in-memory state is gone.");
  }

  void RestartNode(int i) {
    if (nodes_[i].raft) return;
    StartNode(i);
    Log(nodes_[i].id + " restarted and reloaded its term, vote and log from disk.");
  }

  // Splits the cluster. Nodes not listed stay on side 0.
  void Partition(const std::vector<int>& minority) {
    side_.clear();
    std::string names;
    for (int i : minority) {
      side_[nodes_[i].id] = 1;
      names += (names.empty() ? "" : ", ") + nodes_[i].id;
    }
    Log("Network partition: {" + names + "} cut off from the rest.");
  }

  void Isolate(int i) { Partition({i}); }

  void PartitionLeader() {
    int leader = CurrentLeader();
    if (leader < 0) {
      Log("No leader to cut off yet.");
      return;
    }
    int buddy = (leader + 1) % opt_.nodes;
    Partition({leader, buddy});
  }

  void Heal() {
    if (side_.empty()) return;
    side_.clear();
    Log("Network healed. Every node can reach every other node again.");
  }

  // ---- client operations ---------------------------------------------------

  // target < 0 means "whichever node currently leads with the highest term".
  std::string Put(const std::string& key, const std::string& value, int target) {
    int node = target >= 0 ? target : CurrentLeader();
    if (node < 0 || !nodes_[node].raft) {
      Log("PUT " + key + " failed: no reachable leader right now. Try again after the election.");
      return "{\"ok\":false,\"error\":\"no leader\"}";
    }
    uint64_t index = 0, term = 0;
    kv::Status s = nodes_[node].raft->Append(Command::Put(key, value).Encode(), &index, &term);
    if (s.IsNotLeader()) {
      std::string hint = s.message().empty() ? "unknown" : s.message();
      Log("PUT " + key + " sent to " + nodes_[node].id + ", which is not the leader (redirect to " + hint + ").");
      return "{\"ok\":false,\"error\":\"not leader\",\"leader\":\"" + JsonEscape(hint) + "\"}";
    }
    if (!s.ok()) return "{\"ok\":false,\"error\":\"" + JsonEscape(s.ToString()) + "\"}";
    writes_.push_back({key, value, nodes_[node].id, index, term, "pending", now_ms_});
    if (writes_.size() > 12) writes_.pop_front();
    Log("PUT " + key + "=" + value + " appended by " + nodes_[node].id + " at index " + std::to_string(index) +
        " (term " + std::to_string(term) + "). Waiting for a majority.");
    return "{\"ok\":true,\"index\":" + std::to_string(index) + "}";
  }

  std::string Get(const std::string& key) {
    int leader = CurrentLeader();
    if (leader < 0) return "{\"ok\":false,\"error\":\"no leader\"}";
    const auto& kvs = nodes_[leader].machine->data;
    auto it = kvs.find(key);
    std::string msg = "GET " + key + " from leader " + nodes_[leader].id + ": " +
                      (it == kvs.end() ? std::string("not found") : it->second);
    Log(msg);
    return "{\"ok\":true,\"node\":\"" + nodes_[leader].id + "\",\"found\":" + (it == kvs.end() ? "false" : "true") +
           ",\"value\":\"" + (it == kvs.end() ? "" : JsonEscape(it->second)) + "\"}";
  }

  // ---- state for the UI ------------------------------------------------------

  std::string State() const {
    std::string out = "{\"now\":" + std::to_string(now_ms_) + ",\"nodes\":[";
    for (size_t i = 0; i < nodes_.size(); ++i) {
      if (i) out += ",";
      out += NodeJson(nodes_[i]);
    }
    out += "],\"msgs\":[";
    bool first = true;
    for (const auto& m : msgs_) {
      if (!first) out += ",";
      first = false;
      double span = std::max<double>(1, static_cast<double>(m.deliver - m.sent));
      double p = std::clamp((static_cast<double>(now_ms_) - m.sent) / span, 0.0, 1.0);
      out += "{\"from\":\"" + m.from + "\",\"to\":\"" + m.to + "\",\"kind\":\"" + m.kind + "\",\"p\":" + Num(p) +
             ",\"dropped\":" + (m.dropped ? "true" : "false") + ",\"done\":" + (m.done ? "true" : "false") + "}";
    }
    out += "],\"writes\":[";
    for (size_t i = 0; i < writes_.size(); ++i) {
      const auto& w = writes_[i];
      if (i) out += ",";
      out += "{\"key\":\"" + JsonEscape(w.key) + "\",\"value\":\"" + JsonEscape(w.value) + "\",\"node\":\"" + w.node +
             "\",\"index\":" + std::to_string(w.index) + ",\"term\":" + std::to_string(w.term) + ",\"status\":\"" +
             w.status + "\"}";
    }
    out += "],\"events\":[";
    for (size_t i = 0; i < events_.size(); ++i) {
      if (i) out += ",";
      out += "{\"t\":" + std::to_string(events_[i].first) + ",\"text\":\"" + JsonEscape(events_[i].second) + "\"}";
    }
    out += "],\"partitioned\":" + std::string(side_.empty() ? "false" : "true") + "}";
    return out;
  }

  // Introspection for tests.
  int CurrentLeader() const {
    int best = -1;
    uint64_t best_term = 0;
    for (size_t i = 0; i < nodes_.size(); ++i) {
      if (!nodes_[i].raft) continue;
      auto s = nodes_[i].raft->GetStatus();
      if (s.role == raft::Role::kLeader && s.term >= best_term) {
        best = static_cast<int>(i);
        best_term = s.term;
      }
    }
    return best;
  }
  const std::map<std::string, std::string>& Data(int i) const { return nodes_[i].machine->data; }
  raft::RaftStatus NodeStatus(int i) const { return nodes_[i].raft->GetStatus(); }
  bool Alive(int i) const { return nodes_[i].raft != nullptr; }
  std::string WriteStatus(size_t i) const { return i < writes_.size() ? writes_[i].status : ""; }
  uint64_t now_ms() const { return now_ms_; }

 private:
  struct KvMachine : raft::StateMachine {
    std::map<std::string, std::string> data;
    void Apply(uint64_t, const std::string& command) override {
      auto cmd = Command::Decode(command);
      if (!cmd) return;
      if (cmd->op == Command::Op::kPut) {
        data[cmd->key] = cmd->value;
      } else {
        data.erase(cmd->key);
      }
    }
  };

  struct Msg {
    std::string from, to, owner;  // owner: the node whose callback is pending
    std::string kind;             // vote, vote-ok, vote-no, append, append-ok, append-no
    uint64_t sent = 0, deliver = 0;
    bool dropped = false, done = false;
    std::function<bool()> arrive;  // runs at delivery time; false = lost on the way
    std::function<void()> fail;    // reports the RPC as failed to its owner
  };

  class Transport : public raft::Transport {
   public:
    Transport(Simulation* sim, std::string self) : sim_(sim), self_(std::move(self)) {}
    void RequestVote(const std::string& peer, const raft::RequestVoteRequest& req, VoteCallback cb) override {
      sim_->SendRequest<raft::RequestVoteRequest, raft::RequestVoteResponse>(
          self_, peer, "vote", req, [](raft::RaftNode& n, const auto& r) { return n.HandleRequestVote(r); },
          [](const raft::RequestVoteResponse& r) { return r.vote_granted ? "vote-ok" : "vote-no"; }, std::move(cb));
    }
    void AppendEntries(const std::string& peer, const raft::AppendEntriesRequest& req, AppendCallback cb) override {
      sim_->SendRequest<raft::AppendEntriesRequest, raft::AppendEntriesResponse>(
          self_, peer, req.entries.empty() ? "heartbeat" : "append", req,
          [](raft::RaftNode& n, const auto& r) { return n.HandleAppendEntries(r); },
          [](const raft::AppendEntriesResponse& r) { return r.success ? "append-ok" : "append-no"; }, std::move(cb));
    }

   private:
    Simulation* sim_;
    std::string self_;
  };

  struct Node {
    std::string id;
    std::unique_ptr<KvMachine> machine;
    std::unique_ptr<Transport> transport;
    std::unique_ptr<raft::RaftNode> raft;
    raft::Role last_role = raft::Role::kFollower;
    uint64_t last_term = 0;
  };

  struct Write {
    std::string key, value, node;
    uint64_t index, term;
    std::string status;  // pending, committed, lost
    uint64_t at;
  };

  void StartNode(int i) {
    Node& n = nodes_[i];
    raft::RaftConfig c;
    c.id = n.id;
    for (const auto& other : nodes_) {
      if (other.id != n.id) c.peers.push_back(other.id);
    }
    c.data_dir = opt_.data_root + "/" + n.id;
    c.election_timeout_min = std::chrono::milliseconds(opt_.election_min_ms);
    c.election_timeout_max = std::chrono::milliseconds(opt_.election_max_ms);
    c.heartbeat_interval = std::chrono::milliseconds(opt_.heartbeat_ms);
    c.sync_log = false;
    c.background_threads = false;
    c.clock = [this] { return Clock::time_point(std::chrono::milliseconds(now_ms_)); };
    n.machine = std::make_unique<KvMachine>();
    n.transport = std::make_unique<Transport>(this, n.id);
    n.raft = std::make_unique<raft::RaftNode>(c, n.transport.get(), n.machine.get());
    n.raft->Start();
    n.last_role = raft::Role::kFollower;
    n.last_term = n.raft->GetStatus().term;
  }

  Node* Find(const std::string& id) {
    for (auto& n : nodes_) {
      if (n.id == id) return &n;
    }
    return nullptr;
  }

  bool Linked(const std::string& a, const std::string& b) const {
    auto sa = side_.find(a), sb = side_.find(b);
    int x = sa == side_.end() ? 0 : sa->second;
    int y = sb == side_.end() ? 0 : sb->second;
    return x == y;
  }

  uint64_t Latency() { return std::uniform_int_distribution<int>(opt_.latency_min_ms, opt_.latency_max_ms)(rng_); }

  // Request travels from -> to; if it arrives, the reply travels back. Either
  // leg can be lost to a partition or a crashed node.
  template <typename Req, typename Resp, typename Handler, typename Label, typename Cb>
  void SendRequest(const std::string& from, const std::string& to, const std::string& kind, const Req& req,
                   Handler handler, Label label, Cb cb) {
    auto callback = std::make_shared<Cb>(std::move(cb));
    Msg m;
    m.from = from;
    m.to = to;
    m.owner = from;
    m.kind = kind;
    m.sent = now_ms_;
    m.deliver = now_ms_ + Latency();
    m.fail = [callback] { (*callback)(false, Resp{}); };
    m.arrive = [this, from, to, req, handler, label, callback]() {
      Node* target = Find(to);
      if (!target || !target->raft || !Linked(from, to)) {
        (*callback)(false, Resp{});
        return false;
      }
      Resp resp = handler(*target->raft, req);
      Msg reply;
      reply.from = to;
      reply.to = from;
      reply.owner = from;
      reply.kind = label(resp);
      reply.sent = now_ms_;
      reply.deliver = now_ms_ + Latency();
      reply.fail = [callback] { (*callback)(false, Resp{}); };
      reply.arrive = [this, from, to, resp, callback]() {
        Node* origin = Find(from);
        if (!origin || !origin->raft || !Linked(to, from)) {
          (*callback)(false, Resp{});
          return false;
        }
        (*callback)(true, resp);
        return true;
      };
      pending_.push_back(std::move(reply));
      return true;
    };
    pending_.push_back(std::move(m));
  }

  void Deliver() {
    // Messages created during this tick go into pending_ and join msgs_ after the scan.
    for (auto& m : pending_) msgs_.push_back(std::move(m));
    pending_.clear();
    for (size_t i = 0; i < msgs_.size(); ++i) {
      if (msgs_[i].done || msgs_[i].deliver > now_ms_) continue;
      msgs_[i].done = true;
      auto arrive = msgs_[i].arrive;  // copy: arrive() may grow pending_, never msgs_
      bool ok = arrive();
      msgs_[i].dropped = !ok;
    }
    for (auto& m : pending_) msgs_.push_back(std::move(m));
    pending_.clear();
  }

  void ObserveTransitions() {
    for (auto& n : nodes_) {
      if (!n.raft) continue;
      auto s = n.raft->GetStatus();
      if (s.role != n.last_role || s.term != n.last_term) {
        if (s.role == raft::Role::kCandidate && (n.last_role != raft::Role::kCandidate || s.term != n.last_term)) {
          Log(n.id + " heard nothing from a leader in time, so it started an election for term " +
              std::to_string(s.term) + ".");
        } else if (s.role == raft::Role::kLeader && n.last_role != raft::Role::kLeader) {
          Log(n.id + " won a majority of votes and is leader for term " + std::to_string(s.term) + ".");
        } else if (s.role == raft::Role::kFollower && n.last_role == raft::Role::kLeader) {
          Log(n.id + " saw a higher term (" + std::to_string(s.term) + ") and stepped down to follower.");
        }
        n.last_role = s.role;
        n.last_term = s.term;
      }
    }
  }

  void ResolveWrites() {
    for (auto& w : writes_) {
      if (w.status != "pending") continue;
      // The highest commit index any live node knows about decides the outcome.
      for (const auto& n : nodes_) {
        if (!n.raft) continue;
        auto s = n.raft->GetStatus();
        if (s.commit_index < w.index) continue;
        auto e = n.raft->LogSlice(w.index, 1);
        if (e.empty()) continue;
        if (e[0].term == w.term) {
          w.status = "committed";
          Log("PUT " + w.key + " committed: index " + std::to_string(w.index) +
              " is stored on a majority and will survive any minority failure.");
        } else {
          w.status = "lost";
          Log("PUT " + w.key + " was never committed. Index " + std::to_string(w.index) +
              " was overwritten by the new leader's log, so it was safe to discard.");
        }
        break;
      }
    }
  }

  void Log(const std::string& text) {
    events_.emplace_back(now_ms_, text);
    if (events_.size() > 40) events_.pop_front();
  }

  static std::string Num(double v) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "%.3f", v);
    return buf;
  }

  std::string NodeJson(const Node& n) const {
    std::string side = std::to_string(side_.count(n.id) ? side_.at(n.id) : 0);
    if (!n.raft) return "{\"id\":\"" + n.id + "\",\"alive\":false,\"side\":" + side + "}";
    auto s = n.raft->GetStatus();
    double timer = 0;
    if (s.role != raft::Role::kLeader) {
      double total = std::chrono::duration<double, std::milli>(s.election_deadline - s.election_timer_start).count();
      double done = static_cast<double>(now_ms_) -
                    std::chrono::duration<double, std::milli>(s.election_timer_start.time_since_epoch()).count();
      timer = total > 0 ? std::clamp(done / total, 0.0, 1.0) : 0;
    }
    std::string out = "{\"id\":\"" + n.id + "\",\"alive\":true,\"side\":" + side + ",\"role\":\"" +
                      raft::RoleName(s.role) + "\",\"term\":" + std::to_string(s.term) + ",\"leader\":\"" +
                      s.leader_id + "\",\"votedFor\":\"" + s.voted_for + "\",\"lastIndex\":" +
                      std::to_string(s.last_index) + ",\"commit\":" + std::to_string(s.commit_index) +
                      ",\"applied\":" + std::to_string(s.last_applied) + ",\"timer\":" + Num(timer) + ",\"log\":[";
    uint64_t from = s.last_index > 8 ? s.last_index - 7 : 1;
    auto entries = n.raft->LogSlice(from, 8);
    for (size_t i = 0; i < entries.size(); ++i) {
      if (i) out += ",";
      std::string label = "no-op";
      if (auto cmd = Command::Decode(entries[i].command)) {
        label = cmd->op == Command::Op::kPut ? cmd->key + "=" + cmd->value : "del " + cmd->key;
      }
      out += "{\"i\":" + std::to_string(entries[i].index) + ",\"t\":" + std::to_string(entries[i].term) +
             ",\"c\":\"" + JsonEscape(label) + "\"}";
    }
    out += "],\"kv\":{";
    size_t k = 0;
    for (const auto& [key, value] : n.machine->data) {
      if (k++ == 16) break;
      if (k > 1) out += ",";
      out += "\"" + JsonEscape(key) + "\":\"" + JsonEscape(value) + "\"";
    }
    return out + "}}";
  }

  Options opt_;
  std::mt19937 rng_;
  uint64_t now_ms_ = 0;
  std::vector<Node> nodes_;
  std::vector<Msg> msgs_;
  std::vector<Msg> pending_;
  std::map<std::string, int> side_;
  std::deque<Write> writes_;
  std::deque<std::pair<uint64_t, std::string>> events_;
};

}  // namespace kv::sim
