// Emscripten entry point: exposes the Raft cluster simulation to JavaScript.
#include <emscripten/bind.h>

#include "raft_sim.h"

using kv::sim::Simulation;

EMSCRIPTEN_BINDINGS(raft_sim) {
  emscripten::class_<Simulation>("Simulation")
      .constructor<>()
      .function("step", &Simulation::Step)
      .function("put", &Simulation::Put)
      .function("get", &Simulation::Get)
      .function("crash", &Simulation::Crash)
      .function("restartNode", &Simulation::RestartNode)
      .function("isolate", &Simulation::Isolate)
      .function("partitionLeader", &Simulation::PartitionLeader)
      .function("heal", &Simulation::Heal)
      .function("leader", &Simulation::CurrentLeader)
      .function("state", &Simulation::State);
}
