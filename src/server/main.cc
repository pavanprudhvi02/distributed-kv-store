// kvd: one storage node.
//
//   kvd --id n1 --listen 0.0.0.0:7001
//       --cluster n1=localhost:7001,n2=localhost:7002,n3=localhost:7003
//       --shards 4 --data ./data/n1

#include <csignal>
#include <iostream>
#include <sstream>
#include <thread>

#include "server/kv_service.h"

namespace {

std::atomic<bool> g_stop{false};

void Usage() {
  std::cerr << "usage: kvd --id ID --listen HOST:PORT --cluster ID=HOST:PORT,... "
               "[--shards N] [--data DIR] [--no-sync]\n";
}

bool ParseCluster(const std::string& spec, std::map<std::string, std::string>* out) {
  std::stringstream ss(spec);
  std::string item;
  while (std::getline(ss, item, ',')) {
    auto eq = item.find('=');
    if (eq == std::string::npos || eq == 0 || eq + 1 == item.size()) return false;
    (*out)[item.substr(0, eq)] = item.substr(eq + 1);
  }
  return !out->empty();
}

}  // namespace

int main(int argc, char** argv) {
  kv::NodeConfig config;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto value = [&]() -> std::string { return i + 1 < argc ? argv[++i] : ""; };
    if (arg == "--id") {
      config.id = value();
    } else if (arg == "--listen") {
      config.listen_address = value();
    } else if (arg == "--cluster") {
      if (!ParseCluster(value(), &config.cluster)) {
        Usage();
        return 2;
      }
    } else if (arg == "--shards") {
      config.shards = static_cast<uint32_t>(std::stoul(value()));
    } else if (arg == "--data") {
      config.data_dir = value();
    } else if (arg == "--no-sync") {
      config.sync = false;
    } else {
      Usage();
      return 2;
    }
  }
  if (config.id.empty() || config.listen_address.empty() || !config.cluster.count(config.id) || config.shards == 0) {
    Usage();
    return 2;
  }
  if (config.data_dir.empty()) config.data_dir = "./data/" + config.id;

  kv::KvNode node(config);
  kv::Status s = node.Start();
  if (!s.ok()) {
    std::cerr << "kvd: " << s.ToString() << "\n";
    return 1;
  }
  std::cout << "kvd " << config.id << " listening on " << config.listen_address << " with " << config.shards
            << " shards, data in " << config.data_dir << std::endl;

  std::signal(SIGINT, [](int) { g_stop = true; });
  std::signal(SIGTERM, [](int) { g_stop = true; });
  while (!g_stop) std::this_thread::sleep_for(std::chrono::milliseconds(100));

  std::cout << "kvd " << config.id << " shutting down" << std::endl;
  node.Shutdown();
  return 0;
}
