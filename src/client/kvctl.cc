// kvctl: command-line client.
//
//   kvctl --cluster localhost:7001,localhost:7002,localhost:7003 put user:1 ada
//   kvctl --cluster ... get user:1
//   kvctl --cluster ... del user:1
//   kvctl --cluster ... status

#include <iomanip>
#include <iostream>
#include <sstream>

#include "client/kv_client.h"

namespace {

int Usage() {
  std::cerr << "usage: kvctl --cluster HOST:PORT[,HOST:PORT...] (put KEY VALUE | get KEY | del KEY | status)\n";
  return 2;
}

std::vector<std::string> Split(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, ',')) {
    // Accept both "host:port" and "id=host:port", the format kvd uses.
    auto eq = item.find('=');
    if (eq != std::string::npos) item = item.substr(eq + 1);
    if (!item.empty()) out.push_back(item);
  }
  return out;
}

}  // namespace

int main(int argc, char** argv) {
  std::vector<std::string> addresses;
  std::vector<std::string> args;
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--cluster" && i + 1 < argc) {
      addresses = Split(argv[++i]);
    } else {
      args.push_back(a);
    }
  }
  if (addresses.empty() || args.empty()) return Usage();

  kv::KvClient client(addresses);
  const std::string& cmd = args[0];

  if (cmd == "status") {
    for (const auto& addr : addresses) {
      kvstore::StatusResponse resp;
      kv::Status s = client.ClusterStatus(addr, &resp);
      if (!s.ok()) {
        std::cout << addr << "  unreachable\n";
        continue;
      }
      std::cout << addr << "  node " << resp.node_id() << "\n";
      for (const auto& sh : resp.shards()) {
        std::cout << "  shard " << sh.shard() << "  " << std::left << std::setw(9) << sh.role() << " term "
                  << sh.term() << "  leader " << (sh.leader_id().empty() ? "?" : sh.leader_id()) << "  commit "
                  << sh.commit_index() << "  applied " << sh.last_applied() << "\n";
      }
    }
    return 0;
  }

  kv::Status s = client.Connect();
  if (!s.ok()) {
    std::cerr << "cannot reach cluster: " << s.ToString() << "\n";
    return 1;
  }

  if (cmd == "put" && args.size() == 3) {
    s = client.Put(args[1], args[2]);
    if (s.ok()) std::cout << "OK\n";
  } else if (cmd == "get" && args.size() == 2) {
    std::string value;
    s = client.Get(args[1], &value);
    if (s.ok()) std::cout << value << "\n";
    if (s.IsNotFound()) {
      std::cout << "(not found)\n";
      return 3;
    }
  } else if (cmd == "del" && args.size() == 2) {
    s = client.Delete(args[1]);
    if (s.ok()) std::cout << "OK\n";
  } else {
    return Usage();
  }
  if (!s.ok()) {
    std::cerr << s.ToString() << "\n";
    return 1;
  }
  return 0;
}
