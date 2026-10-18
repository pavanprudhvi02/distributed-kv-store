// kvbench: closed-loop load generator.
//
//   kvbench --cluster localhost:7001,localhost:7002,localhost:7003
//           --threads 8 --ops 2000 --value-size 100 --read-ratio 0.5

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <random>
#include <sstream>
#include <thread>

#include "client/kv_client.h"

int main(int argc, char** argv) {
  std::vector<std::string> addresses;
  int threads = 4, ops = 1000, value_size = 100, keyspace = 10000;
  double read_ratio = 0.5;
  for (int i = 1; i + 1 < argc; i += 2) {
    std::string a = argv[i], v = argv[i + 1];
    if (a == "--cluster") {
      std::stringstream ss(v);
      std::string item;
      while (std::getline(ss, item, ',')) {
        auto eq = item.find('=');
        addresses.push_back(eq == std::string::npos ? item : item.substr(eq + 1));
      }
    } else if (a == "--threads") {
      threads = std::stoi(v);
    } else if (a == "--ops") {
      ops = std::stoi(v);
    } else if (a == "--value-size") {
      value_size = std::stoi(v);
    } else if (a == "--read-ratio") {
      read_ratio = std::stod(v);
    } else if (a == "--keys") {
      keyspace = std::stoi(v);
    }
  }
  if (addresses.empty()) {
    std::cerr << "usage: kvbench --cluster HOST:PORT,... [--threads N] [--ops N per thread] "
                 "[--value-size BYTES] [--read-ratio 0..1] [--keys N]\n";
    return 2;
  }

  std::vector<std::vector<double>> latencies(threads);
  std::atomic<int> errors{0};
  auto start = std::chrono::steady_clock::now();
  std::vector<std::thread> workers;
  for (int t = 0; t < threads; ++t) {
    workers.emplace_back([&, t] {
      kv::KvClient client(addresses);
      if (!client.Connect().ok()) {
        errors += ops;
        return;
      }
      std::mt19937 rng(t + 1);
      std::string value(value_size, 'x');
      latencies[t].reserve(ops);
      for (int i = 0; i < ops; ++i) {
        std::string key = "bench:" + std::to_string(rng() % keyspace);
        auto t0 = std::chrono::steady_clock::now();
        kv::Status s;
        if (std::uniform_real_distribution<double>(0, 1)(rng) < read_ratio) {
          std::string out;
          s = client.Get(key, &out);
          if (s.IsNotFound()) s = kv::Status::Ok();
        } else {
          s = client.Put(key, value);
        }
        auto t1 = std::chrono::steady_clock::now();
        if (!s.ok()) ++errors;
        latencies[t].push_back(std::chrono::duration<double, std::milli>(t1 - t0).count());
      }
    });
  }
  for (auto& w : workers) w.join();
  double seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

  std::vector<double> all;
  for (auto& l : latencies) all.insert(all.end(), l.begin(), l.end());
  std::sort(all.begin(), all.end());
  auto pct = [&](double p) { return all.empty() ? 0.0 : all[std::min(all.size() - 1, static_cast<size_t>(p * all.size()))]; };
  std::cout << "ops: " << all.size() << "  errors: " << errors << "  time: " << seconds << " s\n"
            << "throughput: " << static_cast<long>(all.size() / seconds) << " ops/s\n"
            << "latency ms  p50 " << pct(0.50) << "  p95 " << pct(0.95) << "  p99 " << pct(0.99) << "\n";
  return errors > 0 ? 1 : 0;
}
