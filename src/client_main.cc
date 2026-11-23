#include "lock/bench/client.h"
#include "lock/config.h"
#include "lock/locks/backend.h"
#include "lock/rdma/transport.h"
#include <iostream>
#include <limits>
#include <stdexcept>
namespace {
std::vector<lock::NodeId> ids(const std::vector<std::string>& list) {
    std::vector<lock::NodeId> result;
    for (const auto& v : list) {
        auto id = std::stoul(v);
        if (id == 0 || id > 255) throw std::invalid_argument("invalid node ID");
        result.push_back(id);
    }
    return result;
}
}
int main(int argc, char** argv) {
    try {
        lock::Options opt(argc, argv);
        lock::rdma::Config net_cfg;
        net_cfg.self = opt.number("node-id", 0);
        net_cfg.peers = lock::rdma::parse_peers(opt.require("peers"));
        net_cfg.tcp_port = opt.number("tcp-port", 18515);
        net_cfg.ib_port = opt.number("ib-port", 1);
        net_cfg.device = opt.get("device");
        net_cfg.memory_bytes = opt.number("memory-mib", 128) * 1024ULL * 1024;
        lock::ClientConfig cfg;
        cfg.self = net_cfg.self;
        cfg.replicas = ids(opt.list("replicas"));
        cfg.backend = lock::parse_backend(opt.require("backend"));
        cfg.lock_count = opt.number("locks", 1000);
        cfg.outstanding = opt.number("outstanding", 1);
        cfg.distribution = opt.get("distribution", "uniform");
        cfg.seed = opt.number("seed", 2026);
        cfg.warmup_seconds = opt.number("warmup-seconds", 5);
        cfg.duration_seconds = opt.number("duration-seconds", 15);
        cfg.watchdog_seconds = opt.number("watchdog-seconds", 90);
        cfg.cas_release_guarded = opt.number("cas-release-guarded", 0) != 0;
        lock::rdma::Transport net(net_cfg);
        net.connect();
        std::cout << "READY node=" << cfg.self << std::endl;
        lock::ClientEngine engine(net, cfg);
        engine.run();
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "ERROR " << e.what() << std::endl;
        return 1;
    }
}
