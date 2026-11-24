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
        cfg.mu_lanes = opt.number("mu-lanes", 1);
        if (cfg.mu_lanes < 1 || cfg.mu_lanes > 8)
            throw std::invalid_argument("mu-lanes must be 1..8");
        if (cfg.backend == lock::Backend::Cas && cfg.mu_lanes != 1)
            throw std::invalid_argument("CAS does not use Mu lanes");
        if (cfg.backend != lock::Backend::Cas) {
            net_cfg.replication_lanes = cfg.mu_lanes;
            net_cfg.client_lane_peers = {cfg.replicas.front()};
        }
        cfg.lock_count = opt.number("locks", 1000);
        const auto recovery = opt.number("mu-recovery", 0);
        if (recovery > 1 || (recovery && (cfg.backend != lock::Backend::Mu ||
                                        cfg.replicas.size() != 3 || cfg.mu_lanes != 1)))
            throw std::invalid_argument("invalid Mu client recovery mode");
        cfg.mu_recovery = recovery != 0;
        if (cfg.mu_recovery) {
            net_cfg.recoverable_peer = cfg.replicas.front();
            net_cfg.client_lane_peers.push_back(cfg.replicas[1]);
        }
        cfg.critical_section_ops = opt.number("critical-section-ops", 1);
        if (cfg.critical_section_ops > 1)
            throw std::invalid_argument("critical-section-ops must be 0 or 1");
        cfg.outstanding = opt.number("outstanding", 1);
        cfg.workers = opt.number("workers", 1);
        cfg.distribution = opt.get("distribution", "uniform");
        cfg.seed = opt.number("seed", 2026);
        cfg.warmup_seconds = opt.number("warmup-seconds", 5);
        cfg.duration_seconds = opt.number("duration-seconds", 15);
        cfg.latency_sample_capacity = lock::LatencySamples::capacity_for(
            cfg.duration_seconds, opt.number("latency-sample-capacity", 0));
        cfg.watchdog_seconds = opt.number("watchdog-seconds", 90);
        cfg.cas_release_guarded = opt.number("cas-release-guarded", 0) != 0;
        cfg.handoff_peer_read = opt.number("handoff-peer-read", 0) != 0;
        const auto gc = opt.number("handoff-gc", 0);
        if (gc > 1 || (gc && cfg.backend != lock::Backend::Handoff))
            throw std::invalid_argument("handoff-gc requires handoff and 0 or 1");
        cfg.handoff_gc = gc != 0;
        if (cfg.handoff_peer_read && cfg.backend != lock::Backend::Handoff)
            throw std::invalid_argument("peer READ is only supported by handoff");
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
