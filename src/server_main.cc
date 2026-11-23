#include "lock/config.h"
#include "lock/locks/backend.h"
#include "lock/locks/handoff_state.h"
#include <atomic>
#include <cstddef>
#include "lock/locks/mu_state.h"
#include "lock/locks/replicated_log.h"
#include "lock/rdma/transport.h"
#include <algorithm>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <vector>

namespace {
std::vector<lock::NodeId> ids(const std::vector<std::string>& list) {
    std::vector<lock::NodeId> out;
    for (const auto& item : list) {
        auto id = std::stoul(item);
        if (id == 0 || id > 255) throw std::invalid_argument("invalid node ID");
        out.push_back(id);
    }
    return out;
}
}
int main(int argc, char** argv) {
    try {
        lock::Options opt(argc, argv);
        lock::rdma::Config cfg;
        cfg.self = opt.number("node-id", 0);
        cfg.peers = lock::rdma::parse_peers(opt.require("peers"));
        cfg.tcp_port = opt.number("tcp-port", 18515);
        cfg.ib_port = opt.number("ib-port", 1);
        cfg.device = opt.get("device");
        cfg.memory_bytes = opt.number("memory-mib", 512) * 1024ULL * 1024;
        auto replicas = ids(opt.list("replicas"));
        auto clients = ids(opt.list("clients"));
        auto backend = lock::parse_backend(opt.require("backend"));
        const size_t lock_count = opt.number("locks", 1000);
        if (std::find(replicas.begin(), replicas.end(), cfg.self) == replicas.end())
            throw std::invalid_argument("server node is not a replica");
        if (opt.number("mu-lanes", 1) != 1)
            throw std::invalid_argument("only one replicated log lane supported");
        lock::rdma::Transport net(cfg);
        net.connect();
        std::cout << "READY node=" << cfg.self << std::endl;
        bool leader = cfg.self == replicas.front();
        std::set<lock::NodeId> ready;
        std::set<lock::NodeId> finished;
        bool started = false;
        std::unique_ptr<lock::ReplicatedLog> log;
        std::unique_ptr<lock::MuState> mu;
        std::unique_ptr<lock::HandoffState> handoff;
        if (leader && backend != lock::Backend::Cas) {
            log = std::make_unique<lock::ReplicatedLog>(net, replicas);
            if (backend == lock::Backend::Mu)
                mu = std::make_unique<lock::MuState>(lock_count);
            else
                handoff = std::make_unique<lock::HandoffState>(
                    lock_count, opt.number("prepared-window", 8));
        }
        std::vector<lock::Message> pending;
        size_t head = 0;
        while (true) {
            for (auto& event : net.poll()) {
                if (event.kind == lock::rdma::CompletionKind::Write) {
                    if (!leader || !log) throw std::runtime_error("unexpected server WRITE CQE");
                    log->on_write_complete(event.work_id);
                    continue;
                }
                if (event.kind != lock::rdma::CompletionKind::Receive) continue;
                const auto& m = event.message;
                if (m.sender != event.peer) throw std::runtime_error("spoofed sender ID");
                auto kind = lock::message_kind(m);
                if (!leader) throw std::runtime_error("unexpected follower RPC");
                if (kind == lock::Kind::Ready) {
                    if (std::find(clients.begin(), clients.end(), event.peer) == clients.end())
                        throw std::runtime_error("non-client readiness");
                    ready.insert(event.peer);
                    if (ready.size() == clients.size() && !started) {
                        started = true;
                        for (auto peer : clients)
                            net.send(peer, lock::message(lock::Kind::Ready, cfg.self));
                    }
                } else if (kind == lock::Kind::Stop && started) {
                    if (std::find(clients.begin(), clients.end(), event.peer) == clients.end() ||
                        !finished.insert(event.peer).second)
                        throw std::runtime_error("invalid or repeated client shutdown");
                    if (finished.size() == clients.size()) {
                        // Client Stop is sent only after all its active operations
                        // and one-sided release writes have finished. The leader
                        // has already acknowledged every retired position.
                        for (auto peer : clients)
                            net.send(peer, lock::message(lock::Kind::Stop, cfg.self));
                    }
                } else if ((kind == lock::Kind::Acquire || kind == lock::Kind::Release) &&
                           started && backend != lock::Backend::Cas) {
                    if (std::find(clients.begin(), clients.end(), event.peer) == clients.end() ||
                        lock::request_node(m.request) != event.peer ||
                        m.lock_id >= lock_count)
                        throw std::runtime_error("invalid mutation request");
                    if (handoff) {
                        if (kind != lock::Kind::Acquire)
                            throw std::runtime_error("handoff accepts only Acquire");
                        auto step = handoff->admit(m);
                        for (auto& command : step.append) pending.push_back(command);
                    } else {
                        pending.push_back(m);
                    }
                } else {
                    throw std::runtime_error("unexpected leader request");
                }
            }
            if (!leader || !log) continue;
            // Cap work on one RC send queue while allowing pipelined quorum commits.
            while (head < pending.size() && log->inflight() < 128) {
                log->append(pending[head++]);
            }
            if (head == pending.size()) { pending.clear(); head = 0; }
            while (auto committed = log->next_committed()) {
                if (mu) for (auto& action : mu->apply(committed->command)) {
                    action.message.sender = cfg.self;
                    net.send(action.destination, action.message);
                }
                if (handoff) for (auto& action : handoff->committed(*committed)) {
                    action.message.sender = cfg.self;
                    net.send(action.destination, action.message);
                }
            }
            if (handoff) {
                auto step = handoff->retire([&](uint64_t slot) -> uint64_t {
                    size_t offset = lock::rdma::kLogOffset +
                                    slot * sizeof(lock::LogRecord) +
                                    offsetof(lock::LogRecord, retired);
                    auto* ptr = static_cast<uint64_t*>(net.at(offset, sizeof(uint64_t)));
                    return std::atomic_ref<uint64_t>(*ptr).load(std::memory_order_acquire);
                });
                for (auto& action : step.actions) {
                    action.message.sender = cfg.self;
                    net.send(action.destination, action.message);
                }
                for (auto& command : step.append) pending.push_back(command);
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "ERROR " << e.what() << std::endl;
        return 1;
    }
}
