#include "lock/config.h"
#include "lock/rdma/transport.h"
#include <chrono>
#include <iostream>
#include <set>
#include <stdexcept>

namespace {
using Clock = std::chrono::steady_clock;
void deadline_check(Clock::time_point deadline) {
    if (Clock::now() > deadline)
        throw std::runtime_error("RDMA smoke timed out: fail closed");
}
}
int main(int argc, char** argv) {
    try {
        lock::Options opts(argc, argv);
        lock::rdma::Config cfg;
        cfg.self = opts.number("node-id", 0);
        cfg.peers = lock::rdma::parse_peers(opts.require("peers"));
        cfg.tcp_port = opts.number("tcp-port", 18515);
        cfg.device = opts.get("device");
        cfg.ib_port = opts.number("ib-port", 1);
        lock::rdma::Transport net(cfg);
        net.connect();
        std::cout << "CONNECTED node=" << net.self() << std::endl;
        const auto leader = cfg.peers.front().id;
        std::set<lock::NodeId> ready;
        std::set<lock::NodeId> completed;
        const size_t peers = cfg.peers.size() - 1;
        for (auto& peer : cfg.peers) {
            if (peer.id != cfg.self)
                net.send(peer.id, lock::message(lock::Kind::Ready, cfg.self));
        }
        auto deadline = Clock::now() + std::chrono::seconds(60);
        while (ready.size() < peers) {
            deadline_check(deadline);
            for (const auto& event : net.poll()) {
                if (event.kind != lock::rdma::CompletionKind::Receive) continue;
                if (event.message.sender != event.peer)
                    throw std::runtime_error("smoke sender mismatch");
                auto kind = lock::message_kind(event.message);
                if (kind == lock::Kind::Ready) {
                    ready.insert(event.peer);
                } else if (cfg.self == leader && kind == lock::Kind::Release) {
                    completed.insert(event.peer); // May race the all-peer READY barrier.
                } else {
                    throw std::runtime_error("unexpected smoke handshake message");
                }
            }
        }
        if (cfg.self != leader) {
            auto* value = static_cast<uint64_t*>(net.allocate(sizeof(uint64_t), 8));
            auto* cas_result = static_cast<uint64_t*>(net.allocate(sizeof(uint64_t), 8));
            *value = (static_cast<uint64_t>(cfg.self) << 32) | 0xabcULL;
            *cas_result = UINT64_MAX;
            net.write(leader, value, sizeof(uint64_t),
                      lock::rdma::kDataOffset + cfg.self * sizeof(uint64_t), 1);
            net.compare_swap(leader, cas_result,
                             lock::rdma::kFlagOffset + cfg.self * sizeof(uint64_t),
                             0, cfg.self, 2);
            bool write_ok = false, cas_ok = false;
            while (!write_ok || !cas_ok) {
                deadline_check(deadline);
                for (const auto& event : net.poll()) {
                    if (event.kind == lock::rdma::CompletionKind::Write &&
                        event.work_id == 1) write_ok = true;
                    else if (event.kind == lock::rdma::CompletionKind::CompareSwap &&
                             event.work_id == 2) cas_ok = true;
                    else if (event.kind == lock::rdma::CompletionKind::Receive)
                        throw std::runtime_error("unexpected message before smoke writes");
                }
            }
            if (*cas_result != 0)
                throw std::runtime_error("remote CAS did not observe the initial zero");
            net.send(leader, lock::message(lock::Kind::Release, cfg.self), 3);
            bool acknowledged = false;
            while (!acknowledged) {
                deadline_check(deadline);
                for (const auto& event : net.poll()) {
                    if (event.kind != lock::rdma::CompletionKind::Receive) continue;
                    if (event.peer != leader || event.message.sender != leader ||
                        lock::message_kind(event.message) != lock::Kind::Grant)
                        throw std::runtime_error("invalid smoke acknowledgment");
                    acknowledged = true;
                }
            }
        } else {
            while (completed.size() < peers) {
                deadline_check(deadline);
                for (const auto& event : net.poll()) {
                    if (event.kind != lock::rdma::CompletionKind::Receive) continue;
                    if (event.message.sender != event.peer ||
                        lock::message_kind(event.message) != lock::Kind::Release)
                        throw std::runtime_error("invalid smoke completion");
                    completed.insert(event.peer);
                }
            }
            for (auto peer : completed) {
                auto* value = static_cast<uint64_t*>(net.at(
                    lock::rdma::kDataOffset + peer * sizeof(uint64_t), sizeof(uint64_t)));
                auto* flag = static_cast<uint64_t*>(net.at(
                    lock::rdma::kFlagOffset + peer * sizeof(uint64_t), sizeof(uint64_t)));
                if (*value != ((static_cast<uint64_t>(peer) << 32) | 0xabcULL) ||
                    *flag != peer)
                    throw std::runtime_error("remote WRITE/CAS content mismatch");
                net.send(peer, lock::message(lock::Kind::Grant, cfg.self), peer);
            }
            size_t acknowledged_sends = 0;
            while (acknowledged_sends < peers) {
                deadline_check(deadline);
                for (const auto& event : net.poll())
                    if (event.kind == lock::rdma::CompletionKind::Send &&
                        event.work_id != 0) ++acknowledged_sends;
            }
        }
        std::cout << "RDMA_SMOKE_OK node=" << net.self() << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "ERROR " << ex.what() << std::endl;
        return 1;
    }
}
