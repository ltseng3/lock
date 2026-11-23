#include "lock/config.h"
#include "lock/rdma/transport.h"
#include <chrono>
#include <iostream>
#include <stdexcept>
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
        for (auto& peer : cfg.peers) {
            if (peer.id == cfg.self) continue;
            net.send(peer.id, lock::message(lock::Kind::Ready, cfg.self));
        }
        size_t received = 0;
        const size_t wanted = cfg.peers.size() - 1;
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(15);
        while (received < wanted && std::chrono::steady_clock::now() < deadline) {
            for (auto& event : net.poll()) {
                if (event.kind == lock::rdma::CompletionKind::Receive) {
                    if (lock::message_kind(event.message) != lock::Kind::Ready ||
                        event.message.sender != event.peer)
                        throw std::runtime_error("unexpected smoke message");
                    ++received;
                }
            }
        }
        if (received != wanted) throw std::runtime_error("smoke barrier timed out");
        std::cout << "RDMA_SMOKE_OK node=" << net.self() << std::endl;
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "ERROR: " << ex.what() << std::endl;
        return 1;
    }
}
