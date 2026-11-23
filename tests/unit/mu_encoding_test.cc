#include "lock/locks/mu_encoding.h"
#include "lock/locks/spsc_queue.h"
#include "lock/locks/mu_state.h"
#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <thread>

int main() {
    using namespace lock;
    for (NodeId node : {1u, 4u, 255u}) {
        for (LockId lock : {0u, 999u, 65535u}) {
            for (uint64_t seq : {1ULL, 1400000ULL, kMuSequenceMask}) {
                for (Kind kind : {Kind::Acquire, Kind::Release}) {
                    auto m = message(kind, node, lock, make_request_id(node, seq));
                    const auto encoded = pack_mu_command(m);
                    auto restored = unpack_mu_command(encoded);
                    assert(restored.request == m.request);
                    assert(restored.kind == m.kind);
                    assert(restored.sender == m.sender);
                    assert(restored.lock_id == m.lock_id);
                    assert(pack_mu_command(restored) == encoded);
                }
            }
        }
    }
    auto fails = [](const Message& m) {
        try { (void)pack_mu_command(m); } catch (const std::invalid_argument&) {
            return true;
        }
        return false;
    };
    assert(fails(message(Kind::Acquire, 4, 0, make_request_id(4, 1ULL << 39))));
    assert(fails(message(Kind::Acquire, 4, 65536, make_request_id(4, 1))));
    assert(fails(message(Kind::Acquire, 256, 1, make_request_id(256, 1))));
    assert(fails(message(Kind::Release, 4, 1, make_request_id(5, 1))));
    assert(fails(message(Kind::Grant, 4, 1, make_request_id(4, 1))));
    bool bad_word = false;
    try { (void)unpack_mu_command(0); }
    catch (const std::invalid_argument&) { bad_word = true; }
    assert(bad_word);

    // Exercise concurrent producer and consumer, wraparound and backpressure.
    SpscQueue<uint64_t, 8> queue;
    for (uint64_t i=0; i<8; ++i) assert(queue.try_push(i));
    assert(!queue.try_push(8));
    for (uint64_t i=0; i<8; ++i) {
        uint64_t got=99;
        assert(queue.try_pop(got));
        assert(got==i);
    }
    uint64_t got=0;
    assert(!queue.try_pop(got));
    constexpr uint64_t N=100000;
    std::thread producer([&] {
        for (uint64_t i=1; i<=N; ++i)
            while (!queue.try_push(i)) std::this_thread::yield();
    });
    for (uint64_t i=1; i<=N; ++i) {
        while (!queue.try_pop(got)) std::this_thread::yield();
        assert(got==i);
    }
    producer.join();
    assert(queue.size()==0);
}
