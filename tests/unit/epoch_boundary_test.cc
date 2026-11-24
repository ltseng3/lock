#include "lock/locks/epoch_boundary.h"
#include <array>
#include <stdexcept>
using namespace lock;
void check(bool ok) { if (!ok) throw std::runtime_error("epoch test"); }
template<class F> void rejects(F f) {
    bool failed = false;
    try { f(); } catch (const std::exception&) { failed = true; }
    check(failed);
}
int main() {
    const auto a = encode_epoch_boundary({1, 100});
    const auto b = encode_epoch_boundary({2, 100});
    const auto c = encode_epoch_boundary({3, 300});
    const std::array history{a,b,c};
    check(epoch_at(history, 99) == 0);
    check(epoch_at(history, 100) == 2);
    check(epoch_at(history, 299) == 2);
    check(epoch_at(history, 300) == 3);
    check(epoch_at({}, 0) == 0);
    check(decode_epoch_boundary(a).first_slot == 100);
    rejects([] { encode_epoch_boundary({0, 100}); });
    rejects([] { encode_epoch_boundary({1, 1ULL << 48}); });
    rejects([&] { epoch_at(std::array{a,c}, 0); });
    rejects([&] { epoch_at(std::array{a,encode_epoch_boundary({2,99})}, 0); });
    const size_t memory = 512ULL * 1024 * 1024;
    check(epoch_boundary_offset(memory,1) == epoch_boundary_base(memory));
    check(epoch_boundary_offset(memory,kEpochBoundaryRecords) + 8 ==
          memory - kRecoveryScratchBytes);
    rejects([&] { epoch_boundary_offset(memory,0); });
    auto m = message(Kind::Handoff, 4, 5, make_request_id(5,7), 1);
    check(sizeof(m) == 64 && m.epoch == 1 && message_kind(m) == Kind::Handoff);
}
