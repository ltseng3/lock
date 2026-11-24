#include "lock/locks/mu_recovery.h"
#include <stdexcept>
using namespace lock;
void check(bool ok) { if (!ok) throw std::runtime_error("Mu recovery test"); }
template<class F> void rejects(F f) {
    bool failed = false; try { f(); } catch (const std::exception&) { failed = true; }
    check(failed);
}
int main() {
    check(safe_replay_prefix(1, 1) == 0);
    check(safe_replay_prefix(3, 2) == 1);
    check(safe_replay_prefix(100, 512) == 0);
    check(safe_replay_prefix(1000, 512) == 488);
    MuRecoveryState s(2, 1000);
    const auto a = make_request_id(4, 1), b = make_request_id(5, 1);
    const auto c = make_request_id(4, 2);
    s.apply(message(Kind::Acquire, 4, 0, a));
    s.apply(message(Kind::Acquire, 5, 0, b));
    check(s.lookup(a).status == MuRequestStatus::Granted);
    check(s.lookup(b).status == MuRequestStatus::Waiting);
    check(s.active() == 2);
    s.apply(message(Kind::Release, 4, 0, a));
    check(s.lookup(a).status == MuRequestStatus::Released);
    check(s.lookup(b).status == MuRequestStatus::Granted);
    check(s.applied() == 3 && s.active() == 1);
    check(reconcile_mu(s.lookup(a), 0, true) == MuRecoveryAction::Ack);
    check(reconcile_mu(s.lookup(b), 0, false) == MuRecoveryAction::Grant);
    check(reconcile_mu(s.lookup(b), 0, true) == MuRecoveryAction::RetryRelease);
    check(reconcile_mu(s.lookup(c), 1, false) == MuRecoveryAction::RetryAcquire);
    rejects([&] { reconcile_mu(s.lookup(a), 0, false); });
    rejects([&] { reconcile_mu(s.lookup(b), 1, true); });
    rejects([&] { s.apply(message(Kind::Acquire, 5, 0, b)); });
    auto state = s.take_state();
    auto actions = state.apply(message(Kind::Release, 5, 0, b));
    check(actions.size() == 1 && message_kind(actions[0].message) == Kind::ReleaseAck);
}
