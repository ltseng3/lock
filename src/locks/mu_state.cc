#include "lock/locks/mu_state.h"
#include <stdexcept>
namespace lock {
MuState::MuState(size_t locks) : locks_(locks) {
    if (locks == 0) throw std::invalid_argument("need at least one lock");
}
std::vector<Action> MuState::apply(const Message& committed) {
    if (committed.lock_id >= locks_.size() || committed.request == 0)
        throw std::runtime_error("invalid Mu command");
    auto& lock = locks_[committed.lock_id];
    const Kind kind = message_kind(committed);
    std::vector<Action> actions;
    if (kind == Kind::Acquire) {
        if (lock.holder == 0) {
            lock.holder = committed.request;
            actions.push_back({request_node(lock.holder),
                message(Kind::Grant, 0, committed.lock_id, lock.holder)});
        } else {
            lock.waiting.push_back(committed.request);
        }
    } else if (kind == Kind::Release) {
        if (lock.holder != committed.request)
            throw std::runtime_error("Mu unlock from non-holder");
        actions.push_back({request_node(committed.request),
            message(Kind::ReleaseAck, 0, committed.lock_id, committed.request)});
        lock.holder = 0;
        if (!lock.waiting.empty()) {
            lock.holder = lock.waiting.front();
            lock.waiting.pop_front();
            actions.push_back({request_node(lock.holder),
                message(Kind::Grant, 0, committed.lock_id, lock.holder)});
        }
    } else {
        throw std::runtime_error("Mu state received non-mutation");
    }
    return actions;
}
} // namespace lock
