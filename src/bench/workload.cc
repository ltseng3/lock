#include "lock/bench/workload.h"
#include <cmath>
#include <stdexcept>
#include <vector>
namespace lock {
LockPicker::LockPicker(size_t locks, std::string distribution, uint64_t seed)
    : rng_(seed), uniform_(0, static_cast<uint32_t>(locks - 1)) {
    if (locks == 0 || locks > 10000000)
        throw std::invalid_argument("invalid lock count");
    double skew = 0.0;
    if (distribution == "zipf_0.5") skew = 0.5;
    else if (distribution == "zipf_0.99") skew = 0.99;
    else if (distribution != "uniform")
        throw std::invalid_argument("invalid lock distribution");
    if (skew > 0) {
        use_zipf_ = true;
        std::vector<double> weights(locks);
        for (size_t i = 0; i < locks; ++i)
            weights[i] = 1.0 / std::pow(static_cast<double>(i + 1), skew);
        zipf_ = std::discrete_distribution<uint32_t>(weights.begin(), weights.end());
    }
}
LockId LockPicker::next() { return use_zipf_ ? zipf_(rng_) : uniform_(rng_); }
} // namespace lock
