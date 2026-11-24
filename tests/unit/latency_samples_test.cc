#include "lock/bench/latency_samples.h"
#include <stdexcept>
using namespace lock;
void check(bool ok) {
    if (!ok) throw std::runtime_error("latency samples test failed");
}
template<class F> void rejects(F f) {
    bool failed = false;
    try { f(); } catch (const std::exception&) { failed = true; }
    check(failed);
}
int main() {
    check(LatencySamples::capacity_for(12) == 18000000);
    check(LatencySamples::capacity_for(12, 8000000) == 8000000);
    rejects([] { LatencySamples::capacity_for(0); });
    rejects([] { LatencySamples::capacity_for(100); });
    rejects([] { LatencySamples::capacity_for(1, 64000001); });
    LatencySamples samples;
    rejects([&] { samples.record(1, 2); });
    samples.prepare(5);
    check(samples.capacity() == 5 && samples.size() == 0);
    check(samples.summarize().acquire.p50 == 0);
    rejects([&] { samples.prepare(2); });
    samples.record(30, 100);
    samples.record(10, 200);
    samples.record(20, 300);
    samples.record(40, 400);
    // Spare initialized zeros must never enter quantile calculations.
    auto q = samples.summarize();
    check(q.acquire.p50 == 20 && q.acquire.p99 == 40 && q.acquire.p999 == 40);
    check(q.total.p50 == 200 && q.total.p99 == 400 && q.total.p999 == 400);
    samples.record(50, 500);
    rejects([&] { samples.record(60, 600); });
    check(samples.capacity() == 5 && samples.size() == 5);
    q = samples.summarize();
    check(q.acquire.p50 == 30 && q.total.p99 == 500);
}
