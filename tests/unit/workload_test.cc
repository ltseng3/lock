#include "lock/bench/workload.h"
#include <cassert>
int main() {
    for (const char* dist : {"uniform", "zipf_0.5", "zipf_0.99"}) {
        lock::LockPicker a(1000,dist,7654), b(1000,dist,7654);
        for (int i=0;i<10000;++i) {
            auto x = a.next();
            assert(x < 1000);
            assert(x == b.next());
        }
    }
}
