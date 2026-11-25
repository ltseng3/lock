# lock

RDMA locking implementations and experiment runner. Three lock backends (CAS, replicated Mu, direct handoff), a shared workload driver and CloudLab deployment scripts.

## Local core build

    cmake -S . -B build -DLOCK_WITH_RDMA=OFF
    cmake --build build -j
    (cd build && ctest --output-on-failure)

Hardware tests require InfiniBand and libibverbs. See docs/architecture.md.

## CloudLab build toolchain

The project accepts **CMake 3.16.3** (the Ubuntu 20.04 system version). It still
requires a compiler and standard library implementing C++20; GCC 10 or newer is
recommended. If the system C++ compiler is too old, install `g++-10` and use
`CXX=g++-10 cmake -S . -B build` to configure a **fresh** build directory.

## Experimental recovery

See [recovery design](docs/recovery.md) for the failure model, epoch
metadata, ownership invariants, implementation status and validation plan.
The recovery branches are separate from the failure-free benchmark baseline.

On the experimental recovery/handoff branch, the initial Handoff recovery
smoke experiment is:

    python3 experiments/run.py --suite=experiments/suites/handoff_recovery.json --max-trials=1 --remote-dir=/tmp/lock-handoff-recovery

All nodes must run the same wire version. Client crashes, leases, election
and multiple replacements remain unsupported; see the design document.

For the experimental transport stall ablation, see [Handoff stall diagnostics](docs/handoff-stall-diagnostics.md).
