# lock

RDMA locking implementations and experiment runner. Three lock backends (CAS, replicated Mu, direct handoff), a shared workload driver and CloudLab deployment scripts.

## Local core build

    cmake -S . -B build -DLOCK_WITH_RDMA=OFF
    cmake --build build -j
    ctest --test-dir build --output-on-failure

Hardware tests require InfiniBand and libibverbs. See docs/architecture.md.
