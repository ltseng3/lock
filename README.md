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
