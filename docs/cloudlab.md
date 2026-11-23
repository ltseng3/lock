# Eight-node CloudLab runs

The experiment controller runs on **node 4 (10.10.1.4)**, and uses the SSH user **tsengle** by default.

The topology files contain `"controller_node_id": 4` and `"ssh_user": "tsengle"`. For node 4, the controller runs shell commands and rsync locally and copies logs directly; **no SSH or SCP to 10.10.1.4** is attempted. All other nodes are accessed by SSH as `tsengle@10.10.1.X`. The SSH and RDMA addresses remain independently configurable via `ssh_host` and `rdma_ip`.

Run from the repository checkout on node 4:

    git checkout main && git pull origin main
    python3 experiments/run.py --smoke
    python3 experiments/run.py --only-backend=cas --max-trials=1
    python3 experiments/run.py

You can override the username with `--ssh-user=...` if needed. **The controller must actually be running on node 4**, since commands targeting controller_node_id are executed locally rather than over SSH. For a different controller machine, update `controller_node_id` in the topology before running.

The default layout has replica nodes 1–3 and client nodes 4–8. For the optional five-replica experiment, nodes 1–5 are replicas and nodes 6–8 are clients; node 4 remains the local controller, regardless of its experimental role.

    python3 experiments/run.py --topology experiments/topologies/apt8_five.json --suite experiments/suites/five_small.json

Install CMake, a C++20 compiler, libibverbs-dev and rsync on all eight nodes, and ensure SSH connectivity from node 4 to the **other seven** hosts. The InfiniBand port must be active and memory-registration limits must support the configured MR sizes (512 MiB by default on servers). Raw logs and a run manifest are saved under `results/`.

See docs/validation.md for the correctness boundary. Automated CI cannot verify CloudLab hardware behavior.

## Ubuntu 20.04 / CMake 3.16.3

CMake 3.16.3 is supported; no CMake upgrade is required. A C++20-capable
compiler and standard library are required (GCC 10+). If the default compiler
is GCC 9, install `g++-10` on each node and configure a fresh build directory
using `CXX=g++-10 cmake -S . -B build -DLOCK_WITH_RDMA=ON`. The experiment runner automatically prefers `g++-10` when installed. Do not reuse
a build directory configured with a different compiler. The legacy CTest
invocation is `(cd build && ctest --output-on-failure)`.
