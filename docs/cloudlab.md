# Eight-node CloudLab runs

The default topology uses three replica nodes at 10.10.1.1–10.10.1.3 and five client nodes at 10.10.1.4–10.10.1.8. Every node has separate ssh_host and rdma_ip fields in experiments/topologies/apt8.json. These are initial example values; adjust the file if CloudLab assigns different interfaces or addresses.

The controller must be able to SSH and rsync to every target node. It runs from any node with the Git checkout and access to the eight private addresses. Install CMake, a C++20 compiler, libibverbs-dev, ssh and rsync on the nodes. The selected InfiniBand port must be active; the connection bootstrap uses the configured IPv4 addresses and TCP port 18515.

Start with the transport smoke run:

    python3 experiments/run.py --ssh-user=$USER --smoke

For the full suite, after the client/server binaries are available:

    python3 experiments/run.py --ssh-user=$USER

For a quick first run:

    python3 experiments/run.py --only-backend=cas --max-trials=1

To inspect commands without contacting the nodes:

    python3 experiments/run.py --dry-run --max-trials=1

Each trial archives per-node logs and a manifest. Invalid trials are marked invalid and are not included in final statistics. The suite defaults to 1000 locks, uniform/Zipf workloads, and outstanding windows of 1–16.

The bootstrap currently supports homogeneous little-endian machines using reliable-connected InfiniBand QPs. The experiment controller does not implement machine or leader recovery. Hardware-level performance must be measured, not inferred from CI results.
