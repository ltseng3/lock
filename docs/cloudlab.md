# Eight-node CloudLab runs

The default topology uses three replica nodes at 10.10.1.1–10.10.1.3 and five client nodes at 10.10.1.4–10.10.1.8. Each node has separate ssh_host and rdma_ip fields in experiments/topologies/apt8.json. Edit those fields if private addresses differ.

The controller runs from one machine with SSH and rsync access to all eight nodes. Install CMake, a C++20 compiler, libibverbs-dev, ssh and rsync. Make sure InfiniBand is active and its MR memlock limits support the configured sizes (by default 512 MiB on server processes).

    python3 experiments/run.py --ssh-user=$USER --smoke
    python3 experiments/run.py --ssh-user=$USER --only-backend=cas --max-trials=1
    python3 experiments/run.py --ssh-user=$USER
    python3 experiments/run.py --dry-run --max-trials=1

To check five replicas on the same eight nodes:

    python3 experiments/run.py --ssh-user=$USER --topology experiments/topologies/apt8_five.json --suite experiments/suites/five_small.json

The suite contains 1000 locks, uniform/Zipf distributions, client worker counts and per-worker outstanding windows, and a single completed 8-byte RDMA WRITE while holding each lock. Raw logs and a run manifest are stored in results. The five-replica setting does not need a separate executable.

See docs/validation.md for the correctness boundary and validation checklist. Benchmarks have not been verified on CloudLab by automated CI.
