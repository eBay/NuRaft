Raft Benchmark
---------
Benchmark program to measure the performance of the pure Raft replication logic, excluding disk I/O and state machine overhead.

It uses
* In-memory Raft log store, based on `std::map`.
* Empty state machine which does nothing on commit.

How to Run
-----
* Parameters
```sh
$ ./raft_bench <server ID> <IP address:port> <benchmark duration in second> <input traffic (IOPS)> <number of threads> <payload size in byte> <server 2 IP address:port> <server 3 IP address:port> ...
```

* Run followers first
```sh
$ ./raft_bench 2 10.10.10.2:12345 3600
```
```sh
$ ./raft_bench 3 10.10.10.3:12345 3600
```
It will run follower 2 and 3 for an hour.

* Run leader
```sh
$ ./raft_bench 1 10.10.10.1:12345 30 100 2 256 10.10.10.2:12345 10.10.10.3:12345
```
It will
* Run the benchmark for 30 seconds.
* Target traffic: 100 requests/second.
* 2 client threads.
* Each payload size will be 256 bytes.

After each run, **all followers MUST BE killed and then re-launched**.

Quick Benchmark Results
-----------------------
[Go to the page](../../docs/bench_results.md)