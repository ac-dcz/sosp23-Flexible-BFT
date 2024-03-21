# Flexible Advancement in Asynchronous BFT Consensus

about Flexible Advancement in Asynchronous BFT Consensus...

## Quick Start

Flexible Advancement in Asynchronous BFT Consensus is written in Rust, but all benchmarking scripts are written in Python and run with [Fabric](http://www.fabfile.org/).
To deploy and benchmark a testbed of 4 nodes on your local machine, clone the repo and install the python dependencies:

```
$ git clone https://github.com/ac-dcz/sosp23-Flexible-BFT
$ cd sosp23-Flexible-BFT/benchmark
$ pip install -r requirements.txt
```

You also need to install Clang (required by rocksdb) and [tmux](https://linuxize.com/post/getting-started-with-tmux/#installing-tmux) (which runs all nodes and clients in the background). Finally, run a local benchmark using fabric:

```
$ fab local
```

This command may take a long time the first time you run it (compiling rust code in `release` mode may be slow) and you can customize a number of benchmark parameters in `fabfile.py`. When the benchmark terminates, it displays a summary of the execution similarly to the one below.

```
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Protocol: 0
 DDOS attack: False
 Committee size: 4 nodes
 Input rate: 1,000 tx/s
 Transaction size: 512 B
 Faults: 0 nodes
 Execution time: 22 s

 Consensus timeout delay: 2,000 ms
 Consensus sync retry delay: 10,000 ms
 Consensus max payloads size: 500 B
 Consensus min block delay: 0 ms
 Mempool queue capacity: 10,000 B
 Mempool max payloads size: 15,000 B
 Mempool min block delay: 0 ms

 + RESULTS:
 Consensus TPS: 951 tx/s
 Consensus BPS: 487,076 B/s
 Consensus latency: 329 ms

 End-to-end TPS: 945 tx/s
 End-to-end BPS: 484,020 B/s
 End-to-end latency: 566 ms
-----------------------------------------
```
