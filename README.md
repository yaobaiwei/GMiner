# G-Miner

G-Miner is a general distributed system aimed at general graph mining.

G-Miner, a distributed toolkit, a popular framework for large-scale graph processing. G-Miner adopts a number of state-of-art graph based algorithms and formulates a set of key operations in graph processing related fields. We implement these operations as the APIs of G-Miner, which provide strong performance guarantees due to the bounds on computation and memory. Over and above them, G-Miner itself has a high parallelism with both CPU and network as well.


## Feature Highlights

- **General Graph Mining Schema:** G-Miner aims to provide a unified programming framework for implementing distributed algorithms for a wide range of graph mining applications. To design this framework, we have summarized common patterns of existing graphmining algorithms.

- **Task Model:** G-Miner supports asynchronous execution of various types of operations (i.e., CPU, network, disk) and efficient load balancing by modeling a graph mining job as a set of independent tasks. A task consists of three fields: sub-graph, candidates and context.

- **Task-Pipeline:** G-Miner provides the task-pipeline, which is designed to asyn-chronously process the following three major operations in G-Miner: (1)CPU computation to process the update operation on each task, (2)network communication to pull candidates from remote machines, and (3)disk writes/reads to buffer intermediate tasks on local disk of every machine.


## Getting Started

* **Dependencies Install**

  G-Miner is built with the same dependencies of our previous project [Pregel+](http://www.cse.cuhk.edu.hk/pregelplus/index.html). To install G-Miner's dependencies (e.g., MPI, HDFS), using the instructions in this [guide](http://www.cse.cuhk.edu.hk/pregelplus/documentation.html).

* **Build**

Please manually MODIFY the dependency path for MPI and HDFS in CMakeLists.txt in root directory.

```bash
$ export GMINER_HOME=/path/to/gminer_root  # must configure this ENV
$ cd $GMINER_HOME
$ ./auto-build.sh
```

* [**Tutorials**](docs/TUTORIALS.md)


## Academic Paper

[**Eurosys 2018**] [G-Miner: An Efficient Task-Oriented Graph Mining System](docs/G-Miner-Eurosys18.pdf). Hongzhi Chen, Miao Liu, Yunjian Zhao, Xiao Yan, Da Yan, James Cheng.

## Acknowledgement
The subgraph-centric vertex-pulling API is attributed to our prior work [G-thinker](https://arxiv.org/abs/1709.03110).

## License

Copyright 2018 Husky Data Lab, CUHK
