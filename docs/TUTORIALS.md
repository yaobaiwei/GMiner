# G-Miner Tutorials

## Table of Contents

* [Sample datasets](#data)
* [Uploading the dataset to HDFS](#put)
* [Dataset partition](#partition)
* [Configuring and running G-Miner](#run)


<a name="data"></a>
## Sample datasets

Here we present 4 sample datasets with different formats and every `.adj` file represents a complete dataset.

```bash
$ cd $GMINER_HOME/sample-datasets
$ ls
normal_sample.adj  label_sample.adj  attr_sample.adj  multi_attr_sample.adj
```

- Supported format (one line):

 `normal format:` </br>
```bash
{vertex_id}	{num_neighbors} {neighbor1} {neighbor2} ...
```

 `label format:` </br>
```bash
{vertex_id} {vertex_label}	{neighbor1} {neighbor1_label} {neighbor2} {neighbor2_label} ...
```

 `attribute format:` </br>
```bash
{vertex_id}	{attribute_list}	{num_neighbors} {neighbor1} {neighbor2} ...
```

 `multiple attributes format:` </br>
```bash
{vertex_id}	{attribute_list}	{num_neighbors} {neighbor1} {neighbor1_attrlist} {neighbor2} {neighbor2_attrlist} ...
```

- User defined format:
When using user-defined Partitioner and Application classes, the users need to implement the virtual functions in order to control the formats </br>
`to_vertex()` in `User-Defined-Partitioner`: specify the input format for a partition program </br>
`to_line()` in `User-Defined-Partitioner`: specify the output (to hdfs) format for a partition program </br>
`to_vertex()` in `User-Defined-Application`: specify the input format for an application and it should match with the output format of the partitioned data. </br>


<a name="put"></a>
## Uploading the dataset to HDFS
G-Miner requires that a large graph data is partitioned into smaller files under the same data folder, and these files are loaded by different computing processes during graph computing.

To achieve this goal, we cannot use the command `hadoop fs -put {local-file} {hdfs-path}` directly. Otherwise, the data file is loaded by one G-Miner process, and the other processes simply wait for it to finish loading.

We remark that parallel loading only speeds up data loading, and has no influence on the performance of graph partitioning and computing.

To put a large graph data onto HDFS, one needs to compile and run this data-putting program `put` with two arguments **{local-file}** and **{hdfs-path}**.
```bash
$ cd $GMINER_HOME
$ mpiexec -n 1 $GMINER_HOME/release/put /local_path/to/dataset/{local-file} /hdfs_path/to/dataset/
```

<a name="partition"></a>
## Dataset partition
Before running application codes, we should do graph partitioning. We also have provided some basic partition algorithms with different graph formats supported as default.

```bash
$ cd $GMINER_HOME
$ mpiexec -n {num_workers} -f machines.cfg $GMINER_HOME/release/partition /hdfs_path/to/dataset/  /hdfs_path/to/input/ {partition_method}
```

**partition_method**: We provide 2 default partitioning strategy with 3 input graph formats. Case in point, `normal_bdg` indicates the format of the graph dataset is `normal` without label or attributes and the partition strategy is `bdg` as our paper proposed.

- They are `normal_bdg`, `label_bdg`, `attr_bdg`, `normal_hash`, `label_hash`, `attr_hash` respectively.

- Note: `multi_attr` is an optional flag(argument) following the **{partition_method}** when the format of a dataset has multiple attributes.




<a name="run"></a>
## Configuring and running G-Miner

**1)** Edit `gminer-conf.ini` and `machines.cfg`

**gminer-conf.ini:**
```bash
#ini file for example
#new line to end this ini

[PATH]
;for application I/O and local temporary storage
HDFS_HOST_ADDRESS = master
HDFS_PORT = 9000
HDFS_INPUT_PATH  = /hdfs_path/to/input/
HDFS_OUTPUT_PATH = /hdfs_path/to/output/
LOCAL_TEMP_PATH  = /local_path/to/G-Miner_tmp
FORCE_WRITE = TRUE    ;force to write HDFS_OUTPUT_PATH

[COMPUTING]
;for task computing configurations
CACHE_SIZE = 1000000  ;the size of cachetable in each worker
NUM_COMP_THREAD = 10  ;number of threads in threadpool for task computation
PIPE_POP_NUM = 100    ;number of tasks popped out each batch in the pipeline

[STEALING]
;for task stealing configurations
POP_NUM = 100         ;number of tasks for pque pops tasks to remote worker during one stealing procedure
SUBG_SIZE_T = 30      ;threshold that task can be moved to other workers only if its current subgraph size <= SUBG_SIZE_T
LOCAL_RATE = 0.5      ;threshold that task can be moved to other workers only if its current local rate <= LOCAL_RATE

[SYNC]
;for context and aggregator sync
SLEEP_TIME = 0        ;unit:second; do context and aggregator sync periodically during computation; if SLEEP_TIME == 0, then no sync happens during computation
```

**machines.cfg (one per line):**
```bash
worker1
worker2
worker3
.......
```

**2)** Launch a G-Miner application generally

```bash
$ mpiexec -n {num_workers+1} -f machines.cfg $GMINER_HOME/release/app {app_arguments}
```

Note: compared with the partition, G-Miner's app needs an extra MPI process playing the role of "master", so we use **{num_workers+1}** instead.

**3)** Launch provided G-Miner applications
- Note: </br>
`normal_youtube.adj` data is used for G-Miner App `TC` and `MCF` after the partition </br>
`label_youtube.adj` data is used for G-Miner App `GM` after the partition </br>
`attr_youtube.adj` data is used for G-Miner App `CD` after the partition </br>
`multi_attr_youtube.adj` data is used for G-Miner App `GC` after the partition </br>

- Triangle Counting (TC) </br>
It takes a non-attributed graph as the input and uses only the 1-hop neighbors of each vertex in the computation period.
```bash
$ mpiexec -n {num_workers+1} -f machines.cfg $GMINER_HOME/release/tc
```

- Graph Matching (GM) </br>
It is a fundamental operation for graph mining and network analysis which takes a labeled graph as the input.
```bash
$ mpiexec -n {num_workers+1} -f machines.cfg $GMINER_HOME/release/gm
```

- Maximum Clique Finding (MCF) </br>
It is applied on non-attributed graphs and can be computed based on the 1-hop neighborhood. We followed [**The Maximum Clique Problem**](http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.56.6221) to implement an efficient algorithm with optimized pruning strategy on G-Miner.
```bash
$ mpiexec -n {num_workers+1} -f machines.cfg $GMINER_HOME/release/mc
```

- Community Detection (CD) </br>
It defines a set of vertices which share common attributes and together form a dense subgraph as a community. We adopted a [**state-of-art algorithm**](https://link.springer.com/content/pdf/10.1007/3-540-45066-1_22.pdf) to mine the dense subgraph topology and guarantee the similarity of attributes in a community by a filtering condition on newly added vertex candidates.
```bash
$ export K_THRESHOLD="3"
$ mpiexec -n {num_workers+1} -f machines.cfg $GMINER_HOME/release/cd  "${K_THRESHOLD}"
```

- Graph Clustering (GC) (FocusCO) </br>
It is widely used for recommendation area. We followed the [**FocusCO kdd'14**](http://www.perozzi.net/publications/14_kdd_focused.pdf) to group focused clusters from an attributed graph based on user preference. For distributed implementation, we pre-defined the weight of every attribute and then implemented the **Algorithm A1** of the paper.
```bash
$ export MIN_WEIGHT="0.92"    #threshold that only add the new candidates into subgraph with weight >= MIN_WEIGHT
$ export MIN_CORE_SIZE="3"    #threshold that only compute the seedtask with its subgraph size >= MIN_CORE_SIZE
$ export MIN_RESULT_SIZE="5"  #threshold that only store the result cluster with size >= MIN_RESULT_SIZE
$ export DIFF_RATIO="0.001"   #threshold for judging two weight is similarity or not
$ export ITER_ROUND_MAX="10"  #threshold that only compute number of iterations < ITER_ROUND_MAX with each task
$ export CAND_MAX_TIME="3"    #threshold that only compute the top CAND_MAX_TIME*subgraph_size candidates in each round during computation
$ mpiexec -n {num_workers+1} -f machines.cfg $GMINER_HOME/release/fco "${MIN_WEIGHT}" "${MIN_CORE_SIZE}" "${MIN_RESULT_SIZE}" "${DIFF_RATIO}" "${ITER_ROUND_MAX}" "${CAND_MAX_TIME}"
```
