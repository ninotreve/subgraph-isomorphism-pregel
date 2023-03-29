# Hybrid Subgraph Matching for Distributed Systems 

## Citation
Y. Zhang, W. Zheng, Z. Zhang, P. Peng, and X. Zhang, [Hybrid Subgraph Matching Framework Powered by Sketch Tree for Distributed Systems](https://doi.org/10.1109/icde53745.2022.00082), 2022 IEEE 38th International Conference on Data Engineering (ICDE), May 2022, doi: 10.1109/icde53745.2022.00082.

## Installation (Dependency)
This system is built on [Pregel+](http://www.cse.cuhk.edu.hk/pregelplus/index.html). Please follow the [installation instructions](http://www.cse.cuhk.edu.hk/pregelplus/deploy-hadoop2.html) to install Pregel+, most importantly, please install HDFS and MPICH.

Remember to modify the file `src/utils/ydhdfs2.h`. In line 29 and 30, the current hostname is `localhost` and `port` is `9000`, but please change the host name and port to your corresponding port (in correspondance with `$HADOOP_HOME/etc/hadoop/core-site.xml`). 

After HDFS and MPICH are installed, you can compile the code by running `make` in this directory.

## Usage
First, you need to upload the input graph files onto HDFS. The data graph file and the query graph file follow the below format:

Each line represent a vertex, its ID and label followed by its neighbors' ID and label, i.e.,
```
<Vertex ID> <Vertex Label> <Neighbor 1 ID> <Neighbor 1 Label> <Neighbor 2 ID> <Neighbor 2 Label> ...
```
All words are separated by blank separator (can be arbitrary number of space or `\t`).

We provide a toy query graph and toy data graph in the directory of `graphs` for your reference.

Upload the input graph files onto HDFS by running
```
hadoop fs -put <local data files> <destination directory on Hadoop>
```

To run the code,
```
mpiexec -n <num_of_processes> -f <path/to/your/hostfile> ./run -d <path/to/your/data/graph/file> -q <path/to/your/query/graph/file> -pseudo on -order degree -input HDFS
```
where:
 - `-n` indicates the number of processes used in total;
 - `-f` indicates the hostfile (only used when you want to run the process on multiple machines connected via SSH);
 - `-d` and `-q` indicate the path to the data graph file and the query graph file (in HDFS), respectively;
 - `-pseudo on` means turning on the pseudo-children technique, use the keywork `off` to turn it off, but we suggest you to turn it on;
 - `-order` indicates the method of generating sketch tree (`degree` means degree-aware, `random` means random and `ri` means neighbor-aware, we suggest you use `degree`);
 - `-input HDFS` means the input files are from HDFS, and must be included.

The hostfile admits the following format:
```
master:2
slave1:2
...
slave8:2
```
`master`, `slave1` to `slave8` are hostnames of machines, and 2 indicates the number of processes run on each machine. In this example, we use 9 machines, each running two processes, thus 18 processes in total, so `n=18`.

The matching process and results will be printed on screen. 

## Example
```
mpiexec -n 2 ./run -d /graphs/toy.txt -q /graphs/query.txt -pseudo on -order degree -input HDFS
```
The result:
```
Data graph path: /graphs/toy.txt
Query graph path (HDFS): /graphs/query.txt
Input Format (1 for default, 0 for g-thinker): 1
Output graph path:
Optimization techniques: Pseudo-children Counting/

Loading data graph...
Loading data graph time : 1.154081 seconds
Preprocessing...
Preprocessing time : 0.000057 seconds
In total, offline time : 1.154190 seconds

Loading query graph...
Loading query graph time : 0.114581 seconds
Building query tree...
depth = 4 max branch number = 0
Building query tree time : 0.000030 seconds
**Subgraph matching**
Subgraph matching time : 0.000100 seconds
**Subgraph enumeration**
Subgraph enumeration time : 0.000020 seconds
Dumping results...
Dumping results time : 0.000000 seconds
In total, online time : 0.114795 seconds
================ Final Report ===============
Mapping count: 4
COMPUTE Time : 0.000175 seconds
```

## Code Structure
All the source codes is inside `src` directory. `dev` is a directory for experiments and further development, and is not stable released. 

## Issues
Please send to yuejiazhang21@m.fudan.edu.cn for any further questions.
