# Usage

Go inside the `src` folder, run
```
make clean
make
```
To run the code on multiple machines connected via HDFS, first need to upload the input files onto HDFS. Then, run
```
mpiexec -n num_of_processes -f path/to/your/hostfile ./run -d path/to/your/data/graph/file -q path/to/your/query/graph/file -pseudo on -order degree -input HDFS
```
where `-n` indicates the number of processes used in total, `-f` indicates the hostfile, `-d` and `-q` indicate the path to the data graph file and the query graph file (in HDFS), respectively. `-pseudo on` means turning on the pseudo-children technique, use the keywork `off` to turn it off. `-order` indicates the method of generating sketch tree (`degree` means degree-aware, `random` means random and `ri` means neighbor-aware, see the paper for more infomation). Finally, `-input HDFS` means the input files are from HDFS, and must be included.

The hostfile admits the following format:
```
master:2
slave1:2
...
slave8:2
```
`master`, `slave1` to `slave8` are hostnames of machines, and 2 indicates the number of processes run on each machine. In this example, we use 9 machines, each running two processes, thus 18 processes in total, so `n=18`.

The data graph and the query graph admit the following format:
```
verticeID labelID neighbor1ID neighbor1Label neighbor2ID neighbor2Label ...
```
