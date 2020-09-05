# 实验计划

### Partitioning

random partition *vs.* label-neighbor-aware partition (sum of vertices *vs.* sum of degree)

- The drop in cross-machine messages

### Preprocessing

neighbors' labels *vs.* neighbors' labels + edges *vs.* two-hop neighbors

- The drop in messages, the increase in speed
- The storage comparison (combined with space complexity)

### Filtering

no filtering *vs.* steady-state filter *vs.* ?

- The trade-off: does the benefit outweigh the filtering time overhead?
- percentage of successful messages.

### Matching Strategy

query path *vs.* query tree

### Matching Order

叶越多越好地生成DFS树 *vs.* 好order的DFS树（c.f. SIGMOD 20）

### Enumeration (for query tree)

method 1 *vs.* method 2



**All of the above:**

- related to the data graph? (like, if the label distribution is skewed)