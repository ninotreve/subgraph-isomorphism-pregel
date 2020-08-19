# subgraph-isomorphism-pregel
Subgraph isomorphism on Pregel+.

### Implementation v1.0 (0819) 
**by Yuejia Zhang**

这个版本完全没有改动system code。或者说，是强行把子图查询塞进了Pregel+的框架里。

查询图和数据图一样以"[Data|Query] VertexID Label Neighbors"这样的形式存储[1]，每一个节点可能代表数据或查询节点。数据节点之间互相传消息完成子图查询，而查询节点向Aggregator传消息，数据节点通过调用`getAggregator`了解下一个查询节点的标签和邻居信息[2]。

Preprocessing[3]是邻居之间互传label信息，后续相当于一个没有degree的LDF filter。匹配过程就是一个节点到下一个节点，“下一个节点”是已匹配的邻居的交集。

查询图有m个节点，每个节点需要两个超步完成（发信息给已匹配的节点+已匹配的节点发信息给邻居节点），所以一共需要2m超步。按照所有Pregel算法的要求，每一个超步结束后都进行同步[4]。

以下是`compute`函数的代码框架：

```
virtual void compute(MessageContainer & messages)
{
    if (!id.isQuery)
    {
        if (step_num() == 1)
            // send label info to neighbors
        else if (step_num() == 2)
            // receive label info from neighbors, and start matching
        else if (step_num() % 2 == 1)
            // match message fragments
            mappings = match(messages);
            for each mapping
                /* add current vertex to mapping
                 * send messages to mapped vertices that are neighboring to
                 * the next query vertex. 
                 */
        else 
            /* receive message fragments,
             * and forward them to neighbors of specific labels. 
             */       
    }
}
```

目前只实现了单查询的功能。在目前的版本里，如果要多查询，就是多个查询同步进行（如果查询差异很大，会减少整体时间）。

#### 问题
1. 查询图和数据图的文件格式。如果点和边的信息分开存储，必须重新设计输入接口。（对应`Worker.h`中`run`的前半部分）
2. 我本来是想让master确定query的访问顺序，再把query发送给各个worker的，需要对源代码进行改动：在`Worker`类中添加查询图。
3. 之前提出的预处理方案没有实现。如果要实现的话，可以分为两个application。
4. 在子图查询中，这是没有必要的。但删除synchronization barrier，是否等于要修改信息传递的逻辑？现在的设计是，所有节点同步后，进行消息传递。（用的是MPI）

#### 待优化
1. 数据图节点的分布。Pregel+只支持hash partition。要么按照某一种分区方式重新标号（大图代价是否会太大？），要么（如果不重新标号）就只能修改Pregel+的分区逻辑（也就是寻点逻辑）。
2. 同一台机器上节点之间的信息发送可以发指针。除此之外，目前的代码里，可能会有浪费空间/时间的复制操作，可以由指针来代替。（对不起，我对C/C++不熟悉，我太菜了T_T）
3. 在MASTER和SLAVE之间通信，了解各台机器的工作状态(statistics)，实现load balancing.
4. 另一种求交集算法的设计和比较，以及bloom filter
5. 多查询优化，分为以下几个部分：
    - 匹配过程中的partial mapping可以保存下来，如果别的查询有共同前缀的话，可以直接从partial mapping开始匹配。
    - MASTER重新设计查询顺序，以满足load balancing。
    - 消息合并。可以利用Pregel里有Combiner。

#### 阅读计划
1. Pregel+的官网上有一些应用，我只看了比较简单的几个：Hash-min, Pagerank, SSSP. 我想再看看其它比较复杂的，或许能有新的idea。
2. Pregel+提出的两项技术：vertex mirroring和request-respond.
3. 现在对Pregel比较熟悉了，我打算去仔细读一读Quegel ("Query-Centric Pregel")。
