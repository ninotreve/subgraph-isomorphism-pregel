# subgraph-isomorphism-pregel
Subgraph isomorphism on [Pregel+](http://www.cse.cuhk.edu.hk/pregelplus/index.html).

### Implementation v1.0 (0819) 
**by Yuejia Zhang**

这个版本完全没有改动system code。或者说，是强行把子图查询塞进了Pregel+的框架里。

查询图和数据图一样以"[Data|Query] VertexID Label Neighbors"这样的形式存储[Issue #1](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/1)，每一个节点可能代表数据或查询节点。数据节点之间互相传消息完成子图查询，而查询节点向Aggregator传消息，数据节点通过调用`getAggregator`了解下一个查询节点的标签和邻居信息[Issue #2](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/2)。

Preprocessing[Issue #3](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/3)是邻居之间互传label信息，后续相当于一个没有degree的LDF filter。匹配过程就是一个节点到下一个节点，“下一个节点”是已匹配的邻居的交集。

查询图有m个节点，每个节点需要两个超步完成（发信息给已匹配的节点+已匹配的节点发信息给邻居节点），所以一共需要2m超步。按照所有Pregel算法的要求，每一个超步结束后都进行同步[Issue #4](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/4)。

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
            for each message
                /* if backward neighbors in neighbors, add current vertex to mapping
                 * send messages to mapped vertices that are neighboring to
                 * the next query vertex. 
                 */
        else 
            /* receive message (partial MAPPING),
             * and forward them to neighbors of specific labels. 
             */       
    }
}
```


