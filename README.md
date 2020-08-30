# subgraph-isomorphism-pregel
Subgraph isomorphism on [Pregel+](http://www.cse.cuhk.edu.hk/pregelplus/index.html).

### Implementation v1.0 (0819) 

这个版本完全没有改动system code。或者说，是强行把子图查询塞进了Pregel+的框架里。

查询图和数据图一样以"[Data|Query] VertexID Label Neighbors"这样的形式存储[Issue #1](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/1)，每一个节点可能代表数据或查询节点。数据节点之间互相传消息完成子图查询，而查询节点向Aggregator传消息，数据节点通过调用`getAggregator`了解下一个查询节点的标签和邻居信息[Issue #2](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/2)。

Preprocessing[Issue #3](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/3)是邻居之间互传label信息，后续相当于一个没有degree的LDF filter。匹配过程就是一个节点到下一个节点，“下一个节点”是已匹配的邻居的交集。

查询图有m个节点，每个节点需要两个超步完成（发信息给已匹配的节点+已匹配的节点发信息给邻居节点），所以一共需要2m超步。按照所有Pregel算法的要求，每一个超步结束后都进行同步[Issue #4](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/4)。

以下是`compute`函数的代码框架：

```c++
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

### Implementation v2.0 (0830) 

查询图和数据图的格式都是"VertexID Label NumOfNeighbors Neighbor1 Neighbor2 ..."]

计算过程被分为了五步：
```c++
	worker.load_data(data_path);
	worker.run_preprocess();

	wakeAll();
	worker.load_query(query_path);
	worker.run_compute();
	worker.dump_graph(out_path, force_write);
```

在`worker.load_query(query_path)`中，Master读入所有的查询节点，存入数据结构`Query`中（本质是存储查询节点的哈希表，详见[Issue #2](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/2)）。再把`query`发送给每一台Slave，这样Slave在计算过程中随时可以访问`Query`。

在`worker.run_compute()`中，Slave按照预设好的查询顺序依次进行匹配，注意到按照DFS方式生成的查询树可以避免消息的回传，因此如果查询图有m个节点，那么匹配过程最多需要m个超步完成。

```c++
virtual void preprocess(MessageContainer & messages)
{
	if (step_num() == 1)
	{ // send label info to neighbors }
	else // if (step_num() == 2)
	{ // receive label info from neighbors }
}
```

```c++
virtual void compute(MessageContainer & messages)
{
	if (step_num() == 1)
	{   // start mapping with vertices with same label
		int root_u = query->root;
		int root_label = query->getLabel(root_u);
		if (value().label == root_label)
		{
			vector<VertexID> mapping;
			continue_mapping(mapping, root_u);
		}
		vote_to_halt();
	}
	else
	{
		for (size_t i = 0; i < messages.size(); i++)
		{
			SIMessage & msg = messages[i];
			//check if backward neighbors in neighbors
			if (flag) continue_mapping(msg.mapping, msg.vertex);
		}
		vote_to_halt();
	}
}
```


