# subgraph-isomorphism-pregel
Subgraph isomorphism on [Pregel+](http://www.cse.cuhk.edu.hk/pregelplus/index.html).


### Implementation v2.0 (0830) (baseline)

Usage: `run -d data_graph_folder -q query_graph_folder -out output_folder` （如果是在eclipse里跑，需要设置run configurations）

查询图和数据图的格式都是"VertexID Label NumOfNeighbors Neighbor1 Neighbor2 ..."

计算过程被分为了六步。其中前两步`load_data(data_path)`和`run_type(PREPROCESS)`是离线阶段完成的，而后四步是在线完成的。
```c++
	worker.load_data(data_path);
	worker.run_type(PREPROCESS);

	worker.load_query(query_path);
	wakeAll();
	worker.run_type(MATCH);
	wakeAll();
	worker.run_type(ENUMERATE);
	worker.dump_graph(out_path, force_write);
```

#### 数据图预处理
数据图的每个节点发标签信息给自己的所有邻居。这样，每个节点就知道邻居的标签信息了。
```c++
virtual void preprocess(MessageContainer & messages)
{
	if (step_num() == 1)
	{ // send label info to neighbors }
	else // if (step_num() == 2)
	{ // receive label info from neighbors }
}
```

#### 导入查询图
在`worker.load_query(query_path)`中，Master读入所有的查询节点，存入数据结构`Query`中（本质是存储查询节点的哈希表，详见[Issue #2](https://github.com/ninotreve/subgraph-isomorphism-pregel/issues/2)）。Master用贪心算法（度数大的节点优先），将查询图转化为查询树，再把`Query`发送给每一台Slave，这样Slave在计算过程中随时可以访问`Query`。

![示例查询图，如果无法显示，需要科学上网](https://github.com/ninotreve/subgraph-isomorphism-pregel/blob/master/results/example1.PNG)

示例图：（左）查询图；（右）使用贪心算法把查询图转换为查询树，绿色虚线边是向后邻居边，详见`pregel_app_subgraph.h`中的`Query::dfs(int currID, int parentID, bool isRoot)`。根据每个分支上的分支节点（橙色）数，为每个节点赋予一个分支数`branch_number`，即红色数字，详见`pregel_app_subgraph.h`中的`Query::addBranchNumber(int currID, int num)`。

#### 子图匹配
Slave按照预设好的查询顺序，对查询树的多个分支同时进行匹配。注意到按照DFS方式生成的查询树可以避免消息的回传，因此如果查询图有m个节点，那么匹配过程最多需要m个超步完成。

在每一步匹配中，确定向后邻居的边的条件全部满足，然后调用`continue_mapping(mapping, root_u)`，即把自己这个节点加入部分匹配，再将部分匹配发送给满足以下两个条件的邻居：(1) 标签信息正确；(2) 邻居节点不在部分匹配中。

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

#### 子图枚举（分支拼接）

这个步骤最多需要`max_branch_number`个超步完成，每个超步中，将同一`branch_number`的叶节点上的部分匹配发送给分支节点（示例查询中的橙色节点），由分支节点对部分匹配进行拼接（join）。

```c++
virtual void enumerate(MessageContainer & messages)
{
	if (step_num() < query->max_branch_number + 2)
	{
		for (size_t i = 0; i < messages.size(); i++)
		{
			SIMessage & msg = messages[i];
			// classify each message and save it into join_results
		}

		for (join_it = join_results.begin(); join_it != join_results.end();
				join_it ++)
		{
			prefix = join_it->first;
			// for each prefix, join the branches together and save it into results
			// the current version expands the partial result.
		}

		for (it = this->results.begin(); it != this->results.end(); it++)
		{ 
			// send result to its parent if it is going to be used in the next iteration
		}
	}
	vote_to_halt();
}
```
