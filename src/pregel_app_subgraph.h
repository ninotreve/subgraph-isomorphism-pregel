#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//#define DEBUG_MODE_ACTIVE 1
//#define DEBUG_MODE_MSG 1
#define DEBUG_MODE_PARTIAL_RESULT 1
#define DEBUG_MODE_RESULT 1

//input line format:
//  vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
//output line format:
//  # MATCH
//  query_vertexID \t data_vertexID

//------------SIKey = VertexID-------------------------------------------------
//------------SIValue = <label, hash_map<neighbors, labels> >------------------

struct SIValue
{
	int label;
	hash_map<VertexID, int> neighbors;
};

ibinstream & operator<<(ibinstream & m, const SIValue & v){
	m << v.label << v.neighbors;
	return m;
}

obinstream & operator>>(obinstream & m, SIValue & v){
	m >> v.label >> v.neighbors;
	return m;
}

//--------SIMessage = <type, vertex, label, mapping, branch_result,>--------
//  e.g. <type = LABEL_INFOMATION, vertex, label>
//	     <type = MAPPING, mapping, vertex = next_u>
//		 <type = BRANCH_RESULT, branch_result, vertex = curr_u>

struct SIMessage
{
	int type;
	VertexID vertex;
	int label;
	vector<VertexID> mapping;
	vector<vector<VertexID>> branch_result;

	SIMessage()
	{
	}

	SIMessage(int type, VertexID vertex, int label)
	{ // for label information
		this->type = type;
		this->vertex = vertex;
		this->label = label;
	}

	SIMessage(int type, vector<VertexID> mapping, VertexID next_u)
	{ // for mapping
		this->type = type;
		this->mapping = mapping;
		this->vertex = next_u;
	}

	SIMessage(int type, vector<vector<VertexID>> res, VertexID curr_u)
	{ // for branch result
		this->type = type;
		this->branch_result = res;
		this->vertex = curr_u;
	}
};

ibinstream & operator<<(ibinstream & m, const SIMessage & v){
	m << v.type << v.vertex << v.label << v.mapping << v.branch_result;
	return m;
}

obinstream & operator>>(obinstream & m, SIMessage & v){
	m >> v.type >> v.vertex >> v.label >> v.mapping >> v.branch_result;
	return m;
}

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 0,
	MAPPING = 1,
	BRANCH_RESULT = 2
};

//=============================================================================
struct SINode
{
	int id;
	int label;
	vector<int> nbs;

	// for depth-first search
	bool visited;
	int branch_number;
	int dfs_number; // 0-based
	int parent;
	int level; // root: level 0. Used as an index in mapping.
	vector<int> children;
	vector<int> b_nbs; // backward neighbors

	SINode() {}

	SINode(int id, int label)
	{
		this->id = id;
		this->label = label;
		this->visited = false;
	}

	void add_edge(int other)
	{
		this->nbs.push_back(other);
	}
};

ibinstream& operator<<(ibinstream& m, const SINode& node)
{
    m << node.id << node.label << node.nbs << node.branch_number << node.parent
      << node.dfs_number << node.level << node.children << node.b_nbs;
    return m;
}

obinstream& operator>>(obinstream& m, SINode& node)
{
    m >> node.id >> node.label >> node.nbs >> node.branch_number >> node.parent
      >> node.dfs_number >> node.level >> node.children >> node.b_nbs;
    return m;
}

//==========================================================================
// Overloading << operator of SINode for debug purpose.
ostream & operator << (ostream & os, const SINode & node)
{
	os << "ID: " << node.id << endl;
	os << "Label: " << node.label << endl;
	os << "Branch number: " << node.branch_number << endl;
	os << "Neighbors: " << node.nbs << endl;
	os << "Backward neighbors: " << node.b_nbs << endl;
	return os;
}

//==========================================================================
// Define hash of vector
namespace __gnu_cxx {
	template <>
	struct hash<vector<int>> {
		size_t operator()(vector<int> v) const
		{
			size_t seed = 0;
			for (size_t i = 0; i < v.size(); i++)
			{
				hash_combine(seed, v[i]);
			}
			return seed;
		}
	};
}


//===========================================================

bool sortByValDesc(const pair<int, int> &a, const pair<int, int> &b)
{
	return (a.second > b.second);
}

class SIQuery:public Query<SINode>
{
public:
	int root;
	hash_map<int, SINode> nodes;

	int max_branch_number = 0;
	vector<int> dfs_order;
	// <vertex, nearest branch ancestor or root if it doesn't have one>
	hash_map<int, int> nbancestors;

	virtual void init()
	{
		if (! this->nodes.empty())
		{
			/*
			// pick arbitrary start vertex
			this->root = this->nodes.begin()->first;
			*/

			// pick vertex with largest degree
			hash_map<int, SINode>::iterator it;
			size_t degree, max_degree = 0;
			for (it = this->nodes.begin(); it != this->nodes.end(); it++)
			{
				degree = it->second.nbs.size();
				if (degree > max_degree)
				{
					max_degree = degree;
					this->root = it->first;
				}
			}
			this->dfs(this->root, 0, true);
			this->addBranchNumber(this->root, 0, this->root);
		}
	}

	virtual void addNode(char* line)
	{
		char * pch;
		pch = strtok(line, " ");
		int id = atoi(pch);

		pch = strtok(NULL, " ");
		int label = atoi(pch);

		this->nodes[id] = SINode(id, label);

		pch = strtok(NULL, " ");
		int num = atoi(pch);
		for (int k = 0; k < num; k++)
		{
			pch=strtok(NULL, " ");
			int neighbor = atoi(pch);
			if (id > neighbor)
			{
				this->nodes[id].add_edge(neighbor);
				this->nodes[neighbor].add_edge(id);
			}
		}
		// cout << "Node " << id << " Label " << label << endl;
	}

	virtual void printOrder()
	{
		for (size_t i = 0; i < this->dfs_order.size(); i++)
		{
			SINode* curr = &this->nodes[this->dfs_order[i]];
			cout << "Node " << i << endl;
			cout << *curr << "It has " << curr->children.size() << " children." << endl;
		}
	}

	void dfs(int currID, int parentID, bool isRoot)
	{
		// recursive function to implement depth-first search.
		// only called when current node is not visited.
		SINode* curr = &this->nodes[currID];
		curr->visited = true;
		curr->dfs_number = this->dfs_order.size();
		dfs_order.push_back(currID);

		if (isRoot)
			curr->level = 0;
		else
		{
			curr->parent = parentID;
			SINode* parent = & this->nodes[parentID];
			parent->children.push_back(currID);
			curr->level = parent->level + 1;
		}

		// we must have two loops to avoid descendants being visited
		// by other descendants.
		// the first loop also stores unvisited neighbor's degree.
		vector<pair<int, int> > unv_nbs_degree;

		for (vector<int>::iterator it = curr->nbs.begin();
				it != curr->nbs.end(); it++)
		{
			if (this->nodes[*it].visited)
				curr->b_nbs.push_back(*it);
			else
				unv_nbs_degree.push_back(
						make_pair(*it, this->nodes[*it].nbs.size()));
		}

		sort(unv_nbs_degree.begin(), unv_nbs_degree.end(), sortByValDesc);

		for (vector<pair<int, int> >::iterator it = unv_nbs_degree.begin();
				it != unv_nbs_degree.end(); it++)
		{
			if (! this->nodes[it->first].visited)
			{
				this->dfs(it->first, currID, false);
			}
		}
	}

	void addBranchNumber(int currID, int num, int ancID)
	{
		// recursive function to add branch number.
		this->nbancestors[currID] = ancID;

		SINode* curr = &this->nodes[currID];
		if (curr->children.size() > 1)
		{
			num ++;
			ancID = currID;
		}

		curr->branch_number = num;

		if (num > this->max_branch_number)
			this->max_branch_number = num;
		for (size_t i = 0; i < curr->children.size(); i++)
			this->addBranchNumber(curr->children[i], num, ancID);
	}

	// Query is read-only, so a lot of get functions
	int getLabel(int id) { return this->nodes[id].label; }
	int getLevel(int id) { return this->nodes[id].level; }
	int getBranchNumber(int id) { return this->nodes[id].branch_number;	}
	int getDFSNumber(int id) { return this->nodes[id].dfs_number; }
	int getParent(int id) { return this->nodes[id].parent; }
	int getChildrenNumber(int id)
	{ return (int) this->nodes[id].children.size(); }
	int getChildID(int id, int index)
	{ return this->nodes[id].children[index]; }
	vector<int> getBNeighbors(int id)
	{ return this->nodes[id].b_nbs; }
	int getNearestBranchingAncestor(int id)
	{ return this->nbancestors[id]; }
};

ibinstream & operator<<(ibinstream & m, const SIQuery & q){
	m << q.root << q.nodes << q.max_branch_number << q.dfs_order
	  << q.nbancestors;
	return m;
}

obinstream & operator>>(obinstream & m, SIQuery & q){
	m >> q.root >> q.nodes >> q.max_branch_number >> q.dfs_order
	  >> q.nbancestors;
	return m;
}

//===============================================================

class SIVertex:public Vertex<VertexID, SIValue, SIMessage>
{
	public:
		// used in the final step:
		// key = query_vertex, value = vector of path mapping
		int root_query_vertex;
		hash_map<int, vector<vector<VertexID>>> results;

		void continue_mapping(vector<VertexID> &mapping, int &curr_u)
		{ // Add current vertex to mapping;
		  // Send messages to neighbors with label of next_u(next query vertex).
			hash_map<VertexID, int> & nbs = value().neighbors;
			hash_map<VertexID, int>::iterator it;
			SIQuery* query = (SIQuery*)getQuery();

			mapping.push_back(id);
#ifdef DEBUG_MODE_PARTIAL_RESULT
			cout << "[Result] Current query vertex: " << curr_u << " Partial mapping: " << mapping << endl;
#endif

			int children_num = query->getChildrenNumber(curr_u);
			if (children_num == 0)
			{ // leaf query vertex
				this->results[curr_u].push_back(mapping);
			}
			else
			{
				int next_u;
				for (int i = 0; i < children_num; i++)
				{ // for every child in query graph
					next_u = query->getChildID(curr_u, i);
					SIMessage msg = SIMessage(MAPPING, mapping, next_u);
					for (it = nbs.begin(); it != nbs.end(); it++)
					{
						if (it->second == query->getLabel(next_u) &&
							notContains(mapping, it->first))
						{ // check for label and uniqueness
							send_message(it->first, msg);
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Message sent from " << id << " to "
							 << it->first << ". \n\t"
							 << "Type: MAPPING. \n\t"
							 << "Mapping: " << msg.mapping << endl;
#endif
						}
					}
				}
			}
		}

		virtual void preprocess(MessageContainer & messages)
		{
			hash_map<VertexID, int> & nbs = value().neighbors;
			hash_map<VertexID, int>::iterator it;

#ifdef DEBUG_MODE_ACTIVE
				cout << "[DEBUG] STEP NUMBER " << step_num()
					 << " ACTIVE Vertex ID " << id << endl;
#endif

			if (step_num() == 1)
			{ // send label info to neighbors
				SIMessage msg = SIMessage(LABEL_INFOMATION, id, value().label);
				for (it = nbs.begin(); it != nbs.end(); it++)
					send_message(it->first, msg);
				vote_to_halt();
			}
			else // if (step_num() == 2)
			{   // receive label info from neighbors
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					if (msg.type == LABEL_INFOMATION)
						nbs[msg.vertex] = msg.label;
				}
				vote_to_halt();
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			hash_map<VertexID, int> & nbs = value().neighbors;
			SIQuery* query = (SIQuery*)getQuery();

#ifdef DEBUG_MODE_ACTIVE
				cout << "[DEBUG] STEP NUMBER " << step_num()
					 << " ACTIVE Vertex ID " << id << endl;
#endif

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
			{   // check if backward neighbors in neighbors
				int curr_level, nb_level;
				bool flag;
				vector<int> b_nbs;
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					b_nbs = query->getBNeighbors(msg.vertex);
					curr_level = query->getLevel(msg.vertex);
					flag = true;
					for (size_t i = 0; i < b_nbs.size(); i++)
					{
						nb_level = query->getLevel(b_nbs[i]);
						if ((nb_level != curr_level - 1) &&
								(nbs.find(msg.mapping[nb_level]) == nbs.end()))
						{
							flag = false;
							break;
						}
					}
					if (flag) continue_mapping(msg.mapping, msg.vertex);
				}
				vote_to_halt();
			}
		}

		virtual void enumerate(MessageContainer & messages)
		{
			SIQuery* query = (SIQuery*)getQuery();
			hash_map<vector<VertexID>,
			hash_map<int, vector<vector<VertexID> > > > join_results;
			hash_map<vector<VertexID>,
			hash_map<int, vector<vector<VertexID> > > >::iterator join_it;
			hash_map<int, vector<vector<VertexID>>>::iterator it;
			vector<pair<int, vector<vector<VertexID> > > > tails;
			vector<VertexID> prefix, tail;
			vector<vector<VertexID> > temp_results;
			int num, curr_u, anc_u, to_send;

			// join operations for supersteps [2, max_branch_number + 1]
			if (step_num() > 1 && step_num() < query->max_branch_number + 2)
			{
				//cout << "0. step_num = " << step_num() << endl;
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					curr_u = msg.vertex;
					anc_u = query->getNearestBranchingAncestor(curr_u);
					for (size_t i = 0; i < msg.branch_result.size(); i++)
					{
						vector<VertexID> & mapping = msg.branch_result[i];
						for (int j = 0; j <= query->getLevel(anc_u); j++)
							prefix.push_back(mapping[j]);
						for (int j = query->getLevel(anc_u) + 1;
							 j < mapping.size(); j++)
							tail.push_back(mapping[j]);
						//cout << "4. prefix should be [1]: " << prefix << endl;
						//cout << "5. tail should be [5] or [6] or [7]: " << tail << endl;
						join_results[prefix][curr_u].push_back(tail);
						prefix.clear();
						tail.clear();
					}
				}

				for (join_it = join_results.begin(); join_it != join_results.end();
						join_it ++)
				{
					prefix = join_it->first;
					it = join_it->second.begin();
					anc_u = query->getNearestBranchingAncestor(it->first);
					//cout << "6. it->first (curr_u): " << it->first << endl;
					//cout << "7. anc_u: " << anc_u << endl;

					for (; it != join_it->second.end(); it++)
					{
						num = query->getDFSNumber(it->first);
						tails.push_back(make_pair(num, it->second));
					}

					sort(tails.begin(), tails.end());
					//cout << "8. tails[0].first should be 1: " << tails[0].first << endl;
					//cout << "9. tails[0].second[0] should be [5] or [9]: " << (tails[0].second)[0] << endl;

					// make sure every child sends you result!
					if (tails.size() == query->getChildrenNumber(anc_u)
							&& tails.size() > 1)
					{
						temp_results = joinVectors(prefix, tails[0].second,
								tails[1].second);
						for (size_t i = 2; i < tails.size(); i++)
						{
							vector<VertexID> head_v;
							temp_results = joinVectors(head_v,
									temp_results, tails[i].second);
						}
						if (temp_results.size() != 0)
						//cout << "10. temp_results[0]: " << temp_results[0] << endl;
						this->results[anc_u].insert(this->results[anc_u].end(),
								temp_results.begin(), temp_results.end());
					}
					tails.clear();
				}
			}

			// send messages for supersteps [1, max_branch_number]
			if (step_num() > (query->max_branch_number))
			{
				vote_to_halt();
			}
			else
			{
				for (it = this->results.begin(); it != this->results.end(); it++)
				{
					curr_u = it->first;
					num = query->getBranchNumber(curr_u);
					//cout << "1. curr_u: " << curr_u << " branch_num: " << num << endl;
					if (num != 0 && num > query->max_branch_number - step_num()
							&& !it->second.empty())
					{ // non-empty to guarantee that query vertex is a leaf vertex
						anc_u = query->getNearestBranchingAncestor(curr_u);
						//cout << "2. anc_u should be 1: " << anc_u << endl;
						to_send = it->second[0][query->getLevel(anc_u)];
						//cout << "3. to_send should be 1 or 2: " << to_send << endl;
						send_message(to_send,
								SIMessage(BRANCH_RESULT, it->second, curr_u));
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Message sent from " << id << " to "
							 << to_send << ". \n\t"
							 << "Type: BRANCH RESULT. \n\t"
							 << "The first of it " << it->second[0] << endl;
#endif
						this->results[curr_u].clear();
					}
				}
			}
		}
};

//=============================================================================

class SIWorker:public Worker<SIVertex, SIQuery>
{
	char buf[100];

	public:
		// C version
		// input line format:
		// vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
		virtual SIVertex* toVertex(char* line)
		{
			char * pch;
			SIVertex* v = new SIVertex;

			pch = strtok(line, " ");
			v->id = atoi(pch);

			pch = strtok(NULL, " ");
			v->value().label = atoi(pch);

			pch = strtok(NULL, " ");
			int num = atoi(pch);
			for (int k = 0; k < num; k++)
			{
				pch=strtok(NULL, " ");
				int neighbor = atoi(pch);
				v->value().neighbors[neighbor] = 0;
			}
			return v;
		}

		virtual void toline(SIVertex* v, BufferedWriter & writer)
		{
			hash_map<int, vector<vector<VertexID>>>::iterator it;
			SIQuery* query = (SIQuery*)getQuery();
			for (it = v->results.begin(); it != v->results.end(); it++)
			{
				vector<vector<VertexID>> & results = it->second;
				for (size_t i = 0; i < results.size(); i++)
				{
					vector<VertexID> & mapping = results[i];
					sprintf(buf, "# Match\n");
					writer.write(buf);

#ifdef DEBUG_MODE_RESULT
					cout << "[DEBUG] Vertex ID: " << v->id << endl;
					cout << "[DEBUG] Result: " << mapping << endl;
#endif
					for (size_t j = 0; j < mapping.size(); j++)
					{
						sprintf(buf, "%d %d\n", query->dfs_order[j], mapping[j]);
						writer.write(buf);
					}
				}
			}
		}
};

/*
class CCCombiner_pregel:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg)
		{
			if(old>new_msg) old=new_msg;
		}
};
*/

void pregel_subgraph(string data_path, string query_path, string out_path,
		bool force_write)
{
	SIWorker worker;
	//CCCombiner_pregel combiner;
	//if(use_combiner) worker.setCombiner(&combiner);

	//SIAgg agg;
	//worker.setAggregator(&agg);

	SIQuery query;
	worker.setQuery(&query);

	worker.load_data(data_path);
	worker.run_type(PREPROCESS);

	worker.load_query(query_path);
	wakeAll();
	worker.run_type(MATCH);
	wakeAll();
	worker.run_type(ENUMERATE);
	worker.dump_graph(out_path, force_write);
}
