#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//#define DEBUG_MODE_ACTIVE 1
#define DEBUG_MODE_MSG 1
#define DEBUG_MODE_PARTIAL_RESULT 1
#define DEBUG_MODE_RESULT 1

typedef vector<VertexID> Mapping;
typedef hash_map<int, vector<Mapping> > Result;

// the first int for anc_u, the second int for curr_u's branch_num.
typedef hash_map<int, map<int, vector<Mapping> > > mResult;

//input line format:
//  vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
//output line format:
//  # MATCH
//  query_vertexID \t data_vertexID

//--------------------SIKey = <VertexID, partial_mapping>--------------------

struct SIKey {
    VertexID vID;
    vector<int> partial_mapping;

    SIKey()
    {
    }

    SIKey(int v)
    {
    	this->vID = v;
    }

    SIKey(int v, vector<int> & partial_mapping)
    {
        this->vID = v;
        this->partial_mapping = partial_mapping;
    }

    inline bool operator<(const SIKey& rhs) const
    {
        return (vID < rhs.vID);
    }

    inline bool operator>(const SIKey& rhs) const
    {
        return (vID > rhs.vID);
    }

    inline bool operator==(const SIKey& rhs) const
    {
        return (vID == rhs.vID) && (partial_mapping == rhs.partial_mapping);
    }

    inline bool operator!=(const SIKey& rhs) const
    {
        return (vID != rhs.vID) || (partial_mapping != rhs.partial_mapping);
    }

    int hash()
    {
    	int seed = vID;
    	for (size_t i = 0; i < partial_mapping.size(); i++)
    		seed += partial_mapping[i];
    	return seed % ((unsigned int)_num_workers);
    }
};

ibinstream& operator<<(ibinstream& m, const SIKey& v)
{
    m << v.vID;
    cout << "Successful: v.vID: " << v.vID << endl;
    m << v.partial_mapping;
    cout << "Successful: v.mapping: " << v.partial_mapping << endl;
    return m;
}

obinstream& operator>>(obinstream& m, SIKey& v)
{
    m >> v.vID;
    cout << "Successful: v.vID: " << v.vID << endl;
    m >> v.partial_mapping;
    cout << "Successful: v.mapping: " << v.partial_mapping << endl;
    return m;
}

class SIKeyHash {
public:
    inline int operator()(SIKey key)
    {
        return key.hash();
    }
};

namespace __gnu_cxx {
	template <>
	struct hash<SIKey> {
		size_t operator()(SIKey key) const
		{
	        size_t seed = 0;
	        hash_combine(seed, key.vID);
	        for (size_t i = 0; i < key.partial_mapping.size(); i++)
	        	hash_combine(seed, key.partial_mapping[i]);
	        return seed % ((unsigned int)_num_workers);
		}
	};
}

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

//==========================================================================

struct SIBranch;
vector<Mapping> crossJoin(vector<Mapping> v1, vector<SIBranch> b);

struct SIBranch
{
	Mapping p;
	vector<vector<SIBranch> > branches;

	SIBranch() {};

	SIBranch(Mapping p)
	{
		this->p = p;
	}

	void addBranch(vector<SIBranch> branch)
	{
		this->branches.push_back(branch);
	}

	vector<Mapping> expand()
	{
		vector<Mapping> v;
		v.push_back(this->p);
		for (size_t i = 0; i < branches.size(); i++)
			v = crossJoin(v, this->branches[i]);

		return v;
	}
};

vector<Mapping> crossJoin(vector<Mapping> v1, vector<SIBranch> b)
{
	vector<Mapping> results;
	Mapping m;
	for (size_t i = 0; i < v1.size(); i++)
	{
		for (size_t j = 0; j < b.size(); j++)
		{
			vector<Mapping> v2 = b[j].expand();
			for (size_t k = 0; k < v2.size(); k++)
			{
				m = v1[i];
				m.insert(m.end(), v2[k].begin(), v2[k].end());
				if (notContainsDuplicate(m)) results.push_back(m);
			}
		}
	}
	return results;
}

ibinstream& operator<<(ibinstream& m, const SIBranch& branch)
{
    m << branch.p;
    cout << "Successful: branch: " << branch.p << endl;

    m << branch.branches.size();

    for (size_t i = 0; i < branch.branches.size(); i++)
    {
    	m << branch.branches[i].size();

    	for (size_t j = 0; j < branch.branches[i].size(); j++)
		{
    		m << branch.branches[i][j];
		}
    }

    return m;
}

obinstream& operator>>(obinstream& m, SIBranch& branch)
{
    m >> branch.p;
    cout << "Successful: branch: " << branch.p << endl;

    size_t size;
    m >> size;
    branch.branches.resize(size);

    for (size_t i = 0; i < branch.branches.size(); i++)
    {
    	m >> size;
    	branch.branches[i].resize(size);

    	for (size_t j = 0; j < branch.branches[i].size(); j++)
		{
    		m >> branch.branches[i][j];
		}
    }

    return m;
}

//--------SIMessage = <type, vertex, label, mapping, branch>--------
//  e.g. <type = LABEL_INFOMATION, vertex, label>
//	     <type = MAPPING, mapping, vertex = next_u>
//		 <type = BRANCH_RESULT, mapping, vertex = curr_u>
// 		 <type = BRANCH, branch, vertex = curr_u>

struct SIMessage
{
	int type;
	VertexID vertex;
	int label;
	vector<VertexID> mapping;
	SIBranch branch;

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
	{ // for mapping or branch result
		this->type = type;
		this->mapping = mapping;
		this->vertex = next_u; // or curr_u
	}

	SIMessage(int type, SIBranch branch, VertexID curr_u)
	{ // for new enumeration
		this->type = type;
		this->branch = branch;
		this->vertex = curr_u;
	}
};

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 0,
	MAPPING = 1,
	BRANCH_RESULT = 2,
	BRANCH = 3
};

ibinstream & operator<<(ibinstream & m, const SIMessage & v)
{
	m << v.type;
	if (v.type == MESSAGE_TYPES::LABEL_INFOMATION)
		m << v.vertex << v.label;
	else if (v.type == MESSAGE_TYPES::MAPPING
			|| v.type == MESSAGE_TYPES::BRANCH_RESULT)
		m << v.mapping << v.vertex;
	else if (v.type == MESSAGE_TYPES::BRANCH)
		m << v.branch << v.vertex;

	return m;
}

obinstream & operator>>(obinstream & m, SIMessage & v)
{
	m >> v.type;
	if (v.type == MESSAGE_TYPES::LABEL_INFOMATION)
		m >> v.vertex >> v.label;
	else if (v.type == MESSAGE_TYPES::MAPPING
			|| v.type == MESSAGE_TYPES::BRANCH_RESULT)
		m >> v.mapping >> v.vertex;
	else if (v.type == MESSAGE_TYPES::BRANCH)
		m >> v.branch >> v.vertex;

	return m;
}



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

	bool isBranch(int id)
	{ return this->getChildrenNumber(id) > 1; }
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

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
	public:
		// used in the final step:
		// key = query_vertex, value = vector of path mapping
		int root_query_vertex;
		Result results;

		void continue_mapping(vector<VertexID> &mapping, int &curr_u,
				bool add_flag)
		{ // Add current vertex to mapping;
		  // Send messages to neighbors with label of next_u(next query vertex)
		  // if add_flag, add a dummy vertex for each mapping.
			hash_map<VertexID, int> & nbs = value().neighbors;
			hash_map<VertexID, int>::iterator it;
			SIQuery* query = (SIQuery*)getQuery();

			mapping.push_back(id.vID);
#ifdef DEBUG_MODE_PARTIAL_RESULT
			cout << "[Result] Current query vertex: " << curr_u << " Partial mapping: " << mapping << endl;
#endif
			if (add_flag)
			{
				SIVertex* v = new SIVertex;
				v->id = SIKey(id.vID, mapping);
				this->add_vertex(v);
			}

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
							send_message(SIKey(it->first), msg);
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Message sent from " << id.vID << " to "
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
					 << " ACTIVE Vertex ID " << id.vID << endl;
#endif

			if (step_num() == 1)
			{ // send label info to neighbors
				SIMessage msg = SIMessage(LABEL_INFOMATION, id.vID, value().label);
				for (it = nbs.begin(); it != nbs.end(); it++)
					send_message(SIKey(it->first), msg);
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

		virtual void compute(MessageContainer &messages, WorkerParams &params)
		{
			hash_map<VertexID, int> & nbs = value().neighbors;
			SIQuery* query = (SIQuery*)getQuery();
			bool add_flag; // add a dummy vertex for each mapping

#ifdef DEBUG_MODE_ACTIVE
				cout << "[DEBUG] STEP NUMBER " << step_num()
					 << " ACTIVE Vertex ID " << id.vID << endl;
#endif

			if (step_num() == 1)
			{   // start mapping with vertices with same label
				int root_u = query->root;
				int root_label = query->getLabel(root_u);
				add_flag = params.enumerate && query->isBranch(root_u);
				if (value().label == root_label)
				{
					vector<VertexID> mapping;
					continue_mapping(mapping, root_u, add_flag);
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
					if (flag)
					{
						add_flag = params.enumerate &&
								query->isBranch(msg.vertex);
						continue_mapping(msg.mapping, msg.vertex,
								add_flag);
					}
				}
				vote_to_halt();
			}
		}

		void enumerate_new(MessageContainer & messages)
		{
			SIQuery* query = (SIQuery*)getQuery();
			hash_map<int, vector<vector<VertexID> > >::iterator it;
			int num, curr_u, anc_u, to_v, j;
			// hash_map<curr_u, map<chd_u's DFS number, vector<SIBranch> > >
			hash_map<int, map<int, vector<SIBranch> > > u_children;
			hash_map<int, map<int, vector<SIBranch> > >::iterator it1;
			map<int, vector<SIBranch> >::iterator it2;

			hash_set<int> delete_set; // hash map keys to be deleted

			// send mappings from the leaves for superstep [1, max_num_number]
			if (step_num() > 0 && step_num() < query->max_branch_number + 1)
			{
				for (it = this->results.begin(); it != this->results.end();
						it++)
				{
					curr_u = it->first;
					num = query->getBranchNumber(curr_u);
					if (num != 0 && num > query->max_branch_number - step_num()
							&& !it->second.empty())
					{ // non-empty to guarantee that query vertex is a leaf
						anc_u = query->getNearestBranchingAncestor(curr_u);
						for (size_t i = 0; i < it->second.size(); i++)
						{
							Mapping m1, m2;
							Mapping & m = it->second[i];
							to_v = m[query->getLevel(anc_u)];
							for (j = 0; j <= query->getLevel(anc_u); j++)
								m1.push_back(m[j]);
							for (; j < (int) m.size(); j++)
								m2.push_back(m[j]);
							SIBranch b = SIBranch(m2);
							send_message(SIKey(to_v, m1),
								SIMessage(BRANCH, b, curr_u));
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Superstep " << step_num()
							 << "\n\tMessage sent from (leaf) " << id.vID
							 <<	" to <" << to_v << ", " << m1 << ">."
							 << "\n\tType: BRANCH. "
							 << "\n\tMapping: " << m2 << endl;
#endif
						}
						delete_set.insert(curr_u);
					} // end of if
				} // end of for loop

				for (hash_set<int>::iterator it = delete_set.begin();
						it != delete_set.end(); ++it)
				{
					this->results.erase(*it);
					cout << "curr_u: " << *it << " erased." << endl;
				}
			}

			// receive msg and join for supersteps [2, max_branch_number + 1]
			// send msg to ancestor for supersteps [2, max_branch_number]
			if (step_num() > 1 && step_num() < query->max_branch_number + 2)
			{
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					curr_u = query->getNearestBranchingAncestor(msg.vertex);
					num = query->getDFSNumber(msg.vertex);
					u_children[curr_u][num].push_back(msg.branch);
				}

				for (it1 = u_children.begin(); it1 != u_children.end();
						it1++)
				{
					Mapping m1, m2;
					Mapping &m = id.partial_mapping;
					curr_u = it1->first;

					// make sure every child sends you result!
					if ((int) it1->second.size() !=
							query->getChildrenNumber(curr_u))
						return;

					if (step_num() == query->max_branch_number + 1)
					{
						SIBranch b = SIBranch(m);
						for (it2 = it1->second.begin();
								it2 != it1->second.end(); it2++)
							b.addBranch(it2->second);
						this->results[curr_u] = b.expand();
					}
					else
					{
						anc_u = query->getNearestBranchingAncestor(curr_u);
						to_v = m[query->getLevel(anc_u)];
						for (j = 0; j <= query->getLevel(anc_u); j++)
							m1.push_back(m[j]);
						for (; j < (int) m.size(); j++)
							m2.push_back(m[j]);
						SIBranch b = SIBranch(m2);
						for (it2 = it1->second.begin();
								it2 != it1->second.end(); it2++)
							b.addBranch(it2->second);
						send_message(SIKey(to_v, m1),
								SIMessage(BRANCH, b, curr_u));
#ifdef DEBUG_MODE_MSG
					cout << "[DEBUG] Superstep " << step_num()
						 << "\n\tMessage sent from (branching) " << id.vID
						 <<	" to <" << to_v << ", " << m1 << ">. \n\t"
						 << "Type: BRANCH. \n\t"
						 << "Mapping: " << m2 << endl;
#endif
					}
				}
			}

			if (this->results.empty() ||
					step_num() > (query->max_branch_number))
			{
				vote_to_halt();
			}
		}

		void enumerate_old(MessageContainer & messages)
		{
			SIQuery* query = (SIQuery*)getQuery();

			hash_map<Mapping, mResult> join_results;
			hash_map<Mapping, mResult>::iterator join_it;
			Mapping prefix, tail;
			vector<Mapping> temp_results;
			int num, curr_u, anc_u, to_send;

			// join operations for supersteps [2, max_branch_number + 1]
			if (step_num() > 1 && step_num() < query->max_branch_number + 2)
			{
				// sort out all the messages, classify them according to prefix
				// and curr_u, store in join_results
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					curr_u = msg.vertex;
					anc_u = query->getNearestBranchingAncestor(curr_u);

					Mapping & mapping = msg.mapping;
					for (int j = 0; j <= query->getLevel(anc_u); j++)
						prefix.push_back(mapping[j]);
					for (size_t j = query->getLevel(anc_u) + 1;
						 j < mapping.size(); j++)
						tail.push_back(mapping[j]);

					num = query->getDFSNumber(curr_u);
					join_results[prefix][anc_u][num].push_back(tail);
					prefix.clear();
					tail.clear();
				}

				// for each prefix and its tails, perform cross join.
				for (join_it = join_results.begin();
						join_it != join_results.end();
						join_it ++)
				{
					prefix = join_it->first;
					for (mResult::iterator it = join_it->second.begin();
							it != join_it->second.end(); it++)
					{
						anc_u = it->first;
						// make sure every child sends you result!
						if ((int) it->second.size() ==
								query->getChildrenNumber(anc_u))
						{
							temp_results.push_back(prefix);
							for (map<int, vector<Mapping> >::iterator
									i = it->second.begin();
									i != it->second.end(); i++)
							{
								temp_results = joinVectors(
										temp_results, i->second);
							}
							this->results[anc_u].insert(
									this->results[anc_u].end(),
									temp_results.begin(), temp_results.end());
							temp_results.clear();
						}
					}
				}
			}

			// send messages for supersteps [1, max_branch_number]
			if (step_num() > (query->max_branch_number))
			{
				vote_to_halt();
			}
			else
			{
				for (Result::iterator it = this->results.begin();
						it != this->results.end(); it++)
				{
					curr_u = it->first;
					num = query->getBranchNumber(curr_u);
					if (num != 0 && num > query->max_branch_number - step_num()
							&& !it->second.empty())
					{ // non-empty to guarantee that query vertex is a leaf
						anc_u = query->getNearestBranchingAncestor(curr_u);
						for (size_t i = 0; i < it->second.size(); i++)
						{
							to_send = it->second[i][query->getLevel(anc_u)];
							send_message(SIKey(to_send),
								SIMessage(BRANCH_RESULT, it->second[i], curr_u));
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Message sent from " << id.vID << " to "
							 << to_send << ".\n\t"
							 << "Type: BRANCH RESULT. \n\t"
							 << "curr_u: " << curr_u << endl;
#endif
						}
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
			v->id = SIKey(atoi(pch));

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
			hash_map<int, vector<Mapping> >::iterator it;
			SIQuery* query = (SIQuery*)getQuery();
			for (it = v->results.begin(); it != v->results.end(); it++)
			{
				vector<Mapping> & results = it->second;
				for (size_t i = 0; i < results.size(); i++)
				{
					Mapping & mapping = results[i];
					sprintf(buf, "# Match\n");
					writer.write(buf);

#ifdef DEBUG_MODE_RESULT
					cout << "[DEBUG] Vertex ID: " << v->id.vID << endl;
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

void pregel_subgraph(const WorkerParams & params)
{
	SIWorker worker;
	//CCCombiner_pregel combiner;
	//if(use_combiner) worker.setCombiner(&combiner);

	//SIAgg agg;
	//worker.setAggregator(&agg);

	SIQuery query;
	worker.setQuery(&query);

	worker.load_data(params.data_path);
	worker.run_type(PREPROCESS, params);

	worker.load_query(params.query_path);
	wakeAll();
	worker.run_type(MATCH, params);
	wakeAll();
	worker.run_type(ENUMERATE, params);
	worker.dump_graph(params.output_path, params.force_write);
}
