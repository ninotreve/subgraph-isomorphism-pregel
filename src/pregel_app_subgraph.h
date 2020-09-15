#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

//#define DEBUG_MODE_ACTIVE 1
//#define DEBUG_MODE_MSG 1
//#define DEBUG_MODE_PARTIAL_RESULT 1
//#define DEBUG_MODE_RESULT 1

//input line format:
//  vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
//output line format:
//  # MATCH
//  query_vertexID \t data_vertexID

//----------------SIKey = <VertexID, WorkerID, partial_mapping>----------

struct SIKey;
typedef vector<SIKey> Mapping;
typedef hash_map<int, hash_set<SIKey> > Candidate;
typedef hash_map<int, vector<Mapping> > Result;
typedef hash_map<pair<int, int>, int> AggMap;

// the first int for anc_u, the second int for curr_u's branch_num.
typedef hash_map<int, map<int, vector<Mapping> > > mResult;

struct SIKey {
    VertexID vID;
    int wID;
    Mapping partial_mapping;

    SIKey()
    {
    }

    SIKey(int v, int w)
    {
    	this->vID = v;
    	this->wID = w;
    }

    SIKey(int v, int w, Mapping & partial_mapping)
    {
        this->vID = v;
        this->wID = w;
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
    	return wID;
    }
};

ibinstream& operator<<(ibinstream& m, const SIKey& v)
{
    m << v.vID;
    m << v.wID;
    m << v.partial_mapping;
    return m;
}

obinstream& operator>>(obinstream& m, SIKey& v)
{
    m >> v.vID;
    m >> v.wID;
    m >> v.partial_mapping;
    return m;
}

class SIKeyHash {
public:
    inline int operator()(SIKey key)
    {
    	// this hash for partitioning vertices
        return key.hash();
    }
};

namespace __gnu_cxx {
	template <>
	struct hash<SIKey> {
		size_t operator()(SIKey key) const
		{
			// this is general hash
	        size_t seed = 0;
	        hash_combine(seed, key.vID);
	        for (SIKey &k : key.partial_mapping)
	        	hash_combine(seed, k.vID);
	        return seed;
		}
	};
}

//==========================================================================
// Define hash of Mapping

namespace __gnu_cxx {
	template <>
	struct hash<Mapping> {
		size_t operator()(Mapping m) const
		{
			size_t seed = 0;
			for (SIKey &k : m)
				hash_combine(seed, k.vID);
			return seed;
		}
	};
}

// Define hash of pair

namespace __gnu_cxx {
	template <>
	struct hash<pair<int, int>> {
		size_t operator()(pair<int, int> p) const
		{
			size_t seed = 0;
			hash_combine(seed, p.first);
			hash_combine(seed, p.second);
			return seed;
		}
	};
}

//------------SIValue = <label, hash_map<neighbors, labels> >------------------

struct SIValue
{
	int label;
	hash_map<SIKey, int> neighbors;
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
vector<Mapping> crossJoin(vector<Mapping> v1, vector<SIBranch> &b);

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
		for (vector<SIBranch> &b : this->branches)
			v = crossJoin(v, b);

		return v;
	}
};

vector<Mapping> crossJoin(vector<Mapping> v1, vector<SIBranch> &vecB)
{
	vector<Mapping> results;
	Mapping m;
	for (size_t i = 0; i < v1.size(); i++)
	{
		for (SIBranch &b : vecB)
		{
			vector<Mapping> v2 = b.expand();
			for (Mapping &m2 : v2)
			{
				m = v1[i];
				m.insert(m.end(), m2.begin(), m2.end());
				if (notContainsDuplicate(m)) results.push_back(m);
			}
		}
	}
	return results;
}

ibinstream& operator<<(ibinstream& m, const SIBranch& branch)
{
    m << branch.p;
    m << branch.branches.size();
    for (vector<SIBranch> vecB : branch.branches)
    {
    	m << vecB.size();
    	for (SIBranch &b : vecB)	m << b;
    }

    return m;
}

obinstream& operator>>(obinstream& m, SIBranch& branch)
{
    m >> branch.p;
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

//--------SIMessage = <type, key, value, mapping, branch>--------
//  e.g. <type = LABEL_INFOMATION, key = vertex, value = label>
//	     <type = MAPPING, mapping, value = next_u>
//		 <type = BRANCH_RESULT, mapping, value = curr_u>
// 		 <type = BRANCH, branch, value = curr_u>
//		 <type = MAPPING_COUNT, value = count>
//		 <type = CANDIDATE, key = vertex, v_int = candidates of v>

struct SIMessage
{
	int type;
	SIKey key;
	int value;
	vector<int> v_int;
	vector<SIKey> mapping;
	SIBranch branch;

	SIMessage()
	{
	}

	SIMessage(int type, SIKey vertex, int label)
	{ // for label information
		this->type = type;
		this->key = vertex;
		this->value = label;
	}

	SIMessage(int type, Mapping mapping, uID next_u)
	{ // for mapping or branch result
		this->type = type;
		this->mapping = mapping;
		this->value = next_u; // or curr_u
	}

	SIMessage(int type, SIBranch branch, uID curr_u)
	{ // for new enumeration
		this->type = type;
		this->branch = branch;
		this->value = curr_u;
	}

	SIMessage(int type, int label)
	{ // for mapping count
		this->type = type;
		this->value = label;
	}

	SIMessage(int type, SIKey vertex)
	{ // for candidate initialization
		this->type = type;
		this->key = vertex;
	}

	void add_int(int i)
	{
		this->v_int.push_back(i);
	}
};

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 0,
	MAPPING = 1,
	BRANCH_RESULT = 2,
	BRANCH = 3,
	MAPPING_COUNT = 4,
	CANDIDATE = 5
};

ibinstream & operator<<(ibinstream & m, const SIMessage & v)
{
	m << v.type;
	switch (v.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m << v.key << v.value;
		break;
	case MESSAGE_TYPES::MAPPING:
	case MESSAGE_TYPES::BRANCH_RESULT:
		m << v.mapping << v.value;
		break;
	case MESSAGE_TYPES::BRANCH:
		m << v.branch << v.value;
		break;
	case MESSAGE_TYPES::MAPPING_COUNT:
		m << v.value;
		break;
	case MESSAGE_TYPES::CANDIDATE:
		m << v.key << v.v_int;
		break;
	}
	return m;
}

obinstream & operator>>(obinstream & m, SIMessage & v)
{
	m >> v.type;
	switch (v.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m >> v.key >> v.value;
		break;
	case MESSAGE_TYPES::MAPPING:
	case MESSAGE_TYPES::BRANCH_RESULT:
		m >> v.mapping >> v.value;
		break;
	case MESSAGE_TYPES::BRANCH:
		m >> v.branch >> v.value;
		break;
	case MESSAGE_TYPES::MAPPING_COUNT:
		m >> v.value;
		break;
	case MESSAGE_TYPES::CANDIDATE:
		m >> v.key >> v.v_int;
		break;
	}
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

	SINode() { this->visited = false; }

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
	m << node.id << node.label << node.nbs;
    return m;
}

obinstream& operator>>(obinstream& m, SINode& node)
{
	m >> node.id >> node.label >> node.nbs;
    return m;
}


//==========================================================================
// Overloading << operator of mapping and vector<int>
// print like Python, for debug purpose

ostream & operator << (ostream & os, const Mapping & v)
{
	os << "[";
	for (size_t i = 0; i < v.size(); i++)
	{
		os << v[i].vID;
		if (i != (v.size() - 1)) os << ", ";
	}
	os << "]";
	return os;
}

ostream & operator << (ostream & os, const vector<int> & v)
{
	os << "[";
	for (size_t i = 0; i < v.size(); i++)
	{
		os << v[i];
		if (i != (v.size() - 1)) os << ", ";
	}
	os << "]";
	return os;
}

//==========================================================================
// Overloading << operator of SINode for debug purpose.
ostream & operator << (ostream & os, const SINode & node)
{
	os << "ID: " << node.id << endl;
	os << "Label: " << node.label << endl;
	os << "Branch number: " << node.branch_number << endl;
	os << "Neighbors: " << node.nbs << endl;
	return os;
}

//===========================================================

bool sortByVal(const pair<int, int> &a, const pair<int, int> &b)
{
	return (a.second < b.second);
}

class SIQuery:public Query<SINode>
{
public:
	hash_map<int, SINode> nodes;

	int root;
	int max_branch_number = 0;
	vector<int> dfs_order;
	// <vertex, nearest branch ancestor or root if it doesn't have one>
	hash_map<int, int> nbancestors;

	virtual void init(const string &order)
	{
		// order = "degree", value = degree
		// order = "candidate", value = candidate size
		if (! this->nodes.empty())
		{
			size_t value, min_value;
			for (auto it = this->nodes.begin(); it != this->nodes.end(); it++)
			{
				if (order == "degree")
					value = - it->second.nbs.size(); // default asc
				else
					value = (*((AggMap*)global_agg))[make_pair(
							it->first, it->first)];
				if (it == this->nodes.begin() || value < min_value)
				{
					min_value = value;
					this->root = it->first;
				}
			}
			this->dfs(this->root, 0, true, order);
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
		cout << "this->nodes size: " << this->nodes.size() << endl;
		for (size_t i = 0; i < this->dfs_order.size(); i++)
		{
			SINode* curr = &this->nodes[this->dfs_order[i]];
			cout << "Node " << i << endl;
			cout << *curr << "It has " << curr->children.size() <<
					" children." << endl;
		}
	}

	void dfs(int currID, int parentID, bool isRoot, const string &order)
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
		// the first loop also stores unvisited neighbor's value
		// (degree or candidate size).
		int value;
		vector<pair<int, int> > unv_nbs_value;

		for (int nextID : curr->nbs)
		{
			if (this->nodes[nextID].visited)
				curr->b_nbs.push_back(nextID);
			else
			{
				if (order == "degree")
					value = - this->nodes[nextID].nbs.size(); // default asc
				else
					value = (*((AggMap*)global_agg))[make_pair(currID, nextID)]
					     + (*((AggMap*)global_agg))[make_pair(nextID, currID)];
				unv_nbs_value.push_back(make_pair(nextID, value));
			}
		}

		sort(unv_nbs_value.begin(), unv_nbs_value.end(), sortByVal);

		for (auto it = unv_nbs_value.begin(); it != unv_nbs_value.end();
				it++)
		{
			if (! this->nodes[it->first].visited)
			{
				this->dfs(it->first, currID, false, order);
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

	// Query is read-only.
	// get functions before dfs.
	vector<int> getNbs(int id) { return this->nodes[id].nbs; }
	void LDFFilter(int label, size_t degree,
			hash_map<int, vector<int> > &cand_map)
	{
		vector<int> nbs;
		int curr_u, lab;
		for (auto nodeit = nodes.begin(); nodeit != nodes.end(); nodeit ++)
		{
			curr_u = nodeit->first;
			lab = nodeit->second.label;
			nbs = nodeit->second.nbs;
			if (lab == label && nbs.size() <= degree)
				cand_map[curr_u] = nbs;
		}
	}

	// get functions after dfs.
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
	if (q.dfs_order.empty())
	{
		m << 0;
		m << q.nodes;
	}
	else
	{
		m << 1;
		m << q.root << q.nodes << q.max_branch_number << q.dfs_order
		  << q.nbancestors;
	}
	return m;
}

obinstream & operator>>(obinstream & m, SIQuery & q){
	int type;
	m >> type;
	if (type == 0)
		m >> q.nodes;
	else
		m >> q.root >> q.nodes >> q.max_branch_number >> q.dfs_order
		  >> q.nbancestors;
	return m;
}

//===============================================================

vector<Mapping> joinVectors(vector<Mapping> & v1,
		vector<Mapping> & v2)
{
	// recursive function to join vectors
	vector<Mapping> results;
	Mapping v;

	for (size_t i = 0; i < v1.size(); i++)
	{
		for (size_t j = 0; j < v2.size(); j++)
		{
			v = v1[i];
			v.insert(v.end(), v2[j].begin(), v2[j].end());
			if (notContainsDuplicate(v))
				results.push_back(v);
		}
	}

	return results;
}

//===============================================================

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
	public:
		// used in the filtering step:
		// typedef hash_map<int, hash_set<SIKey> > Candidate;
		// candidates[curr_u][next_u] = vector<SIKey>
		hash_map<int, Candidate> candidates;
		// curr_u: vector<next_u>
		hash_map<int, vector<int> > cand_map;

		// used in the final step:
		// key = query_vertex, value = vector of path mapping
		int root_query_vertex;
		// typedef hash_map<int, vector<Mapping> > Result;
		Result results;

		void continue_mapping(vector<SIKey> &mapping, int &curr_u,
				bool add_flag, bool filter_flag)
		{ // Add current vertex to mapping;
		  // Send messages to neighbors with label of next_u(next query vertex)
		  // if add_flag, add a dummy vertex for each mapping.
			hash_map<SIKey, int>::iterator it;
			SIQuery* query = (SIQuery*)getQuery();

			mapping.push_back(id);
#ifdef DEBUG_MODE_PARTIAL_RESULT
			cout << "[Result] Current query vertex: " << curr_u << " Partial mapping: " << mapping << endl;
#endif
			if (add_flag)
			{
				SIVertex* v = new SIVertex;
				v->id = SIKey(id.vID, id.wID, mapping);
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
				{ // for every child (next_u) in query graph
					next_u = query->getChildID(curr_u, i);
					SIMessage msg = SIMessage(MAPPING, mapping, next_u);
					if (filter_flag)
					{ // with filtering
						hash_set<SIKey> &keys = candidates[curr_u][next_u];
						for (auto it = keys.begin(); it != keys.end(); it++)
						{
							if (notContains(mapping, *it))
							{
								send_message(*it, msg);
#ifdef DEBUG_MODE_MSG
							cout << "[DEBUG] Message sent from " << id.vID << " to "
								 << it->vID << ". \n\t"
								 << "Type: MAPPING. \n\t"
								 << "Mapping: " << msg.mapping << endl;
#endif
							}
						}
					}
					else
					{ // without filtering
						hash_map<SIKey, int> & nbs = value().neighbors;
						for (it = nbs.begin(); it != nbs.end(); it++)
						{
							if (it->second == query->getLabel(next_u) &&
								notContains(mapping, it->first))
							{ // check for label and uniqueness
								send_message(it->first, msg);
#ifdef DEBUG_MODE_MSG
							cout << "[DEBUG] Message sent from " << id.vID << " to "
								 << it->first.vID << ". \n\t"
								 << "Type: MAPPING. \n\t"
								 << "Mapping: " << msg.mapping << endl;
#endif
							}
						}
					}
				}
			}
		}

		virtual void preprocess(MessageContainer & messages)
		{
			hash_map<SIKey, int> & nbs = value().neighbors;
			hash_map<SIKey, int>::iterator it;

#ifdef DEBUG_MODE_ACTIVE
				cout << "[DEBUG] STEP NUMBER " << step_num()
					 << " ACTIVE Vertex ID " << id.vID << endl;
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
						nbs[msg.key] = msg.value;
				}
				vote_to_halt();
			}
		}

		virtual void compute(MessageContainer &messages, WorkerParams &params)
		{
			hash_map<SIKey, int> & nbs = value().neighbors;
			SIQuery* query = (SIQuery*)getQuery();
			bool add_flag; // add a dummy vertex for each mapping

#ifdef DEBUG_MODE_ACTIVE
				cout << "[DEBUG] STEP NUMBER " << step_num()
					 << " ACTIVE Vertex ID " << id.vID << endl;
#endif

			if (step_num() == 1)
			{
				int root_u = query->root;
				add_flag = params.enumerate && query->isBranch(root_u);
				if (params.filter)
				{ // with filtering
					if (!candidates[root_u].empty())
					{
						vector<SIKey> mapping;
						continue_mapping(mapping, root_u, add_flag,
								params.filter);
					}
				}
				else
				{ // without filtering, map with vertices with same label
					int root_label = query->getLabel(root_u);
					if (value().label == root_label)
					{
						vector<SIKey> mapping;
						continue_mapping(mapping, root_u, add_flag,
								params.filter);
					}
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
					b_nbs = query->getBNeighbors(msg.value);
					curr_level = query->getLevel(msg.value);
					flag = true;
					for (int b_nb : b_nbs)
					{
						nb_level = query->getLevel(b_nb);
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
								query->isBranch(msg.value);
						continue_mapping(msg.mapping, msg.value,
								add_flag, params.filter);
					}
				}
				vote_to_halt();
			}
		}

		void check_candidates(hash_set<int> &invalid_set)
		{
			int u1, u2;
			for (auto cand_it = cand_map.begin(); cand_it != cand_map.end();
					cand_it ++)
			{
				u1 = cand_it->first;
				for (int u : cand_it->second)
				{
					if (candidates[u1][u].empty())
					{
						invalid_set.insert(u1);
						break;
					}
				}
			}

			for (hash_set<int>::iterator set_it = invalid_set.begin();
					set_it != invalid_set.end(); set_it ++)
			{
				u1 = *set_it;
				for (Candidate::iterator it = candidates[u1].begin();
						it != candidates[u1].end(); it++)
				{
					u2 = it->first;
					for (SIKey k : it->second)
					{
						SIMessage msg = SIMessage(CANDIDATE, id);
						msg.add_int(u2);
						msg.add_int(u1);
						send_message(k, msg);
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Superstep " << step_num()
							 << "\n\tMessage sent from " << id.vID
							 <<	" to " << k.vID << "."
							 << "\n\tType: CANDIDATE. "
							 << "\n\tv_int: " << msg.v_int << endl;
#endif
					}
				}
			}
		}

		void filter(MessageContainer & messages)
		{
			SIQuery* query = (SIQuery*)getQuery();
			hash_map<SIKey, int> & nbs = value().neighbors;
			hash_map<SIKey, int>::iterator it;
			vector<int> temp_vec;

			if (step_num() == 1)
			{ // initialize candidates
				query->LDFFilter(value().label, nbs.size(), cand_map);
				SIMessage msg = SIMessage(CANDIDATE, id);
				for (hash_map<int, vector<int> >::iterator cand_it
						= cand_map.begin(); cand_it != cand_map.end();
						cand_it ++)
					msg.add_int(cand_it->first);

				if (!cand_map.empty())
				{
					for (it = nbs.begin(); it != nbs.end(); it++)
					{
						send_message(it->first, msg);
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Superstep " << step_num()
							 << "\n\tMessage sent from " << id.vID
							 <<	" to " << it->first.vID << "."
							 << "\n\tType: CANDIDATE. "
							 << "\n\tv_int: " << msg.v_int << endl;
#endif
					}
				}
				vote_to_halt();
			}
			else if (step_num() == 2)
			{ // initialize candidates
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					for (hash_map<int, vector<int> >::iterator cand_it
							= cand_map.begin(); cand_it != cand_map.end();
							cand_it ++)
					{
						vector<int> &a = cand_it->second;
						set_intersection(a.begin(), a.end(), msg.v_int.begin(),
								msg.v_int.end(), back_inserter(temp_vec));
						for (int u : temp_vec)
							candidates[cand_it->first][u].insert(msg.key);

						temp_vec.clear();
					}
				}

				hash_set<int> invalid_set; // invalid candidates
				check_candidates(invalid_set); // includes sending message

				for (hash_set<int>::iterator set_it = invalid_set.begin();
						set_it != invalid_set.end(); set_it ++)
				{
					candidates.erase(*set_it);
				}
				vote_to_halt();
			}
			else
			{ // filter candidates recursively
				for (SIMessage &msg : messages)
					candidates[msg.v_int[0]][msg.v_int[1]].erase(msg.key);

				hash_set<int> invalid_set; // invalid candidates
				check_candidates(invalid_set); // includes sending message

				for (auto set_it = invalid_set.begin();
						set_it != invalid_set.end(); set_it ++)
					candidates.erase(*set_it);

				vote_to_halt();
			}
		}

		void enumerate_new(MessageContainer & messages)
		{
			// return the number of mappings
			SIQuery* query = (SIQuery*)getQuery();
			hash_map<int, vector<Mapping> >::iterator it;
			int num, curr_u, anc_u, j;
			SIKey to_key;
			// hash_map<curr_u, map<chd_u's DFS number, vector<SIBranch> > >
			hash_map<int, map<int, vector<SIBranch> > > u_children;
			hash_map<int, map<int, vector<SIBranch> > >::iterator it1;
			map<int, vector<SIBranch> >::iterator it2;

			hash_set<int> delete_set; // hash map keys to be deleted

			// send mappings from leaves for supersteps [1, max_branch_number]
			if (step_num() > 0 && step_num() <= query->max_branch_number)
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
							to_key = m[query->getLevel(anc_u)];
							for (j = 0; j <= query->getLevel(anc_u); j++)
								m1.push_back(m[j]);
							for (; j < (int) m.size(); j++)
								m2.push_back(m[j]);
							SIBranch b = SIBranch(m2);
							send_message(SIKey(to_key.vID, to_key.wID, m1),
								SIMessage(BRANCH, b, curr_u));
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Superstep " << step_num()
							 << "\n\tMessage sent from (leaf) " << id.vID
							 <<	" to <" << to_key.vID << ", " << m1 << ">."
							 << "\n\tType: BRANCH. "
							 << "\n\tMapping: " << m2
							 << ", curr_u: " << curr_u << endl;
#endif
						}
						delete_set.insert(curr_u);
					} // end of if
				} // end of for loop

				for (hash_set<int>::iterator it = delete_set.begin();
						it != delete_set.end(); ++it)
				{
					this->results.erase(*it);
				}
			}

			// receive msg and join for supersteps [2, max_branch_number + 1]
			// send msg to ancestor for supersteps [2, max_branch_number]
			if (step_num() > 1 && step_num() <= query->max_branch_number + 1)
			{
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					curr_u = query->getNearestBranchingAncestor(msg.value);
					num = query->getDFSNumber(msg.value);
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
						continue;

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
						to_key = m[query->getLevel(anc_u)];
						for (j = 0; j <= query->getLevel(anc_u); j++)
							m1.push_back(m[j]);
						for (; j < (int) m.size(); j++)
							m2.push_back(m[j]);
						SIBranch b = SIBranch(m2);
						for (it2 = it1->second.begin();
								it2 != it1->second.end(); it2++)
							b.addBranch(it2->second);
						send_message(SIKey(to_key.vID, to_key.wID, m1),
								SIMessage(BRANCH, b, curr_u));
#ifdef DEBUG_MODE_MSG
					cout << "[DEBUG] Superstep " << step_num()
						 << "\n\tMessage sent from (branching) " << id.vID
						 <<	" to <" << to_key.vID << ", " << m1 << ">. \n\t"
						 << "Type: BRANCH. \n\t"
						 << "Mapping: " << m2 << endl;
#endif
					}
				}
			}

			if (this->results.empty()
					|| step_num() >= query->max_branch_number)
			{
				vote_to_halt();
			}

		}

		void enumerate_old(MessageContainer & messages)
		{
			// return the number of mappings
			SIQuery* query = (SIQuery*)getQuery();

			hash_map<Mapping, mResult> join_results;
			hash_map<Mapping, mResult>::iterator join_it;
			Mapping prefix, tail;
			vector<Mapping> temp_results;
			int num, curr_u, anc_u;
			SIKey to_send;

			// join operations for supersteps [2, max_branch_number + 1]
			if (step_num() > 1 && step_num() < query->max_branch_number + 2)
			{
				// sort out all the messages, classify them according to prefix
				// and curr_u, store in join_results
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					curr_u = msg.value;
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
			if (step_num() < query->max_branch_number + 1)
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
							send_message(to_send,
								SIMessage(BRANCH_RESULT, it->second[i], curr_u));
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Message sent from " << id.vID << " to "
							 << to_send.vID << ".\n\t"
							 << "Type: BRANCH RESULT. \n\t"
							 << "curr_u: " << curr_u << endl;
#endif
						}
						this->results[curr_u].clear();
					}
				}
			}
			else
				vote_to_halt();

		}
};

//=============================================================================

// typedef hash_map<pair<int, int>, int> AggMap;

class SIAgg : public Aggregator<SIVertex, AggMap, AggMap>
{
	// uniform aggregator for candidates and mappings
	// agg_map[u1, u1] = candidate(u1);
	// agg_map[u1, u2] = sum_i(|C'_{u1, vi}(u2)|), u1 < u2
	// agg_map[0, 0] = # mappings
public:
	AggMap agg_map;

    virtual void init()
    {
    }
    virtual void stepPartial(SIVertex* v, int type)
    {
    	if (type == FILTER)
    	{
        	int u1, u2;
    		for (auto it = v->candidates.begin(); it != v->candidates.end();
    				it++)
    		{
    			u1 = it->first;
    			agg_map[make_pair(u1, u1)] += 1;
    			for (auto jt = it->second.begin(); jt != it->second.end();
    					jt++)
    			{
    				u2 = jt->first;
    				if (u1 < u2)
    					agg_map[make_pair(u1, u2)] += jt->second.size();
    			}
    		}
    	}
    	else if (type == ENUMERATE)
    	{
    		for (auto it = v->results.begin(); it != v->results.end(); it++)
    			agg_map[make_pair(0, 0)] += it->second.size();
    	}
    }
    virtual void stepFinal(AggMap* part)
    {
    	for (auto it = part->begin(); it != part->end(); it++)
    		agg_map[it->first] += it->second;
    }
    virtual AggMap* finishPartial()
    {
    	return &agg_map;
    }
    virtual AggMap* finishFinal()
    {
    	return &agg_map;
    }
};

//=============================================================================

class SIWorker:public Worker<SIVertex, SIQuery, SIAgg>
{
	char buf[100];

	public:
		// C version
		// input line format:
		// vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
		virtual SIVertex* toVertex(char* line, bool default_format)
		{
			char * pch;
			SIVertex* v = new SIVertex;

			if (default_format)
			{
				pch = strtok(line, " ");
				int id = atoi(pch);
				v->id = SIKey(id, id % _num_workers);

				pch = strtok(NULL, " ");
				v->value().label = atoi(pch);

				pch = strtok(NULL, " ");
				int num = atoi(pch);
				SIKey neighbor;
				for (int k = 0; k < num; k++)
				{
					pch = strtok(NULL, " ");
					id = atoi(pch);
					neighbor = SIKey(id, id % _num_workers);
					v->value().neighbors[neighbor] = 0;
				}
			}
			else
			{
				pch = strtok(line, " \t");
				int id = atoi(pch);
				v->id = SIKey(id, id % _num_workers);

				pch = strtok(NULL, " \t");
				v->value().label = (int) *pch;

				SIKey key;
				while((pch = strtok(NULL, " ")) != NULL)
				{
					id = atoi(pch);
					key = SIKey(id, id % _num_workers);
					pch = strtok(NULL, " ");
					v->value().neighbors[key] = (int) *pch;
				}

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
					cout << "[DEBUG] Worker ID: " << get_worker_id() << endl;
					cout << "[DEBUG] Vertex ID: " << v->id.vID << endl;
					cout << "[DEBUG] Result: " << mapping << endl;
#endif
					for (size_t j = 0; j < mapping.size(); j++)
					{
						sprintf(buf, "%d %d\n", query->dfs_order[j],
								mapping[j].vID);
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

	init_timers();
	start_timer(TOTAL_TIMER);

	SIQuery query;
	worker.setQuery(&query);

	SIAgg agg;
	worker.setAggregator(&agg);

	double time, load_time = 0.0, compute_time = 0.0, dump_time = 0.0,
			offline_time = 0.0, online_time = 0.0;

	time = worker.load_data(params);
	load_time += time;

	if (params.input)
	{
		time = worker.run_type(PREPROCESS, params);
		compute_time += time;
	}

	stop_timer(TOTAL_TIMER);
	offline_time = get_timer(TOTAL_TIMER);
	reset_timer(TOTAL_TIMER);

	time = worker.load_query(params.query_path);
	load_time += time;

	start_timer(COMPUTE_TIMER);
	if (params.filter)
	{
		wakeAll();
		time = worker.run_type(FILTER, params);
	}

	time = worker.build_query_tree(params.order);

	wakeAll();
	time = worker.run_type(MATCH, params);

	wakeAll();
	time = worker.run_type(ENUMERATE, params);
	stop_timer(COMPUTE_TIMER);
	compute_time = get_timer(COMPUTE_TIMER);

	time = worker.dump_graph(params.output_path, params.force_write);
	dump_time += time;

	stop_timer(TOTAL_TIMER);
	online_time = get_timer(TOTAL_TIMER);
	reset_timer(TOTAL_TIMER);

	if (_my_rank == MASTER_RANK)
	{
		cout << "================ Final Report ===============" << endl;
		cout << "Mapping count: " <<
				(*((AggMap*)global_agg))[make_pair(0, 0)] << endl;
		cout << "Load time: " << load_time << " s." << endl;
		cout << "Dump time: " << dump_time << " s." << endl;
		cout << "Compute time: " << compute_time << " s." << endl;
		cout << "Offline time: " << offline_time << " s." << endl;
		cout << "Online time: " << online_time << " s." << endl;
	}
}
