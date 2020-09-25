#ifndef SIQUERY_H
#define SIQUERY_H

typedef vector<vector<int>> AggMat;
// define hash of pair

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
	os << "Label: " << node.label << endl;
	os << "Branch number: " << node.branch_number << endl;
	os << "Neighbors: " << node.nbs << endl;
	return os;
}

//===================================================================

bool sortByVal(const pair<int, int> &a, const pair<int, int> &b)
{
	return (a.second < b.second);
}

class SIQuery:public Query<SINode>
{
public:
	hash_map<int, int> id_ind; // id to index mapping
	vector<SINode> nodes;

	size_t num;
	int root;
	int max_branch_number = 0;
	vector<int> dfs_order;
	// nearest branch ancestor or root if it doesn't have one
	vector<int> nbancestors;

	virtual void init(const string &order)
	{ // call after the query is sent to each worker
		this->num = this->nodes.size();
		this->nbancestors.resize(this->num);

		// order = "degree", value = degree
		// order = "candidate", value = candidate size
		if (! this->nodes.empty())
		{
			size_t value, min_value;
			for (size_t i = 0; i < this->nodes.size(); ++i)
			{
				if (order == "degree")
					value = - this->nodes[i].nbs.size(); // default asc
				else
					value = (*((AggMat*)global_agg))[i][i];
				if (i == 0 || value < min_value)
				{
					min_value = value;
					this->root = i;
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

		int i1 = this->nodes.size();
		this->nodes.push_back(SINode(id, label));
		this->id_ind[id] = i1;

		pch = strtok(NULL, " ");
		int num = atoi(pch);
		for (int k = 0; k < num; k++)
		{
			pch=strtok(NULL, " ");
			int neighbor = atoi(pch);
			if (this->id_ind.find(neighbor) != this->id_ind.end())
			{
				int i2 = this->id_ind[neighbor];
				this->nodes[i1].add_edge(i2);
				this->nodes[i2].add_edge(i1);
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
					value = (*((AggMat*)global_agg))[currID][nextID]
					     + (*((AggMat*)global_agg))[nextID][currID];
				unv_nbs_value.push_back(make_pair(nextID, value));
			}
		}

		sort(unv_nbs_value.begin(), unv_nbs_value.end(), sortByVal);

		for (auto it = unv_nbs_value.begin(); it != unv_nbs_value.end(); it++)
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
	int getID(int id) { return this->nodes[id].id; }
	vector<int> getNbs(int id) { return this->nodes[id].nbs; }

	// fill cand_map with right vertices
	void LDFFilter(int label, size_t degree,
			hash_map<int, vector<int> > &cand_map)
	{
		vector<int> nbs;
		int curr_u, lab;
		for (curr_u = 0; curr_u < this->num; ++curr_u)
		{
			lab = this->nodes[curr_u].label;
			nbs = this->nodes[curr_u].nbs;
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
	m << q.nodes;
	return m;
}

obinstream & operator>>(obinstream & m, SIQuery & q){
	m >> q.nodes;
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

#endif
