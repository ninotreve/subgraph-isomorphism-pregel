#ifndef SIQUERY_H
#define SIQUERY_H

// buckets
// e.g. level 2: u_1(A), u_3(B), u_5(B), u_7(C)
// bucketsize(A) = {u_1}, bucketsize(B) = {u_3, u_5}, bucketsize(C) = {u_7}
// bucketnumber(u_1) = 0, bucketnumber(u_3) = 0, 
// bucketnumber(u_5) = 1, bucketnumber(u_7) = 0.

typedef vector<vector<double>> AggMat;
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
	bool is_branch = false;
	int branch_number;
	int dfs_number; // 0-based
	int parent;
	int level; // root: level 0. Used as an index in mapping.
	vector<int> children;
	// pseudo children: do not send messages
	vector<int> ps_children_labels, ps_children_labels_count;
	// backward neighbors and their positions (excluding its parent!)
	vector<int> b_nbs, b_nbs_pos;
	// backward vertices with same label and their positions
	vector<int> b_same_lab, b_same_lab_pos;
	// previous mapping from root to self. COMPRESSED
	// dummy denoted as negative number, length = ncol
	vector<int> previous_mapping;
	int dummy_pos = -1; // position of dummy in previous_mapping, -1 if none.
	// ONLY AVAILABLE FOR BRANCH VERTEX: constraints given to children, 
	// as well as whether include itself or not. In the same order as children.
	vector<vector<int>> chd_constraint;
	vector<bool> chd_constraint_self;

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
// Overloading << operator of vector<int>
// print like Python, for debug purpose

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
	os << "[Label: " << node.label 
	   << " Neighbors: " << node.nbs
	   << " Level: " << node.level
	   << " Backward neighbors: " << node.b_nbs
	   << " Backward neighbors positions: " << node.b_nbs_pos
	   << " previous_mapping: " << node.previous_mapping << "]";
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
	// note that capitalized ID all means "index"
	vector<SINode> nodes;

	size_t num;
	int root;
	int max_branch_number = 0;
	int max_level = 0;
	vector<int> dfs_order;
	// nearest branch ancestor or root if it doesn't have one
	vector<int> nbancestors;

	// bucket stat
	vector<vector<int>> bucket_size_key;
	vector<vector<vector<int>>> bucket_size_value;
	vector<int> bucket_number;

	virtual void init(const string &order)
	{ // call after the query is sent to each worker
		this->num = this->nodes.size();
		this->nbancestors.resize(this->num);

		// order = "degree", value = degree
		// order = "candidate", value = candidate size
		// order = "random"
		if (! this->nodes.empty())
		{
			if (order == "random")
			{
				this->root = 0;
			}
			else
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
			}

			vector<int> sequence;
			this->dfs(this->root, 0, true, order, sequence);
			this->addBranchNumber(this->root, 0, this->root);
			this->initBuckets();
			sequence.clear();
			this->addPrevMapping(this->root, sequence, -1);
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
		cout << "# Query Vertices: " << this->nodes.size() << endl;
		for (size_t i = 0; i < this->dfs_order.size(); i++)
		{
			SINode* curr = &this->nodes[this->dfs_order[i]];
			cout << "Node " << this->dfs_order[i] << ": "
				 << *curr << ". with " << curr->children.size() 
				 <<	" children and " << curr->ps_children_labels.size()
				 << " pseudo children." << endl;
			if (!curr->ps_children_labels.empty())
			{
				cout << "{";
				for (size_t j = 0; j < curr->ps_children_labels.size(); j++)
					cout << curr->ps_children_labels[j] << "*" 
						 << curr->ps_children_labels_count[j] << " ";
				cout << "}" << endl;
			}
		}
	}

	void dfs(int currID, int parentID, bool isRoot, const string &order,
		vector<int> &sequence)
	{
		// recursive function to implement depth-first search.
		// only called when current node is not visited.
		SINode* curr = &this->nodes[currID];
		curr->visited = true;
		curr->dfs_number = this->dfs_order.size();
		this->dfs_order.push_back(currID);

		// update level
		if (isRoot)
			curr->level = 0;
		else
		{
			curr->parent = parentID;
			curr->level = this->getLevel(parentID) + 1;
			if (curr->level > this->max_level)
				this->max_level = curr->level;
		}

		// adding in nodes with same label in the sequence 
		for (int id : sequence)
		{
			SINode* anc = &this->nodes[id];
			if (anc->label == curr->label && anc->id != curr->id)
				curr->b_same_lab.push_back(this->id_ind[anc->id]);
		}
		sequence.push_back(currID);

		// we must have two loops to avoid descendants being visited
		// by other descendants.
		// the first loop also stores unvisited neighbor's value
		// (degree or candidate size).
		int value;
		vector<pair<int, int> > unv_nbs_value;

		for (int nextID : curr->nbs)
		{
			SINode* next = &this->nodes[nextID];
			if (next->visited)
			{
				if (next->level != curr->level - 1)
					curr->b_nbs.push_back(this->id_ind[next->id]);
			}
			else
			{
				if (order == "random")
					value = 0;
				else if (order == "degree")
					value = - next->nbs.size(); // default asc
				else if (order == "candidate")
				{
					if (currID > nextID)
						value = (*((AggMat*)global_agg))[currID][nextID];
					else
					    value = (*((AggMat*)global_agg))[nextID][currID];
				}
				unv_nbs_value.push_back(make_pair(nextID, value));
			}
		}

		sort(unv_nbs_value.begin(), unv_nbs_value.end(), sortByVal);

		for (auto it = unv_nbs_value.begin(); it != unv_nbs_value.end(); it++)
		{
			if (! this->nodes[it->first].visited)
			{
				this->dfs(it->first, currID, false, order, sequence);
				int childID = sequence.back();
				SINode *child = &this->nodes[childID];
				if (child->children.empty() && child->b_nbs.empty() &&
					child->ps_children_labels.empty())
				{ // pseudo children of its parent
					int index, childLab = this->getLabel(childID);
					for (index = 0; index < curr->ps_children_labels.size(); index++)
					{
						if (curr->ps_children_labels[index] == childLab)
						{
							curr->ps_children_labels_count[index]++;
							break;
						}
					}
					if (index == curr->ps_children_labels.size())
					{
						curr->ps_children_labels.push_back(childLab);
						curr->ps_children_labels_count.push_back(1);
					}
				}		
				else
					curr->children.push_back(childID);
				
				sequence.pop_back();
			}
		}
	}

	void addBranchNumber(int currID, int num, int ancID)
	{
		// recursive function to add branch number.
		this->nbancestors[currID] = ancID;

		SINode* curr = &this->nodes[currID];
		int children_size = curr->children.size();
		for (int index = 0; index < curr->ps_children_labels.size(); index++)
			children_size += curr->ps_children_labels_count[index];
		if (curr->children.size() >= 1 && children_size > 1)
		{
			curr->is_branch = true;
			num ++;
			ancID = currID;
		}

		curr->branch_number = num;

		if (num > this->max_branch_number)
			this->max_branch_number = num;
		for (size_t i = 0; i < curr->children.size(); i++)
			this->addBranchNumber(curr->children[i], num, ancID);
	}

	bool hasForwardConnection(int ancestorID, int currID, bool exists_gap)
	{
		// recursive helper function to addPrevMapping
		// checks the connection between ancestorID and the subtree of currID
		// exists_gap: ancestorID is not the direct parent of currID
		if (this->getLabel(ancestorID) == this->getLabel(currID) ||
			exists_gap && this->hasEdge(ancestorID, currID))
			return true;
		
		SINode* curr = &this->nodes[currID];
		for (int i = 0; i < curr->children.size(); i++)
			if (this->hasForwardConnection(curr->children[i], currID, true))
				return true;
		
		return false;
	}

	void addPrevMapping(int currID, vector<int> &previous_mapping, int dummy_pos)
	{
		// recursive function to add compressed prefix and update BNP and BSLP
		SINode* curr = &this->nodes[currID];
		for (int vID : previous_mapping)
			curr->previous_mapping.push_back(vID);
		curr->dummy_pos = dummy_pos;

		// generate BNP and BSLP according to previous_mapping
		int pos;
		for (int b_nb: curr->b_nbs)
		{
			for (pos = 0; previous_mapping[pos] != b_nb; pos++);
			curr->b_nbs_pos.push_back(pos);
		}
		for (int b_sl: curr->b_same_lab)
		{
			for (pos = 0; previous_mapping[pos] != b_sl; pos++);
			curr->b_same_lab_pos.push_back(pos);
		}

		int sz = curr->children.size();
		if (curr->is_branch)
		{   // set up chd_constraint for branch vertex
			curr->chd_constraint.resize(sz);
			curr->chd_constraint_self.resize(sz);
			bool add_dummy = false;
			for (int i = 0; i < sz; i++)
			{
				int chd = curr->children[i];	
				for (int ancestor : previous_mapping)
				{
					if (ancestor < 0) continue; //dummy					
					if (this->hasForwardConnection(ancestor, chd, true))
					{
						curr->chd_constraint[i].push_back(ancestor);
						add_dummy = true;
						break;
					}
				}
				curr->chd_constraint_self[i] = 
						this->hasForwardConnection(currID, chd, false);
			}

			if (!add_dummy)
				for (int i = 0; i < sz; i++)
					curr->chd_constraint_self[i] = false;

			for (int i = 0; i < sz; i++)
			{
				vector<int> sequence = curr->chd_constraint[i];
				// add_dummy & include_self: +--
				// add_dummy & !include_self: --
				// !add_dummy: +-
				if (!add_dummy || (add_dummy && curr->chd_constraint_self[i]))
					sequence.push_back(currID);
				if (add_dummy)
					sequence.push_back(-currID-1);
				sequence.push_back(-currID-1);
				this->addPrevMapping(curr->children[i], sequence, 
					curr->chd_constraint[i].size());
			}

		}
		else
		{
			if (sz > 0)
			{
				previous_mapping.push_back(currID);
				this->addPrevMapping(curr->children[0], previous_mapping, dummy_pos);
				previous_mapping.pop_back();
			}
		}
	}

	// Query is read-only.
	// get functions before dfs.
	int getID(int id) { return this->nodes[id].id; }
	int getLabel(int id) { return this->nodes[id].label; }
	vector<int> &getNbs(int id) { return this->nodes[id].nbs; }
	bool hasEdge(int i, int j)
	{
		for (int k : this->getNbs(i))
			if (k == j)
				return true;
		return false;
	}
	int getInverseIndex(int id, int next_u)
	{
		int j;
		for (j = 0; this->nodes[id].nbs[j] != next_u; j++);
		return j;
	}

	// fill cand_map with right vertices
	void LDFFilter(int label, size_t degree, hash_map<int, vector<int> > &cand_map)
	{
		vector<int> nbs;
		int curr_u, lab;
		for (curr_u = 0; curr_u < this->nodes.size(); ++curr_u)
		{
			lab = this->nodes[curr_u].label;
			nbs = this->nodes[curr_u].nbs;
			if (lab == label && nbs.size() <= degree)
			{
				cand_map[curr_u] = nbs;
			}
		}
	}

	// get functions after dfs.
	int getLevel(int id) { return this->nodes[id].level; }
	int getBranchNumber(int id) { return this->nodes[id].branch_number;	}
	int getDFSNumber(int id) { return this->nodes[id].dfs_number; }
	int getParent(int id) { return this->nodes[id].parent; }
	vector<int> &getChildren(int id)
	{ return this->nodes[id].children; }
	vector<int> &getPseudoLabel(int id)
	{ return this->nodes[id].ps_children_labels; }
	vector<int> &getPseudoLabelCount(int id)
	{ return this->nodes[id].ps_children_labels_count; }
	vector<int> &getBNeighborsPos(int id)
	{ return this->nodes[id].b_nbs_pos; }
	vector<int> &getBSameLabPos(int id)
	{ return this->nodes[id].b_same_lab_pos; }
	vector<int> &getPrevMapping(int id)
	{ return this->nodes[id].previous_mapping; }
	bool getIncludeSelf(int id, int i)
	{ return this->nodes[id].chd_constraint_self[i]; }
	int getNCOL(int id)
	{ return this->nodes[id].previous_mapping.size(); }
	int getDummyPos(int id)
	{ return this->nodes[id].dummy_pos; }
	int getNearestBranchingAncestor(int id)
	{ return this->nbancestors[id]; }

	bool isBranch(int id)
	{ return this->nodes[id].is_branch; }

	void initBuckets()
	{
		this->bucket_size_key.resize(this->max_level+1);
		this->bucket_size_value.resize(this->max_level+1);
		this->bucket_number.resize(this->nodes.size());

		SINode *curr;
		bool flag = true;
		for (int i = 0; i < this->nodes.size(); ++i)
		{
			curr = &this->nodes[i];
			vector<int> &k = this->bucket_size_key[curr->level];
			vector<vector<int>> &v = this->bucket_size_value[curr->level];
			for (int j = 0; j < k.size(); ++j)
			{
				if (k[j] == curr->label)
				{
					this->bucket_number[i] = v[j].size();
					v[j].push_back(i);
					flag = false;
					break;
				}
			}
			if (flag)
			{
				k.push_back(curr->label);
				vector<int> v_i = {i};
				v.push_back(v_i);
				this->bucket_number[i] = 0;
			}
			flag = true;
		}
		/*
		for (int i = 0; i < this->max_level+1; ++i)
		{
			cout << "level " << i << endl;			
			cout << "keys: " << this->bucket_size_key[i] << endl;
			cout << "values: " << this->bucket_size_value[i].size() << endl;
			
			for (int j = 0; j < this->bucket_size_value[i].size(); j++)
				cout << this->bucket_size_value[i][j][0] << endl;
		}
		cout << this->bucket_number << endl;
		*/
	}

	// get functions regarding buckets &&&
	vector<int> getBucket(int level, int label)
	{
		vector<int> &k = this->bucket_size_key[level];
		size_t sz = k.size();
		for (size_t j = 0; j < sz; ++j)
		{
			if (k[j] == label) 
				return this->bucket_size_value[level][j];
		}
		return vector<int>();
	}

	int getBucketNumber(int id)
	{ return this->bucket_number[id]; }
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

/*
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
*/
#endif
