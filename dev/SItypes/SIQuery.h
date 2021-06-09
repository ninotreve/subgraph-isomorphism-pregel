#ifndef SIQUERY_H
#define SIQUERY_H

//// Definitions ////

// buckets
// e.g. level 2: u_1(A), u_3(B), u_5(B), u_7(C)
// bucketsize(A) = {u_1}, bucketsize(B) = {u_3, u_5}, bucketsize(C) = {u_7}
// bucketnumber(u_1) = 0, bucketnumber(u_3) = 0, 
// bucketnumber(u_5) = 1, bucketnumber(u_7) = 0.

// index chain
// a vector of length > 1. 
// the last index = index in the mapping; other index = index in the children

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
	bool is_branch = false; // decide whether to build dummy
	bool is_pseudo = false;
	// branch number and index chain are called BNIC
	int branch_number;
	vector<int> index_chain;
	int dfs_number; // 0-based
	int parent; // -1 if none
	int level; // root: level 0. Used as an index in mapping.
	vector<int> children;
	// pseudo children: do not send messages
	vector<int> ps_children;
	// backward neighbors and their positions (excluding its parent!)
	vector<int> b_nbs, b_nbs_pos;
	// backward vertices with same label and their positions
	vector<int> b_same_lab, b_same_lab_pos;
	// previous mapping from root to self. COMPRESSED
	// dummy denoted as negative number, length = ncol
	vector<int> previous_mapping;
	// position of dummy in previous_mapping, -1 if none.
	int dummy_pos = -1;
	// child types, 0 = ordinary, 1 = psd_chd, 
	// +count/-count = multi_psd_chd (no conflict),
	// among which only one is positive (where the neighbors are stored)
	// In the same order as children + ps_children
	vector<int> chd_types;
	// ONLY AVAILABLE FOR BRANCH VERTEX: 
	// constraint INDICEs given to children, (indices of previous_mapping)
	// as well as whether include itself or not. 
	// In the same order as children.
	vector<vector<int>> chd_constraint;
	vector<bool> chd_constraint_self;
	// ONLY AVAILABLE FOR BRANCH VERTEX: branch_senders send branch result to it.
	// In the same order as children.
	vector<int> branch_senders;
	// for Conflict
	bool has_conflict = false;
	vector<int> conflict_index_key;
	vector<int> conflict_index_value;
	// ONLY AVAILABLE FOR BRANCH OR LEAF VERTEX (blu):
	// rci, related conflict indices
	vector<int> rci;
	int caoc_value = 0; // reflect how many conflicts it can solve

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

ostream & operator << (ostream & os, const vector<bool> & v)
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
	os << "[Label: " << (char) node.label 
	   << " Neighbors: " << node.nbs
	   << " Level: " << node.level
	   << " Branch number: " << node.branch_number
	   << " Children: " << node.children
	   << " Backward neighbors: " << node.b_nbs
	   << " Backward neighbors positions: " << node.b_nbs_pos
	   << " Backward same label positions: " << node.b_same_lab_pos
	   << " previous_mapping: " << node.previous_mapping
	   << " chd_types: " << node.chd_types
	   << " chd_constraint_self: " << node.chd_constraint_self
	   << " index_chain: " << node.index_chain 
	   << " branch_senders: " << node.branch_senders << "]";
	return os;
}

//===================================================================

bool sortByVal(const pair<int, int> &a, const pair<int, int> &b)
{
	return (a.second > b.second);
}

struct Conflict
{
	// the order is fixed, which means u1 is always being mapped before u2.
	// u1 = mapped_u; u2 = curr_u (v2 marked)
	// u1_state = the state that checks if conflict vertex exists
	int u1;
	int u2;
	int u1_state;
	int common_ancestor; //caoc
	vector<int> index_chain_1;
	vector<int> index_chain_2;

	Conflict(int u1, int u2, int u1_state, int ca, 
		vector<int> ic_1, vector<int> ic_2, int starting_point)
	{ // Before calling this function, the order is ensured. 
		this->u1 = u1;
		this->u2 = u2;
		this->u1_state = u1_state;
		this->common_ancestor = ca;
		for (int i = starting_point; i < ic_1.size(); i++)
			this->index_chain_1.push_back(ic_1[i]);
		for (int i = starting_point; i < ic_2.size(); i++)
			this->index_chain_2.push_back(ic_2[i]);
	}

	void print()
	{
		cout << "u1: " << u1 << " u2: " << u2 
			 << " u1_state: " << u1_state << " ca: " << common_ancestor << endl;
		cout << "index_chain_1: " << index_chain_1 << endl;
		cout << "index_chain_2: " << index_chain_2 << endl;
	}
};

class SIQuery:public Query<SINode>
{
public:
	hash_map<int, int> id_ind; // id to index mapping
	// note that capitalized ID all means "index"
	vector<SINode> nodes;

	int num;
	int edgenum;
	int root;
	int max_branch_number = 0;
	int max_level = 0;
	vector<int> dfs_order;
	// nearest branch ancestor or -1 if it doesn't have one
	vector<int> nbancestors;
	vector<Conflict> conflicts;

	// bucket stat
	vector<vector<int>> bucket_size_key;
	vector<vector<vector<int>>> bucket_size_value;
	vector<int> bucket_number;

	void init(const string &order, bool pseudo)
	{   
		// call after the query is sent to each worker
		// order = random/degree/ri
		this->num = this->nodes.size();
		this->edgenum = 0;
		for (int i = 0; i < this->num; i++)
			this->edgenum += this->nodes[i].nbs.size();
		this->edgenum /= 2;
		this->nbancestors.resize(this->num);

		if (! this->nodes.empty())
		{
			if (order == "random")
			{
				this->root = 0;
			}
			else if (order == "anti-degree")
			{
				int value, min_value;
				for (int i = 0; i < this->nodes.size(); ++i)
				{
					value = this->nodes[i].nbs.size();
					if (i == 0 || value < min_value)
					{
						min_value = value;
						this->root = i;
					}
				}				
			}
			else
			{
				int value, max_value;
				for (int i = 0; i < this->nodes.size(); ++i)
				{
					value = this->nodes[i].nbs.size();
					if (i == 0 || value > max_value)
					{
						max_value = value;
						this->root = i;
					}
				}
			}

			vector<int> sequence;
			this->dfs(this->root, -1, true, order, sequence, pseudo);
			this->addBNIC(this->root, 0, -1, vector<int>(), 0);
			this->initBuckets();
			sequence.clear();
			this->addPrevMapping(this->root, sequence, -1);
			this->addConflicts();
		}
	}

	virtual void addNode(char* line)
	{
		char * pch;
		pch = strtok(line, " \t");
		int id = atoi(pch);

		pch = strtok(NULL, " \t");
		int label = (int) *pch;

		int i1 = this->nodes.size();
		this->nodes.push_back(SINode(id, label));
		this->id_ind[id] = i1;

		while ((pch = strtok(NULL, " ")) != NULL)
		{
			int neighbor = atoi(pch);
			pch = strtok(NULL, " ");
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
		cout << "------- Print Query Graph ---------" << endl;
		cout << "# Query Vertices: " << this->nodes.size() << endl;
		cout << "# Conflicts: " << this->conflicts.size() << endl;
		cout << "Max Branch Number: " << this->max_branch_number << endl;
		for (size_t i = 0; i < this->dfs_order.size(); i++)
		{
			SINode* curr = &this->nodes[this->dfs_order[i]];
			cout << "Node " << this->dfs_order[i] << ": "
				 << *curr << ". with " << curr->children.size() 
				 <<	" children and " << curr->ps_children.size()
				 << " pseudo children." << endl;
		}
		cout << "------- Print Buckets ---------" << endl;
		for (int i = 0; i < this->max_level+1; ++i)
		{
			cout << "level " << i << endl;
			for (int j = 0; j < this->bucket_size_value[i].size(); j++)
				cout << "[key] " << (char) this->bucket_size_key[i][j]
				     << " [value] " << this->bucket_size_value[i][j] << endl;
		}
		cout << this->bucket_number << endl;
		cout << "------- Print Conflicts ---------" << endl;
		for (int confi = 0; confi < this->conflicts.size(); confi++)
		{
			cout << "Conflict No. " << confi << endl;
			this->conflicts[confi].print();
		}
	}

	void dfs(int currID, int parentID, bool isRoot, const string &order,
		vector<int> &sequence, bool pseudo)
	{
		// recursive function to implement depth-first search.
		// only called when current node is not visited.
		SINode* curr = &this->nodes[currID];
		curr->visited = true;
		curr->parent = parentID;
		curr->dfs_number = this->dfs_order.size();
		this->dfs_order.push_back(currID);

		// update level
		if (isRoot)
			curr->level = 0;
		else
		{
			curr->level = this->getLevel(parentID) + 1;
			if (curr->level > this->max_level)
				this->max_level = curr->level;
		}

		// adding in nodes with same label in the sequence 
		for (int id : sequence)
		{
			SINode* anc = &this->nodes[id];
			if (anc->label == curr->label && id != parentID)
				curr->b_same_lab.push_back(id);
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
					curr->b_nbs.push_back(nextID);
			}
			else
			{
				if (order == "random")
					value = 0;
				else if (order == "degree")
					value = next->nbs.size();
				else if (order == "anti-degree")
					value = -next->nbs.size();
				else // |nbs ^ phi|
				{
					value = 0;
					for (int nbr = 0; nbr < next->nbs.size(); nbr++)
						if (this->nodes[nbr].visited)
							value ++;
				}
				unv_nbs_value.push_back(make_pair(nextID, value));
			}
		}

		sort(unv_nbs_value.begin(), unv_nbs_value.end(), sortByVal);

		for (auto it = unv_nbs_value.begin(); it != unv_nbs_value.end(); it++)
		{
			if (! this->nodes[it->first].visited)
			{
				this->dfs(it->first, currID, false, order, sequence, pseudo);

				// when return from dfs,
				// determine what kind of children are ps_children
				int childID = sequence.back();
				SINode *child = &this->nodes[childID];
				if (pseudo &&
					child->children.empty() && child->b_nbs.empty() &&
					child->ps_children.empty())
				{ // it is pseudo child of its parent
					child->is_pseudo = true;
					curr->ps_children.push_back(childID);
				}
				else
					curr->children.push_back(childID);
				
				sequence.pop_back();
			}
		}
	}

	void addBNIC(int currID, int num, int ancID, vector<int> index_chain, 
		int mapping_index)
	{
		// recursive function to add branch number and index chain.
		// pseudo children also considered as they will use their index chain.
		SINode* curr = &this->nodes[currID];
		this->nbancestors[currID] = ancID;
		if (curr->children.size() == 0 && ancID >= 0)
			this->nodes[ancID].branch_senders.push_back(currID);

		int children_size = curr->children.size() + curr->ps_children.size();
		// A vertex with only pseudo children is not branching vertex, 
		// which means there's no need to build dummy vertex, 
		// simply put pseudo children into SIBranch.
		// But root vertex is an exception since there's no mapping. 
		// We treat it as branch && leaf.
		if (children_size > 1 && (curr->children.size() >= 1 || currID == this->root))
		{
			curr->is_branch = true;
			num ++;
			if (ancID >= 0)
				this->nodes[ancID].branch_senders.push_back(currID);
			ancID = currID;
		}

		curr->branch_number = num;

		index_chain.push_back(mapping_index);
		curr->index_chain = index_chain;
		index_chain.pop_back();

		if (num > this->max_branch_number)
			this->max_branch_number = num;

		if (children_size == 1 & curr->children.size() == 1)
			this->addBNIC(curr->children[0], num, ancID, index_chain, 
				mapping_index+1);
		else
		{
			int chd_sz = curr->children.size();
			for (int i = 0; i < chd_sz; i++)
			{
				index_chain.push_back(i);
				this->addBNIC(curr->children[i], num, ancID,
					index_chain, 0);
				index_chain.pop_back();
			}
			for (int i = 0; i < curr->ps_children.size(); i++)
			{
				index_chain.push_back(chd_sz+i);
				this->addBNIC(curr->ps_children[i], num, -1,
					index_chain, -1);
				// ancID = -1: no need to add to branch_senders
				// mapping_index = -1: pseudo
				index_chain.pop_back();
			}
		} 
	}


	bool hasForwardConnection(int ancestorID, int currID, bool exists_gap)
	{
		// recursive helper function to addPrevMapping
		// checks the connection between ancestorID and the subtree of currID
		// exists_gap: ancestorID is not the direct parent of currID
		if (exists_gap && (this->getLabel(ancestorID) == this->getLabel(currID) ||
			 this->hasEdge(ancestorID, currID)))
			return true;
		
		SINode* curr = &this->nodes[currID];
		for (int i = 0; i < curr->children.size(); i++)
			if (this->hasForwardConnection(ancestorID, curr->children[i], true)) 
				return true;
		
		for (int i = 0; i < curr->ps_children.size(); i++)
			if (this->hasForwardConnection(ancestorID, curr->ps_children[i], true)) 
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
			for (pos = 0; pos < previous_mapping.size() && 
				previous_mapping[pos] != b_nb; pos++);
			if (pos == previous_mapping.size())
			{
				cout << "[BUG]" << endl;
				cout << "Previous_mapping buggy" << endl;
				pos = 0;
			}
			curr->b_nbs_pos.push_back(pos);
		}
		for (int b_sl: curr->b_same_lab)
		{
			for (pos = 0; pos < previous_mapping.size() && 
				previous_mapping[pos] != b_sl; pos++);
			if (pos == previous_mapping.size())
			{
				cout << "[BUG]" << endl;
				cout << "Previous_mapping buggy" << endl;
				pos = 0;
			}
			curr->b_same_lab_pos.push_back(pos);
		}

		int sz = curr->children.size();
		int psz = curr->ps_children.size();
		if (curr->is_branch)
		{   // set up chd_constraint for branch vertex
			curr->chd_constraint.resize(sz+psz);
			curr->chd_constraint_self.resize(sz);
			for (int i = 0; i < sz+psz; i++)
			{
				int chd;
				if (i < sz) chd = curr->children[i];	
				else chd = curr->ps_children[i-sz];
				for (int j = 0; j < previous_mapping.size(); j++)
				{
					int ancestor = previous_mapping[j];
					if (ancestor < 0) continue; //dummy					
					if (this->hasForwardConnection(ancestor, chd, true))
						curr->chd_constraint[i].push_back(j); //push back the index
				}
				if (i < sz)
					if (curr->level == 0)
						curr->chd_constraint_self[i] = false; // dummyID = vID
					else
						curr->chd_constraint_self[i] = 
							this->hasForwardConnection(currID, chd, false);
			}

			for (int i = 0; i < sz+psz; i++)
			{
				vector<int> sequence;
				for (int j = 0; j < curr->chd_constraint[i].size(); j++)
					sequence.push_back(previous_mapping[curr->chd_constraint[i][j]]);
				if (i < sz)
				{
					if (curr->chd_constraint_self[i])
						sequence.push_back(currID);
					// dummy vID
					if (curr->level == 0)
						sequence.push_back(currID);
					else
						sequence.push_back(-currID-1);
					// dummy wID
					sequence.push_back(-currID-1);
					this->addPrevMapping(curr->children[i], sequence, 
						curr->chd_constraint[i].size() + curr->chd_constraint_self[i]);
				}
				else
					this->addPrevMapping(curr->ps_children[i-sz], previous_mapping, -1);
			}
		}
		else // not branch vertex
		{
			previous_mapping.push_back(currID);
			for (int i = 0; i < sz; i++)
				this->addPrevMapping(curr->children[i], previous_mapping, dummy_pos);
			for (int i = 0; i < psz; i++)
				this->addPrevMapping(curr->ps_children[i], previous_mapping, dummy_pos);
			previous_mapping.pop_back();
		}
	}

	void addCIRCI(int u1, int u2, int u1_state, int caoc, int index)
	{
		// add conflict indices (ci) and related conflict indices (rci) 
		this->nodes[u1].has_conflict = true;
		this->nodes[u2].has_conflict = true;

		this->nodes[u2].conflict_index_key.push_back(u1);
		this->nodes[u2].conflict_index_value.push_back(index);

		this->nodes[caoc].rci.push_back(index);
		this->nodes[caoc].caoc_value += 1 << index;
		this->nodes[u1_state].rci.push_back(index);
	}

	int moveToBOL(int u)
	{
		// a helper function to move u to the next branching or leaf
		while (getChildren(u).size() == 1 && getPseudoChildren(u).size() == 0) 
			u = getChildren(u)[0];
		return u;
	}

	void addConflicts()
	{
		// this function generates a sequence of conflicted vertices
		// i.e. with the same label.
		// also makes an inverse index for each curr_u(u2).
		//DEBUG: print out the conflicts

		// doubly nested loop to extract query vertex pairs.
		for (int v1 = 0; v1 < this->nodes.size()-1; v1++)
		{
			for (int v2 = v1+1; v2 < this->nodes.size(); v2++)
			{
				int u1, u2;
				if (getLabel(v1) != getLabel(v2)) continue;
				if (isAncestor(v1, v2) || isAncestor(v2, v1)) continue;

				// v1 is always mapped before v2
				// (Pseudo request messages are dealt before ordinary messages)
				if ((getLevel(v1) > getLevel(v2)) ||
				    (getLevel(v1) == getLevel(v2) && (isPseudo(v2) ||
					getBucketNumber(v1) > getBucketNumber(v2) )
					)
				   )
				{ // swap v1 & v2
					u2 = v1;
					u1 = v2;
				}
				else
				{
					u1 = v1;
					u2 = v2;
				}

				// 2 <= u1->index_chain.size() <= u2->index_chain.size()
				// if i == u1->index_chain.size() - 2 
				// then index_chain[i] must be different
				int ca = this->root;
				int i = 0;
				vector<int> index_chain_1 = getIndexChain(u1);
				vector<int> index_chain_2 = getIndexChain(u2);
				while (true)
				{
					ca = moveToBOL(ca);
					if (index_chain_1[i] != index_chain_2[i])
						break;
					ca = getChildren(ca)[index_chain_1[i]];
					i++;
				}

				// the case of pseudo children
				if (isPseudo(u2))
				{
					int parent = getParent(u2);
					SINode* p = &this->nodes[parent];
					if (isPseudo(u1) && parent == ca)
						continue; // it's not a conflict :)
					else
					{// change parent's chd_type
						int sz = p->children.size();
						for (int k = 0; k < p->ps_children.size(); k++)
							if (p->ps_children[k] == u2)
								p->chd_types[sz+k] = 1;
					}
				}

				int u1_state = moveToBOL(u1);				
				Conflict c = Conflict(u1, u2, u1_state, ca, index_chain_1, index_chain_2, i);
				this->addCIRCI(u1, u2, u1_state, ca, this->conflicts.size());
				this->conflicts.push_back(c); 

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
	// the following id are all index in this->nodes
	int getLevel(int id) { return this->nodes[id].level; }
	int getBranchNumber(int id) { return this->nodes[id].branch_number;	}
	int getDFSNumber(int id) { return this->nodes[id].dfs_number; }
	int getParent(int id) { return this->nodes[id].parent; }
	vector<int> &getChildren(int id)
	{ return this->nodes[id].children; }
	vector<int> &getPseudoChildren(int id)
	{ return this->nodes[id].ps_children; }	
	vector<int> &getChdTypes(int id)
	{ return this->nodes[id].chd_types; }
	vector<int> &getBNeighborsPos(int id)
	{ return this->nodes[id].b_nbs_pos; }
	vector<int> &getBSameLabPos(int id)
	{ return this->nodes[id].b_same_lab_pos; }
	vector<int> &getPrevMapping(int id)
	{ return this->nodes[id].previous_mapping; }
	vector<int> &getBranchSenders(int id)
	{ return this->nodes[id].branch_senders; }
	int getBranchSender(int id, int i)
	{return this->nodes[id].branch_senders[i]; }
	vector<int> &getChdConstraint(int id, int i)
	{ return this->nodes[id].chd_constraint[i]; }
	bool getIncludeSelf(int id, int i)
	{ return this->nodes[id].chd_constraint_self[i]; }
	int getNCOL(int id)
	{ return this->nodes[id].previous_mapping.size(); }
	int getDummyPos(int id)
	{ return this->nodes[id].dummy_pos; }
	int getNearestBranchingAncestor(int id)
	{ return this->nbancestors[id]; }
	vector<Conflict> getConflicts()
	{ return this->conflicts; }
	Conflict getConflict(int ci)
	{ return this->conflicts[ci]; }
	int getConflictNumber(int id, int mapped_u)
	{
		int i = 0;
		for (; i < this->nodes[id].conflict_index_key.size()
		&& this->nodes[id].conflict_index_key[i] != mapped_u; i++);
		if (i < this->nodes[id].conflict_index_value.size())
			return (1 << this->nodes[id].conflict_index_value[i]);
		else // not matched
			return 0;
	}
	int getCAOCValue(int id)
	{ return this->nodes[id].caoc_value; }
	vector<int> getIndexChain(int id)
	{ return this->nodes[id].index_chain; }
	vector<int> getRelatedConflictIndices(int id) // only for blu
	{ return this->nodes[id].rci; }
	bool isCAOC(int id)
	{
		for (int i = 0; i < this->conflicts.size(); i++)
			if (this->conflicts[i].common_ancestor == id)
				return true;
		return false;
	}
	bool isLeaf(int id)
	{ return this->nodes[id].children.empty(); }
	bool isBranch(int id)
	{ return this->nodes[id].is_branch; }
	bool isPseudo(int id)
	{ return this->nodes[id].is_pseudo; }
	bool isAncestor(int u, int v)
	{ // return if u is an ancestor of v
		while (this->getParent(v) != -1)
		{
			if (v == u) return true;
			v = this->getParent(v);
		}
		return (v == u);
	}
	bool hasConflict(int id)
	{ return this->nodes[id].has_conflict; }

	void initBuckets()
	{
		// init chd_types as well
		this->bucket_size_key.resize(this->max_level+1);
		this->bucket_size_value.resize(this->max_level+1);
		this->bucket_number.resize(this->nodes.size());

		SINode *curr;
		for (int i = 0; i < this->nodes.size(); ++i)
		{
			bool flag = true;
			curr = &this->nodes[i];
			vector<int> &keys = this->bucket_size_key[curr->level];
			vector<vector<int>> &vals = this->bucket_size_value[curr->level];
			for (int j = 0; j < keys.size(); ++j)
			{
				if (keys[j] == curr->label)
				{
					if (curr->is_pseudo)
					{ // insert at the front
						for (int val : vals[j])
							this->bucket_number[val] ++;
						vals[j].insert(vals[j].begin(), i);
						this->bucket_number[i] = 0;
					}
					else
					{ // push at the back
						this->bucket_number[i] = vals[j].size();
						vals[j].push_back(i);
					}
					flag = false;
					break;
				}
			}
			if (flag)
			{
				keys.push_back(curr->label);
				vector<int> v_i = {i};
				vals.push_back(v_i);
				this->bucket_number[i] = 0;
			}
			// init chd_types
			int chd_sz = curr->children.size();
			for (int j = 0; j < chd_sz; j++)
				curr->chd_types.push_back(0);
			
			vector<int> labels;
			for (int j = 0; j < curr->ps_children.size(); j++)
			{
				int l, label = this->getLabel(curr->ps_children[j]);
				for (l = 0; l < labels.size() && labels[l] != label; l++);
				if (l == labels.size()) // not found
				{
					curr->chd_types.push_back(1);
					labels.push_back(label);
				}
				else // found
				{
					bool first = true;
					int count = 1;
					for (int k = 0; k < j; k++)
					{
						if (this->getLabel(curr->ps_children[k]) == label)
						{
							if (first) 
								{curr->chd_types[chd_sz+k] ++; first = false;}
							else
								curr->chd_types[chd_sz+k] --;
							count ++;
						}
					}
					curr->chd_types.push_back(-count);
				}
			}
		}
	}

	// get functions regarding buckets &&&
	vector<int> getBucket(int level, int label)
	{
		if (level >= this->bucket_size_key.size())
			return vector<int>();
			
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
