#ifndef SIBRANCH_H
#define SIBRANCH_H

#include "SItypes/SIQuery.h"
//==========================================================================

struct SIBranch
{
	int *mapping;
	int self;
	int ncol;
	int curr_u;
	int T; // generated in enumerateTree
	int mapping_marker; // not sent

	vector<SIBranch*> chd_pointers;
	// the following three all have length |#children|
	vector<vector<pair<int, int>>> unmarked_branches;
	vector<vector<pair<int, int>>> marked_branches;

	vector<int> tree_indices; // valid tis
	vector<int> tree_markers; // length of T
	vector<int> conflux_values; // length of T

	SIBranch() {};

	SIBranch(int *mapping, int self, int ncol, int curr_u, int mapping_marker)
	{
		SIQuery* query = (SIQuery*)getQuery();
		if (ncol != 0)
		{
			this->mapping = new int[ncol];
			for (int i = 0; i < ncol; i++)
				this->mapping[i] = mapping[i];
		}
		this->self = self;
		this->ncol = ncol;
		this->curr_u = curr_u;
		this->mapping_marker = mapping_marker;
		int s = (query->getChdTypes(curr_u)).size();
		this->unmarked_branches = vector<vector<pair<int, int>>>(s);
		this->marked_branches = vector<vector<pair<int, int>>>(s);
	}

	vector<int> getStateRep(int ti)
	{
		vector<int> sr;
		int T0 = this->T;
		for (int bi = 0; bi < this->marked_branches.size(); bi++)
		{
			T0 /= (marked_branches[bi].size() + 1);
			sr.push_back(ti / T0);
			ti = ti % T0;			
		}
		return sr;
	}

	int getChdMarker(int si, int ci)
	{
		if (ci == 0)
			return 0;
		else
		{
			int pi = this->marked_branches[si][ci-1].first;
			int ti = this->marked_branches[si][ci-1].second;
			return this->chd_pointers[pi]->tree_markers[ti];
		}
	}

	bool enumerateTrees(int cv)
	{
		// this function fills tree indices, tree markers and conflux values
		int s = this->unmarked_branches.size();
		if (s == 0)
		{
			this->tree_indices.push_back(0);
			this->tree_markers.push_back(this->mapping_marker);
			this->conflux_values.push_back(0);
			return true;
		}

		vector<int> v = vector<int>(s);
		vector<int> ceil = vector<int>(s);
		vector<int> blank_states = vector<int>();
		this->T = 1;

		for (int i = 0; i < s; i++)
		{
			int mi = this->marked_branches[i].size();
			if (this->unmarked_branches[i].empty())
				if (mi == 0) return false;
				else blank_states.push_back(i);

			ceil[i] = mi + 1;
			this->T *= (mi + 1);
		}

		this->tree_markers.resize(this->T);
		this->conflux_values.resize(this->T);

		for (int ti = 0; ti < T; ti++, v[s-1]++)
		{
			for (int j = s-1; v[j] == ceil[j]; v[j--] = 0, v[j]++);

			bool flag = false;
			for (int j : blank_states)
			{ if (v[j] == 0) { flag = true; break;} }
			if (flag) continue;

			this->tree_indices.push_back(ti);
			int marker = this->mapping_marker;
			for (int j = 0; j < s; j++)
				marker += this->getChdMarker(j, v[j]);
			
			tree_markers[ti] = marker & (~cv);
			conflux_values[ti] = marker & cv;
		}
		return true;
	}

	int extractMapping(int index)
	{ // since the final vertex is stored in this->self,
	  // this is a helper function to help extract the correct vertex
		if (index < this->ncol)
			return this->mapping[index];
		else
			return this->self;
	}

	int extractConflictVertex(int ti, vector<int> index_chain_2)
	{
		int ind = 0, si, ci, pi;
		SIBranch *chd = this;
		while (ind < index_chain_2.size() - 1)
		{
			si = index_chain_2[ind];
			ci = chd->getStateRep(ti)[si]; // > 1
			pi = chd->marked_branches[si][ci-1].first;
			ti = chd->marked_branches[si][ci-1].second;
			chd = chd->chd_pointers[pi];
			ind ++;
		}
		return chd->extractMapping(index_chain_2[ind]);
	}

	int expand(int ti, vector<int> conflict_vs)
	{
		// expand the ti-th tree in trees.
		// recursively calls its children to expand.
		// conflict_vs: vector of length k
		SIQuery* query = (SIQuery*)getQuery();
		vector<int> choices = this->getStateRep(ti);

		vector<int> cis = query->getRelatedConflictIndices(this->curr_u);
		for (int ci: cis)
		{
			Conflict c = query->getConflict(ci);
			if (this->curr_u == c.common_ancestor)
				if ((this->conflux_values[ti] >> ci) & 1)
					conflict_vs[ci] = this->extractConflictVertex(ti, c.index_chain_2);
			
			if (this->curr_u == c.u1_state)
				if (this->extractMapping(c.index_chain_1[c.index_chain_1.size()-1]) 
					== conflict_vs[ci]) 
					return 0;
		}

		int count = 1;
		for (int ci = 0; ci < choices.size(); ci++)
		{
			int count_ci = 0;
			int choice_i = choices[ci];
			int chd_type = query->getChdTypes(this->curr_u)[ci];
			if (choice_i == 0)
			{ // unmarked branches
				if (chd_type == 0) // ordinary child
				{
					for (pair<int, int> p : this->unmarked_branches[ci])
					{
						count_ci += this->chd_pointers[p.first]->expand(
							p.second, conflict_vs);
					}
				}
				else if (chd_type == 1) // pseudo child
				{
					int chd_sz = query->getChildren(this->curr_u).size();
					int chd_u = query->getPseudoChildren(this->curr_u)[ci-chd_sz];
					vector<int> psd_conflict_vs;
					for (int chd_ci : query->getRelatedConflictIndices(chd_u))
						psd_conflict_vs.push_back(conflict_vs[chd_ci]);
					if (psd_conflict_vs.empty())
						count_ci = this->unmarked_branches[ci].size();
					else
					{
						for (pair<int, int> p : this->unmarked_branches[ci])
						{
							bool flag = true;
							for (int cv : psd_conflict_vs)
							{ if (p.first == cv) { flag = false; break; }}
							count_ci += flag;
						}
					}
				}
				else if (chd_type > 1) // multi-psd chd
					count_ci = math_choose(this->unmarked_branches[ci].size(), chd_type);
			}
			else // marked branch (only one)
			{
				pair<int, int> p = this->marked_branches[ci][choice_i-1];
				if (chd_type == 0) // ordinary child
					count_ci = this->chd_pointers[p.first]->expand(p.second, conflict_vs);
				else // psd_chd
				{
					int chd_u = query->getBranchSender(this->curr_u, ci);
					vector<int> psd_conflict_vs;
					count_ci = 1;
					for (int chd_ci : query->getRelatedConflictIndices(chd_u))
					{
						if (p.first == conflict_vs[chd_ci])
							{ count_ci = 0; break; }
					}
				}
			}
			count *= count_ci;
		}
		return count;
	}

	void printMapping()
	{
		cout << "(Mapping) (" << ncol+1 << ") [ ";
		for (int i = 0; i < ncol; i++)
			cout << mapping[i] << ", ";
		cout << self << "]" << endl;
	}

	void print()
	{
		SIQuery* query = (SIQuery*)getQuery();
		cout << "~~~Printing Branch~~~" << endl;
		this->printMapping();
		cout << "curr_u: " << curr_u << endl;
		cout << "mapping_marker: " << mapping_marker << endl;
		cout << "tree_markers & conflux_values: " << endl;
		for (int ti : tree_indices)
			cout << "[ti=" << ti << "]: " << tree_markers[ti] << " | " 
				 << conflux_values[ti] << endl;

		for (int i = 0; i < 2; i++)
		{
			vector<vector<pair<int, int>>> *p;
			if (i == 0)
			{
				cout << "Unmarked branches: " << endl;
				p = &this->unmarked_branches;
			}
			else 
			{
				cout << "Marked branches: " << endl;
				p = &this->marked_branches;
			}
			
			for (int si = 0; si < p->size(); si++)
			{
				cout << "\t Child State " << si << endl;
				cout << "\t \t Number of choices: " << (*p)[si].size() << endl;
				for (int ci = 0; ci < (*p)[si].size(); ci++)
				{
					cout << "\t \t - ";
					int chd_type = query->getChdTypes(this->curr_u)[si];
					if (chd_type > 0)
						cout << "(Psd Child) " << (*p)[si][ci].first << endl;
					else if (chd_type == 0)
					{
						SIBranch *curr = chd_pointers[(*p)[si][ci].first];
						cout << "(Child) " << ci << endl;
						cout << "<Begin printing (Child) " << ci << ">" << endl;
						curr->print();
						cout << "<End printing (Child) " << ci << ">" << endl;
					}
				}
			}
		}
	}

	void print_simple()
	{
		cout << "~~~Printing Branch~~~SIMPLE~~~" << endl;
		this->printMapping();
		cout << "curr_u: " << curr_u << endl;
		cout << "mapping_marker: " << mapping_marker << endl;
		cout << "tree_indices size: " << tree_indices.size() << endl;

		for (int i = 0; i < 2; i++)
		{
			vector<vector<pair<int, int>>> *p;
			if (i == 0)
			{
				cout << "Unmarked branches: " << endl;
				p = &this->unmarked_branches;
			}
			else 
			{
				cout << "Marked branches: " << endl;
				p = &this->marked_branches;
			}
			
			for (int si = 0; si < p->size(); si++)
			{
				cout << "\t Child State " << si << endl;
				cout << "\t \t Number of choices: " << (*p)[si].size() << endl;
			}
		}
	}
};

ibinstream& operator<<(ibinstream& m, const SIBranch& branch)
{
    m << branch.ncol;
	for (int i = 0; i < branch.ncol; i++)
		m << branch.mapping[i];
	m << branch.self << branch.curr_u << branch.T;

	int sz = branch.chd_pointers.size();
    m << sz;
    for (int i = 0; i < sz; i++)
    	m << (*branch.chd_pointers[i]);

	m << branch.unmarked_branches << branch.marked_branches;

	m << branch.tree_indices << branch.tree_markers << branch.conflux_values;
    return m;
}

obinstream& operator>>(obinstream& m, SIBranch& branch)
{
	m >> branch.ncol;
	branch.mapping = new int[branch.ncol];

	for (int i = 0; i < branch.ncol; i++)
		m >> branch.mapping[i];
	m >> branch.self >> branch.curr_u >> branch.T;

	int sz;
    m >> sz;
	branch.chd_pointers.resize(sz);
    for (int i = 0; i < sz; i++)
	{
		SIBranch *b = new SIBranch();
		m >> (*b);
		branch.chd_pointers[i] = b;
	}

	m >> branch.unmarked_branches >> branch.marked_branches;

	m >> branch.tree_indices >> branch.tree_markers >> branch.conflux_values;
    return m;
}


#endif