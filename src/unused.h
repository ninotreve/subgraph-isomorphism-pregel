	SIBranch* copyBranch(SIBranch *child_branch, int child_index)
	{
		// copy branch + delete
		SIBranch *new_branch = new SIBranch(this->mapping, this->self, 
			this->ncol, this->curr_u, this->marker);
		// copy chd and psd_chd
		int chd_sz = this->chd.size();
		new_branch->chd.resize(chd_sz);
		for (int j = 0; j < chd_sz; j++)
		{
			if (j == child_index)
				new_branch->chd[j].push_back(child_branch);
			else
				new_branch->chd[j] = this->chd[j];
		}
		new_branch->psd_chd = this->psd_chd;
		// branch.delete
		return new_branch;
	}

	int recursive_trim(int conflict_v, const vector<int> index_chain, int j)
	{ // return -1 if children[child_index] is empty (no solution)
		int child_index = index_chain[j];
		auto it = this->chd[child_index].begin(); 
		if (index_chain.size() == j+2) // base case
			while (it != this->chd[child_index].end())
				if ((*it)->mapping[index_chain[j+1]] == conflict_v)
					it = this->chd[child_index].erase(it);
				else
					it++;
		else			
			while (it != this->chd[child_index].end())
				if ((*it)->recursive_trim(conflict_v, index_chain, j+1) == -1)
					it = this->chd[child_index].erase(it);
				else
					it ++;
		
		if (this->chd[child_index].empty())
			return -1;
		else
			return 0;
	}