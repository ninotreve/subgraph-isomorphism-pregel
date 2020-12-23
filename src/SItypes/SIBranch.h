#ifndef SIBRANCH_H
#define SIBRANCH_H

/*
//==========================================================================

struct SIBranch;
vector<Mapping> crossJoin(vector<Mapping> v1, vector<SIBranch> &b);

struct SIBranch
{
	int *prefix;
	int nrow;
	vector<vector<int*>> psd_chd;
	vector<vector<SIBranch*>> chd;

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

	bool isValid()
	{
		for (int i = 0; i < this->branches.size(); i++)
			if (this->branches[i].empty())
				return false;
		
		return true;
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
*/

#endif