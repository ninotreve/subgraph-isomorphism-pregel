#ifndef SIBRANCH_H
#define SIBRANCH_H


//==========================================================================

struct SIBranch
{
	int *mapping;
	int self;
	int ncol;
	int curr_u;
	vector<vector<SIBranch*>> chd;
	vector<vector<int>> psd_chd;
	vector<SIBranch*> special_chd; //0225

	SIBranch() {};

	SIBranch(int *p, int ncol, int curr_u)
	{
		this->mapping = p;
		this->ncol = ncol;
		this->curr_u = curr_u;
	}

	SIBranch(int *p, int self, int ncol, int curr_u)
	{
		this->mapping = p;
		this->self = self;
		this->ncol = ncol;
		this->curr_u = curr_u;
	}

	void addChd(int index, SIBranch* branch)
	{
		this->chd[index].push_back(branch);
	}

	void addPsdChd(int index, int nbr)
	{
		this->psd_chd[index].push_back(nbr);
	}

/*
	int expand()
	{
		int count = 1;
		for (int i = 0; i < this->chd.size(); i++)
		{
			int counti = 0;
			for (int j = 0; j < this->chd[i].size(); j++)
				counti += this->chd[i][j]->expand();
			count *= counti;
		}

		for (int i = 0; i < this->psd_chd.size(); i++)
			count *= this->psd_chd[i].size();

		return count;
	}
*/

	int expand()
	{ // special case
		int count = 1;
		for (int i = 0; i < this->chd.size(); i++)
		{
			int counti = 0;
			for (int j = 0; j < this->chd[i].size(); j++)
				counti += this->chd[i][j]->expand();
			count *= counti;
		}

		for (int i = 0; i < this->psd_chd.size(); i++)
			count *= this->psd_chd[i].size();

		for (int i = 0; i < this->special_chd.size(); i++)
		{
			int vertex = this->special_chd[i]->self;
			int counti = 0;
			for (int j = 0; j < this->chd[0].size(); j++)
			{
				if (this->chd[0][j]->self != vertex)
					counti += this->chd[0][j]->expand();
			}
		
			count += counti;
		}

		return count;
	}

	bool isValid()
	{
		if (!this->special_chd.empty()) //0225
			return true;

		for (int i = 0; i < this->chd.size(); i++)
			if (this->chd[i].empty())
				return false;

		for (int i = 0; i < this->psd_chd.size(); i++)
			if (this->psd_chd[i].empty())
				return false;	

		return true;
	}

	void printBranch()
	{
		cout << "Mapping: [ ";
		for (int i = 0; i < ncol; i++)
			cout << mapping[i] << " ";
		cout << self << "]" << endl;

		cout << "Children Size: " << chd.size() << endl;
		for (int i = 0; i < chd.size(); i++)
		{
			cout << "Query children No. " << i+1 << endl;
			for (int j = 0; j < chd[i].size(); j++)
			{
				cout << "\t Choice " << j+1 << ": ";
				chd[i][j]->printBranch();
				cout << endl;
			}
		}
		
		cout << "Pseudo Children Size: " << psd_chd.size() << endl;
		for (int i = 0; i < psd_chd.size(); i++)
		{
			cout << "Query children No. " << i+1 << endl;
			for (int j = 0; j < psd_chd[i].size(); j++)
				cout << "\t Choice " << j+1 << ": " << psd_chd[i][j] << endl;
		}
	}
};

/*
	int expand()
	{
		vector<Mapping> v;
		v.push_back(this->p);
		for (vector<SIBranch> &b : this->branches)
			v = crossJoin(v, b);

		return v;
	}


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
*/

ibinstream& operator<<(ibinstream& m, const SIBranch& branch)
{
	int ncol = branch.ncol + 1;
    m << ncol;
	for (int i = 0; i < ncol; i++)
		m << branch.mapping[i];
	m << branch.self;
	m << branch.curr_u;

	int sz = branch.chd.size();
    m << sz;
    for (int i = 0; i < sz; i++)
    {
		int szi = branch.chd[i].size();
    	m << szi;
    	for (int j = 0; j < szi; j++)
			m << (*branch.chd[i][j]);
    }

	sz = branch.psd_chd.size();
    m << sz;
    for (int i = 0; i < sz; i++)
    {
		int szi = branch.psd_chd[i].size();
    	m << szi;
    	for (int j = 0; j < szi; j++)
			m << branch.psd_chd[i][j];
    }

    return m;
}

obinstream& operator>>(obinstream& m, SIBranch& branch)
{
	m >> branch.ncol;
	branch.mapping = new int[branch.ncol];

	for (int i = 0; i < branch.ncol; i++)
		m >> branch.mapping[i];

	m >> branch.curr_u;

	int sz;
    m >> sz;
	branch.chd.resize(sz);
    for (int i = 0; i < sz; i++)
    {
		int szi;
		m >> szi;
		branch.chd[i].resize(szi);
    	for (int j = 0; j < szi; j++)
		{
			SIBranch *b = new SIBranch();
			m >> (*b);
			branch.chd[i][j] = b;
		}
    }

    m >> sz;
	branch.psd_chd.resize(sz);
    for (int i = 0; i < sz; i++)
    {
		int szi;
		m >> szi;
		branch.psd_chd[i].resize(szi);
    	for (int j = 0; j < szi; j++)
			m >> branch.psd_chd[i][j];
    }

    return m;
}


#endif