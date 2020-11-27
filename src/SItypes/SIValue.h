#ifndef SIVALUE_H
#define SIVALUE_H

#include "SIKey.h"

struct KeyLabel
{
	SIKey key;
	int label;

	KeyLabel() {}

	KeyLabel(SIKey key, int label)
	{
		this->key = key;
		this->label = label;
	}
};

ibinstream & operator<<(ibinstream & m, const KeyLabel & kl){
	m << kl.key << kl.label;
	return m;
}

obinstream & operator>>(obinstream & m, KeyLabel & kl){
	m >> kl.key >> kl.label;
	return m;
}


struct SIValue
{
	int label;
	int degree;
	vector<KeyLabel> nbs_vector;
	hash_set<int> nbs_set;
	hash_map<int, int> nblab_dist; //distribution of labels

	inline bool hasNeighbor(int &vID)
	{
		return nbs_set.find(vID) != nbs_set.end();
	}

	int countOccurrences(vector<int> &labels, vector<int> &counts)
	{
		int prod = 1;
		for (int i = 0; i < labels.size(); i++)
		{
			int label = labels[i];
			int count = counts[i];
			int num = nblab_dist[label];
			for (int j = 0; j < count; j++)
				prod = prod * (num - j) / (j + 1);
		}
		return prod;
	}
};

ibinstream & operator<<(ibinstream & m, const SIValue & v){
	m << v.label << v.degree << v.nbs_vector;
	return m;
}

obinstream & operator>>(obinstream & m, SIValue & v){
	m >> v.label >> v.degree >> v.nbs_vector;
	return m;
}

#endif
