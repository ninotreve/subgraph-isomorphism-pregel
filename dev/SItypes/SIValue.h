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

	inline bool hasNeighbor(int &vID)
	{
		return nbs_set.find(vID) != nbs_set.end();
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
