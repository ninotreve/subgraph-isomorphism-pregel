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
	hash_set<SIKey> nbs_set; // only used when vector size > 20

	bool hasNeighbor(SIKey &k)
	{
		if (degree > 20)
		{
			return nbs_set.find(k) == nbs_set.end();
		}
		else
		{
			for (int i = 0; i < degree; ++i)
			{
				if (nbs_vector[i].key == k) return true;
			}
			return false;
		}
	}
};

ibinstream & operator<<(ibinstream & m, const SIValue & v){
	m << v.label << v.degree << v.nbs_vector << v.nbs_set;
	return m;
}

obinstream & operator>>(obinstream & m, SIValue & v){
	m >> v.label >> v.degree >> v.nbs_vector >> v.nbs_set;
	return m;
}

#endif
