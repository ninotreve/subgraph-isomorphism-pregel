#ifndef SIVALUE_H
#define SIVALUE_H

#include "SIKey.h"

//------------SIValue = <label, hash_map<neighbors, labels> >------------------

struct SIValue
{
	int label;
	hash_map<SIKey, int> neighbors;
};

ibinstream & operator<<(ibinstream & m, const SIValue & v){
	m << v.label << v.neighbors;
	return m;
}

obinstream & operator>>(obinstream & m, SIValue & v){
	m >> v.label >> v.neighbors;
	return m;
}

#endif
