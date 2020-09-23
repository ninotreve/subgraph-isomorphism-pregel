#ifndef SIMESSAGE_H
#define SIMESSAGE_H

//--------SIMessage = <type, key, value, mapping, branch>--------
//  e.g. <type = LABEL_INFOMATION, key = vertex, value = label>
//	     <type = MAPPING, mapping, value = next_u>
//		 <type = BRANCH_RESULT, mapping, value = curr_u>
// 		 <type = BRANCH, branch, value = curr_u>
//		 <type = MAPPING_COUNT, value = count>
//		 <type = CANDIDATE, key = vertex, v_int = candidates of v>

struct SIMessage
{
	int type;
	SIKey key;
	int value;
	vector<int> v_int;
	vector<SIKey> mapping;
	SIBranch branch;

	SIMessage()
	{
	}

	SIMessage(int type, SIKey vertex, int label)
	{ // for label information
		this->type = type;
		this->key = vertex;
		this->value = label;
	}

	SIMessage(int type, Mapping mapping, uID next_u)
	{ // for mapping or branch result
		this->type = type;
		this->mapping = mapping;
		this->value = next_u; // or curr_u
	}

	SIMessage(int type, SIBranch branch, uID curr_u)
	{ // for new enumeration
		this->type = type;
		this->branch = branch;
		this->value = curr_u;
	}

	SIMessage(int type, int label)
	{ // for mapping count
		this->type = type;
		this->value = label;
	}

	SIMessage(int type, SIKey vertex)
	{ // for candidate initialization
		this->type = type;
		this->key = vertex;
	}

	void add_int(int i)
	{
		this->v_int.push_back(i);
	}
};

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 0,
	MAPPING = 1,
	BRANCH_RESULT = 2,
	BRANCH = 3,
	MAPPING_COUNT = 4,
	CANDIDATE = 5
};

ibinstream & operator<<(ibinstream & m, const SIMessage & v)
{
	m << v.type;
	switch (v.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m << v.key << v.value;
		break;
	case MESSAGE_TYPES::MAPPING:
	case MESSAGE_TYPES::BRANCH_RESULT:
		m << v.mapping << v.value;
		break;
	case MESSAGE_TYPES::BRANCH:
		m << v.branch << v.value;
		break;
	case MESSAGE_TYPES::MAPPING_COUNT:
		m << v.value;
		break;
	case MESSAGE_TYPES::CANDIDATE:
		m << v.key << v.v_int;
		break;
	}
	return m;
}

obinstream & operator>>(obinstream & m, SIMessage & v)
{
	m >> v.type;
	switch (v.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m >> v.key >> v.value;
		break;
	case MESSAGE_TYPES::MAPPING:
	case MESSAGE_TYPES::BRANCH_RESULT:
		m >> v.mapping >> v.value;
		break;
	case MESSAGE_TYPES::BRANCH:
		m >> v.branch >> v.value;
		break;
	case MESSAGE_TYPES::MAPPING_COUNT:
		m >> v.value;
		break;
	case MESSAGE_TYPES::CANDIDATE:
		m >> v.key >> v.v_int;
		break;
	}
	return m;
}

#endif