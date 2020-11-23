#ifndef SIMESSAGE_H
#define SIMESSAGE_H

//--------SIMessage = <type, key, value, mapping, branch>--------
//  e.g. <type = LABEL_INFOMATION, key = vertex, value = label>
// 		 <type = DEGREE, key = vertex, value = degree>
//		 <type = NEIGHBOR_PAIR, p_int = edge>
//	     <type = MAPPING, mapping, value = next_u>
//		 <type = BRANCH_RESULT, mapping, value = curr_u>
// 		 <type = BRANCH, branch, value = curr_u>
//		 <type = MAPPING_COUNT, value = count>
//		 <type = CANDIDATE, key = vertex, v_int = candidates of v>

struct SIMessage
{
	int type;

	int id;
	int curr_u;
	int nrow;
	int *mappings;
	vector<int*>* passed_mappings;
	/*
	int* ints;
	SIKey* keys;

	SIKey key;
	int value;
	vector<int> v_int;
	pair<int, int> p_int;
	Mapping mapping;
	vector<Mapping> mappings;
	SIBranch branch;
	*/
	SIMessage()
	{
	}
/*
	SIMessage(int type, SIKey vertex, int value)
	{ // for degree or label information
		this->type = type;
		this->key = vertex;
		this->value = value;
	}

	SIMessage(int type, pair<int, int> p_int)
	{
		this->type = type;
		this->p_int = p_int;
	}

	SIMessage(int type, Mapping mapping, uID next_u)
	{ // for branch result
		this->type = type;
		this->mapping = mapping;
		this->value = next_u; // or curr_u
	}

	SIMessage(int type, int nrow, int ncol, SIKey *pKey)
	{ // for mapping
		this->type = type;
		this->ints = new int[2];
		this->ints[0] = nrow;
		this->ints[1] = ncol;
		this->keys = pKey;
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
	*/
	SIMessage(int type, int curr_u, int nrow, int *mappings)
	{ // for this worker
		this->type = type;
		this->curr_u = curr_u;
		this->nrow = nrow;
		this->mappings = mappings;
	}

	SIMessage(int type, int id, int next_u, vector<int*>* passed_mappings)
	{ // for other workers
		this->type = type;
		this->id = id;
		this->curr_u = next_u;
		this->passed_mappings = passed_mappings;
	}
};

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 0,
	MAPPING = 1, // 1121
	BRANCH_RESULT = 2,
	BRANCH = 3,
	MAPPING_COUNT = 4,
	CANDIDATE = 5,
	DEGREE = 6,
	NEIGHBOR_PAIR = 7
};

ibinstream & operator<<(ibinstream & m, const SIMessage & v)
{
	m << v.type;
	switch (v.type)
	{
		/*
	case MESSAGE_TYPES::LABEL_INFOMATION:
	case MESSAGE_TYPES::DEGREE:
		m << v.key << v.value;
		break;
	case MESSAGE_TYPES::NEIGHBOR_PAIR:
		m << v.p_int.first << v.p_int.second;
		break;*/
	case MESSAGE_TYPES::MAPPING:
		m << v.id;
		m << v.curr_u;

		int nrow = v.passed_mappings->size();
		int ncol = step_num()-1;
		if (ncol == 0) nrow = 1; //for the first step
		m << nrow;
		for (int i = 0; i < nrow; i++)
			for (int j = 0; j < ncol; j++)
				m << (*v.passed_mappings)[i][j];
		break;
		/*
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
		*/
	}
	return m;
}

obinstream & operator>>(obinstream & m, SIMessage & v)
{
	m >> v.type;
	switch (v.type)
	{
		/*
	case MESSAGE_TYPES::LABEL_INFOMATION:
	case MESSAGE_TYPES::DEGREE:
		m >> v.key >> v.value;
		break;
	case MESSAGE_TYPES::NEIGHBOR_PAIR:
		m >> v.p_int.first >> v.p_int.second;
		break;
		*/
	case MESSAGE_TYPES::MAPPING:
		int vID;
		m >> vID;
		m >> v.curr_u;

		int ncol = step_num();
		m >> v.nrow;
		v.mappings = new int[v.nrow * ncol];
		for (int i = 0; i < v.nrow * ncol; i++)
		{
			for (int j = 0; j < ncol - 1; i++)
				m >> v.mappings[i];
			v.mappings[i] = vID;
		}
		break;
		/*
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
		*/
	}
	return m;
}

#endif