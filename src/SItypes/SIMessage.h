#ifndef SIMESSAGE_H
#define SIMESSAGE_H

//--------SIMessage = <type, key, value, mapping, branch>--------
//  e.g. <type = LABEL_INFOMATION, int1 = vertex, int2 = label>
//	     <type = IN_MAPPING, int1 = curr_u, int2 = nrow>
//	     <type = OUT_MAPPING, int1 = vID, int2 = curr_u>

// 		 <type = DEGREE, key = vertex, value = degree>
//		 <type = NEIGHBOR_PAIR, p_int = edge>
//		 <type = BRANCH_RESULT, mapping, value = curr_u>
// 		 <type = BRANCH, branch, value = curr_u>
//		 <type = MAPPING_COUNT, value = count>
//		 <type = CANDIDATE, key = vertex, v_int = candidates of v>

struct SIMessage
{
	int type, int1, int2;
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

	SIMessage(int type, int int1, int int2)
	{ //LABEL_INFOMATION
		this->type = type;
		this->int1 = int1;
		this->int2 = int2;
	}
	
	SIMessage(int type, int int1, int int2, int *mappings)
	{ //IN_MAPPING
		this->type = type;
		this->int1 = int1;
		this->int2 = int2;
		this->mappings = mappings;
	}

	SIMessage(int type, int int1, int int2, vector<int*>* passed_mappings)
	{ //OUT_MAPPING
		this->type = type;
		this->int1 = int1;
		this->int2 = int2;
		this->passed_mappings = passed_mappings;
	}
/*
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
};

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 0,
	IN_MAPPING = 1, // 1121
	OUT_MAPPING = 2, // 1121
	BRANCH_RESULT = 3,
	BRANCH = 4,
	CANDIDATE = 5,
	DEGREE = 6,
	NEIGHBOR_PAIR = 7
};

ibinstream & operator<<(ibinstream &m, const SIMessage &msg)
{
	m << msg.type;
	switch (msg.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m << msg.int1 << msg.int2;
	case MESSAGE_TYPES::OUT_MAPPING:
		m << msg.int1 << msg.int2; //vID & curr_u

		int nrow = msg.passed_mappings->size();
		int ncol = step_num()-1;
		if (ncol == 0) nrow = 1; //for the first step
		m << nrow;
		for (int i = 0; i < nrow; i++)
			for (int j = 0; j < ncol; j++)
				m << ((*msg.passed_mappings)[i])[j];
		break;
		/*
	case MESSAGE_TYPES::DEGREE:
		m << v.key << v.value;
		break;
	case MESSAGE_TYPES::NEIGHBOR_PAIR:
		m << v.p_int.first << v.p_int.second;
		break;
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

obinstream & operator>>(obinstream &m, SIMessage &msg)
{
	m >> msg.type;
	switch (msg.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m >> msg.int1 >> msg.int2;
	case MESSAGE_TYPES::OUT_MAPPING:
		msg.type = MESSAGE_TYPES::IN_MAPPING;
		int vID, ncol = step_num();
		m >> vID;
		m >> msg.int1; //curr_u
		m >> msg.int2; //nrow
		msg.mappings = new int[msg.int2 * ncol];
		for (int i = 0; i < msg.int2 * ncol; i++)
		{
			if ((i+1)%ncol == 0)
				msg.mappings[i] = vID;
			else
				m >> msg.mappings[i];
		}
		break;
		/*
	case MESSAGE_TYPES::DEGREE:
		m >> v.key >> v.value;
		break;
	case MESSAGE_TYPES::NEIGHBOR_PAIR:
		m >> v.p_int.first >> v.p_int.second;
		break;
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