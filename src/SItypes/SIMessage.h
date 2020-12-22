#ifndef SIMESSAGE_H
#define SIMESSAGE_H

struct SIMessage
{
	int type, curr_u, nrow, ncol, vID, dvID, dwID;
	int *mappings;
	vector<int*> *passed_mappings;
	vector<int*> *dummies;
	SIBranch *branch;

	SIMessage()
	{
	}

	SIMessage(int type, int neighbor, int label)
	{ //LABEL_INFOMATION
		this->type = type;
		this->ncol = neighbor;
		this->nrow = label;
	}
	
	SIMessage(int type, int int1, int int2, int *mappings)
	{ //IN_MAPPING
		this->type = type;
		this->int1 = int1;
		this->int2 = int2;
		this->mappings = mappings;
	}

	SIMessage(int type, vector<int*> *passed_mappings, vector<int*> *dummies,
		int curr_u, int nrow, int ncol, int vID)
	{ //OUT_MAPPING
		this->type = type;
		this->passed_mappings = passed_mappings;
		this->dummies = dummies;
		this->curr_u = curr_u;
		this->nrow = nrow;
		this->ncol = ncol;
		this->vID = vID;
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
	IN_MAPPING = 1,
	OUT_MAPPING = 2,
	BMAPPING = 3,
};

ibinstream & operator<<(ibinstream &m, const SIMessage &msg)
{
	m << msg.type;
	switch (msg.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m << msg.int1 << msg.int2;
	case MESSAGE_TYPES::OUT_MAPPING:
		m << msg.vID << msg.curr_u; //vID & curr_u
		m << msg.nrow;
		for (int i = 0; i < msg.nrow; i++)
			for (int j = 0; j < msg.ncol; j++)
				m << ((*msg.passed_mappings)[i])[j];
		break;
	case MESSAGE_TYPES::BMAPPING:
	// fill up this
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