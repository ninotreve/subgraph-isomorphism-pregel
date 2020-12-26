#ifndef SIMESSAGE_H
#define SIMESSAGE_H

struct SIMessage
{
	int type, curr_u, nrow, ncol, vID;
	bool delete = true;
	int *mappings;
	vector<int*> *send_mappings;
	vector<int*> *dummies;

	SIMessage()
	{
	}

	SIMessage(int type, int neighbor, int label)
	{ //LABEL_INFOMATION
		this->type = type;
		this->ncol = neighbor;
		this->nrow = label;
	}
	
	SIMessage(int type, int *mappings, int curr_u, int nrow, int ncol,
		bool delete)
	{ //IN_MAPPING
		this->type = type;
		this->curr_u = curr_u;
		this->nrow = nrow;
		this->ncol = ncol;
		this->mappings = mappings;
		this->delete = delete;
	}

	SIMessage(int type, vector<int*> *send_mappings, vector<int*> *dummies,
		int curr_u, int nrow, int ncol, int vID)
	{ //OUT_MAPPING
		this->type = type;
		this->send_mappings = send_mappings;
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
	BMAPPING_W_SELF = 3,
	BMAPPING_WO_SELF = 4
};

ibinstream & operator<<(ibinstream &m, const SIMessage &msg)
{
	m << msg.type;
	m << msg.delete;
	switch (msg.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m << msg.nrow << msg.ncol;
	case MESSAGE_TYPES::OUT_MAPPING:
		m << msg.vID << msg.curr_u << msg.nrow << (msg.ncol + 1);
		for (int i = 0; i < msg.nrow; i++)
			for (int j = 0; j < msg.ncol; j++)
				m << ((*msg.send_mappings)[i])[j];
		break;
	case MESSAGE_TYPES::BMAPPING_W_SELF:
		m << msg.curr_u << msg.nrow << (msg.ncol + 3);
		for (int i = 0; i < msg.nrow; i++)
		{
			for (int j = 0; j < msg.ncol; j++)
				m << ((*msg.send_mappings)[i])[j];
			m << msg.vID;
			for (int j = 0; j < 2; j++)
				m << ((*msg.dummies)[i])[j];
		}		
	case MESSAGE_TYPES::BMAPPING_WO_SELF:
		m << msg.curr_u << msg.nrow << (msg.ncol + 2);
		for (int i = 0; i < msg.nrow; i++)
		{
			for (int j = 0; j < msg.ncol; j++)
				m << (*msg.send_mappings)[i][j];
			for (int j = 0; j < 2; j++)
				m << (*msg.dummies)[i][j];
		}
		break;
	}
	cout << endl;
	return m;
}

obinstream & operator>>(obinstream &m, SIMessage &msg)
{
	m >> msg.type;
	m >> msg.delete;
	switch (msg.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m >> msg.nrow >> msg.ncol;
	case MESSAGE_TYPES::OUT_MAPPING:
		msg.type = MESSAGE_TYPES::IN_MAPPING;
		m >> msg.vID >> msg.curr_u >> msg.nrow >> msg.ncol;
		msg.mappings = new int[msg.nrow * msg.ncol];
		for (int i = 0; i < msg.nrow * msg.ncol; i++)
		{
			if ((i+1) % (msg.ncol) == 0)
				msg.mappings[i] = msg.vID;
			else
				m >> msg.mappings[i];
		}
		break;
	case MESSAGE_TYPES::BMAPPING_W_SELF:
	case MESSAGE_TYPES::BMAPPING_WO_SELF:
		msg.type = MESSAGE_TYPES::IN_MAPPING;
		m >> msg.curr_u >> msg.nrow >> msg.ncol;
		msg.mappings = new int[msg.nrow * msg.ncol];
		for (int i = 0; i < msg.nrow * msg.ncol; i++)
			m >> msg.mappings[i];
		break;
	}
	cout << endl;
	return m;
}

#endif