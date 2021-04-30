#ifndef SIMESSAGE_H
#define SIMESSAGE_H

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 0,
	IN_MAPPING = 1,
	OUT_MAPPING = 2,
	BMAPPING_W_SELF = 3,
	BMAPPING_WO_SELF = 4,
	BRANCH_RESULT = 5,
	PSD_REQUEST = 6,
	PSD_RESPONSE = 7
};

struct SIMessage
{
	int type, curr_u, u_index, nrow, ncol, vID, wID;
	bool is_delete = true;

	int *mappings;
	vector<int*> *passed_mappings;
	vector<int> *markers;
	vector<int> *dummy_vs;
	vector<int> chd_constraint;
	SIBranch *branch;

	SIMessage()
	{
	}

	SIMessage(int type)
	{ //LABEL_INFOMATION or PSD_REQUEST/RESPONSE
		this->type = type;
	}
	
	SIMessage(int type, int *mappings, int curr_u, int nrow, int ncol,
		bool is_delete, vector<int> *markers)
	{ //IN_MAPPING
		this->type = type;
		this->curr_u = curr_u;
		this->nrow = nrow;
		this->ncol = ncol;
		this->mappings = mappings;
		this->is_delete = is_delete;
		this->markers = markers;
	}

	SIMessage(int type, vector<int*> *passed_mappings, vector<int> *dummy_vs,
		int curr_u, int nrow, int ncol, int vID, int wID, vector<int> *markers,
		vector<int> chd_constraint)
	{ //OUT_MAPPING, B_MAPPING_W/O_SELF
		this->type = type;
		this->passed_mappings = passed_mappings;
		this->dummy_vs = dummy_vs;
		this->curr_u = curr_u;
		this->nrow = nrow;
		this->ncol = ncol;
		this->vID = vID;
		this->wID = wID;
		this->markers = markers;
		this->chd_constraint = chd_constraint;
	}

	SIMessage(int type, SIBranch *branch)
	{ // for BRANCH_RESULT
		this->type = type;
		this->branch = branch;
	}

	SIMessage(int type, int curr_u, int u_index,
		int vID, int wID, int result_index, int state_i)
	{ // for PSD_REQUEST or RESPONSE
		this->type = type;
		this->curr_u = curr_u;  // or conflict value
		this->u_index = u_index;
		this->vID = vID;
		this->wID = wID;
		this->nrow = result_index;
		this->ncol = state_i;
	}

	void print()
	{
		cout << "[Message] #COYB!" << endl;
		cout << "type = " << type << endl;
		if (type == IN_MAPPING)
		{
			cout << "type = IN_MAPPING" << endl;
			cout << "curr_u: " << this->curr_u << endl;
			cout << "nrow: " << this->nrow << endl;
			cout << "ncol: " << this->ncol << endl;
			cout << "mappings: " << endl;
			for (int i = 0; i < this->nrow * this->ncol; i++)
				cout << this->mappings[i] << " ";
			cout << endl;
			cout << "markers: " << endl;
			for (int i = 0; i < this->markers->size(); i++)
				cout << (*this->markers)[i] << " ";
			cout << endl;
		}
		else if (type == OUT_MAPPING || type == BMAPPING_W_SELF
			  || type == BMAPPING_WO_SELF)
		{
			if (type == OUT_MAPPING)
				cout << "type = OUT_MAPPING" << endl;
			if (type == BMAPPING_W_SELF)
				cout << "type == BMAPPING_W_SELF" << endl;
			if (type == BMAPPING_WO_SELF)
				cout << "type == BMAPPING_WO_SELF" << endl;
			cout << "curr_u: " << this->curr_u << endl;
			cout << "nrow: " << this->nrow << endl;
			cout << "ncol: " << this->ncol << endl;
			cout << "vID: " << this->vID << endl;
			cout << "wID: " << this->wID << endl;
			cout << "mappings: " << endl;
			if (type == OUT_MAPPING)
			{
				for (int i = 0; i < this->passed_mappings->size(); i++)
				{
					for (int j = 0; j < ncol; j++)
						cout << (*this->passed_mappings)[i][j] << " ";
					cout << endl;
				}
				cout << endl;
			}
			else
			{
				for (int i = 0; i < this->passed_mappings->size(); i++)
				{
					for (int k: chd_constraint)
						cout << (*this->passed_mappings)[i][k] << " ";
					cout << "[" << (*this->dummy_vs)[i] << "]" << endl;
				}
				cout << endl;
			}
			cout << "markers: " << endl;
			for (int i = 0; i < this->markers->size(); i++)
				cout << (*this->markers)[i] << " ";
			cout << endl;
		}
		else if (type == PSD_REQUEST || type == PSD_RESPONSE)
		{
			cout << "type = " << type << endl;
			cout << "curr_u = " << curr_u  << endl;
			cout << "u_index = " << u_index  << endl;
			cout << "vID = " << vID  << endl;
			cout << "wID = " << wID  << endl;
			cout << "nrow = " << nrow  << endl;
			cout << "ncol = " << ncol << endl;
		}
	}
};


ibinstream & operator<<(ibinstream &m, const SIMessage &msg)
{
	m << msg.type;
	m << msg.is_delete;

	int ncol, sz;
	switch (msg.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m << msg.nrow << msg.ncol;
		break;
	case MESSAGE_TYPES::OUT_MAPPING:
		sz = msg.markers->size();
		m << sz;
		for (int i = 0; i < sz; i++)
			m << (*msg.markers)[i];
		m << msg.vID << msg.curr_u << msg.nrow << (msg.ncol + 1);
		for (int i = 0; i < msg.nrow; i++)
			for (int j = 0; j < msg.ncol; j++)
				m << ((*msg.passed_mappings)[i])[j];
		break;
	case MESSAGE_TYPES::BMAPPING_W_SELF:
		sz = msg.markers->size();
		m << sz;
		for (int i = 0; i < sz; i++)
			m << (*msg.markers)[i];
		ncol = msg.chd_constraint.size() + 3;
		m << msg.curr_u << msg.nrow << ncol;
		for (int i = 0; i < msg.nrow; i++)
		{
			for (int j : msg.chd_constraint)
				m << (*msg.passed_mappings)[i][j];
			m << msg.vID << (*msg.dummy_vs)[i] << msg.wID;
		}
		break;
	case MESSAGE_TYPES::BMAPPING_WO_SELF:
		sz = msg.markers->size();
		m << sz;
		for (int i = 0; i < sz; i++)
			m << (*msg.markers)[i];
		ncol = msg.chd_constraint.size() + 2;
		m << msg.curr_u << msg.nrow << ncol;
		for (int i = 0; i < msg.nrow; i++)
		{
			for (int j : msg.chd_constraint)
				m << (*msg.passed_mappings)[i][j];
			m << (*msg.dummy_vs)[i] << msg.wID;
		}
		break;
	case MESSAGE_TYPES::BRANCH_RESULT:
		m << (*msg.branch);
		break;
	case MESSAGE_TYPES::PSD_REQUEST:
	case MESSAGE_TYPES::PSD_RESPONSE:
		m << msg.curr_u << msg.u_index << msg.vID << msg.wID << msg.nrow << msg.ncol;
		break;
	}
	return m;
}

obinstream & operator>>(obinstream &m, SIMessage &msg)
{
	m >> msg.type;
	m >> msg.is_delete;
	int sz;
	int boo;		
	SIBranch *b = new SIBranch();

	switch (msg.type)
	{
	case MESSAGE_TYPES::LABEL_INFOMATION:
		m >> msg.nrow >> msg.ncol;
		break;
	case MESSAGE_TYPES::OUT_MAPPING:
		msg.type = MESSAGE_TYPES::IN_MAPPING;
		m >> sz;
		msg.markers = new vector<int>(sz);
		for (int i = 0; i < sz; i++)
		{
			m >> boo;
			(*msg.markers)[i] = boo;
		}
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
		m >> sz;
		msg.markers = new vector<int>(sz);
		for (int i = 0; i < sz; i++)
		{
			m >> boo;
			(*msg.markers)[i] = boo;
		}
		msg.type = MESSAGE_TYPES::IN_MAPPING;
		m >> msg.curr_u >> msg.nrow >> msg.ncol;
		msg.mappings = new int[msg.nrow * msg.ncol];
		for (int i = 0; i < msg.nrow * msg.ncol; i++)
			m >> msg.mappings[i];
		break;
	case MESSAGE_TYPES::BRANCH_RESULT:
		m >> (*b);
		msg.branch = b;
		break;
	case MESSAGE_TYPES::PSD_REQUEST:
	case MESSAGE_TYPES::PSD_RESPONSE:
		m >> msg.curr_u >> msg.u_index >> msg.vID >> msg.wID >> msg.nrow >> msg.ncol;
		break;
	}
	return m;
}

#endif