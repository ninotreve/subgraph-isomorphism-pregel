// dev version
#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "utils/Query.h"
using namespace std;

#define LEVEL (step_num()-1)
#define MPRINT(str) \
	if (get_worker_id() == MASTER_RANK) \
		printf("%s\n", (str));


//#define DEBUG_MODE_ACTIVE 1
//#define DEBUG_MODE_BRANCH 1
//#define DEBUG_MODE_MSG 1 
//#define DEBUG_MODE_RESULT_COUNT 1


//input line format:
//  vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
//output line format:
//  # MATCH
//  query_vertexID \t data_vertexID

#include "SItypes/SIKey.h"
#include "SItypes/SIValue.h"
#include "SItypes/SIBranch.h"
#include "SItypes/SIQuery.h"
#include "SItypes/SIAggregator.h"
#include "SItypes/SIMessage.h"
#include "SItypes/SICandidate.h"
#include "SItypes/bloom_filter.h"

//===============================================================

// the first int for anc_u, the second int for curr_u's branch_num.
//typedef hash_map<int, map<int, vector<Mapping> > > mResult;

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
public:
	long mapping_count;
	long tree_count;
	
	// the following two vectors have the same length
	// for leaf vertex, final_u > 0; for dummy vertex, final_u < 0
	vector<int> final_us;
	// for different final_u, including markers, unmarked/marked branches
	vector<vector<SIBranch*>> final_results;

	// for conflicts
	vector<int> mapped_us;

	void preprocess(MessageContainer & messages, WorkerParams &params)
	{
		//convert vector to set & build nblab_dist
		for (int i = 0; i < value().degree; ++i)
		{
			value().nbs_set.insert(value().nbs_vector[i].key.vID);
		}
		vote_to_halt();
	}

	void filter(MessageContainer & messages)
	{
		return;
	}

	bool check_feasibility(int *mapping, int query_u, int vID)
	{ // check vertex uniqueness and backward neighbors 
		SIQuery* query = (SIQuery*)getQuery();
		// check vertex uniqueness
		for (int &b_level : query->getBSameLabPos(query_u))
			if (vID == mapping[b_level])
				return false;

		// check backward neighbors
		for (int &b_level : query->getBNeighborsPos(query_u))
			if (! this->value().hasNeighbor(mapping[b_level]))
				return false;
		return true;
	}

	int build_dummy_vertex(SIBranch* b)
	{
		//Implementation 0313: 
		//  The full mapping is put into the dummy vertex.
		//  Mapping and dummies are separated in the enumerate phase.
		int dummyID = create_dummy_vertex_id();

		SIVertex* v = new SIVertex;
		v->id = SIKey(dummyID, id.wID);
		v->final_us.push_back(-1);
		v->final_results.resize(1);
		v->final_results[0].push_back(b);
		this->add_vertex(v);
		return dummyID;
	}

	SIMessage copy_message(SIMessage msg)
	{
		int new_ncol;
		switch (msg.type)
		{
			case BMAPPING_W_SELF:
				new_ncol = msg.chd_constraint.size() + 3;
				break;
			case BMAPPING_WO_SELF:
				new_ncol = msg.chd_constraint.size() + 2;
				break;
			case OUT_MAPPING:
				new_ncol = msg.ncol + 1;
				break;		
		}

		int *mappings = new int[msg.nrow * new_ncol];

		if (msg.type == OUT_MAPPING)
		{
			for (int i = 0, j; i < msg.nrow; i++)
			{
				for (j = 0; j < msg.ncol; j++)
					mappings[i*new_ncol + j] = ((*msg.passed_mappings)[i])[j];
				mappings[i*new_ncol + j] = msg.vID;
			}
		}
		else
		{
			for (int i = 0, j; i < msg.nrow; i++, j=0)
			{
				for (int k : msg.chd_constraint)
					mappings[i*new_ncol + j++] = ((*msg.passed_mappings)[i])[k];
				if (msg.type == BMAPPING_W_SELF)
					mappings[i*new_ncol + j++] = msg.vID;
				mappings[i*new_ncol + j++] = (*msg.dummy_vs)[i];
				mappings[i*new_ncol + j] = msg.wID;
			}
		}

		/* Print out the copied message
		cout << "[Message]" << endl;
		cout << "nrow: " << msg.nrow << " ncol: " << new_ncol << endl;
		for (int i = 0; i < msg.nrow*new_ncol; i++)
			cout << mappings[i] << " ";
		cout << endl;
		*/

		return SIMessage(MESSAGE_TYPES::IN_MAPPING, mappings,
			msg.curr_u, msg.nrow, new_ncol, msg.markers);
	}

	void addPsdChildren(SIBranch *b, int u_index, int msg_vID, int msg_wID, 
		int result_index)
	{
		// u_index: the index in final_us
		// result_index: the index in final_results[u_index]
		// msg_vID, msg_wID: where the response messages are sent to
		SIQuery* query = (SIQuery*)getQuery();
		vector<int> &ps_chds = query->getPseudoChildren(b->curr_u);
		int chd_sz = query->getChildren(b->curr_u).size();
		for (int i = 0; i < ps_chds.size(); i++)
		{
			int ps_chd = ps_chds[i];
			int label = query->getLabel(ps_chd);
			if (query->hasConflict(ps_chd))
			{   // send a message of psd_request
				vector<vector<int>> neighbors_map = vector<vector<int>>(get_num_workers());
				// Construct neighbors_map: 
				// Loop through neighbors and select out ones 
				// with right labels && FEASIBLE
				for (int j = 0; j < value().degree; ++j)
				{
					KeyLabel &kl = value().nbs_vector[j];
					if ((label == kl.label) && 
						check_feasibility(b->mapping, ps_chd, kl.key.vID))
						neighbors_map[kl.key.wID].push_back(kl.key.vID);
				}
				// send messages to neighbors
				for (int wID = 0; wID < get_num_workers(); wID++)
				{
					if (neighbors_map[wID].empty())
						continue;

					send_messages(wID, neighbors_map[wID], 
						SIMessage(PSD_REQUEST, ps_chd, u_index,
						msg_vID, msg_wID, result_index, chd_sz+i));
				}
			}
			else
			{
				int type = query->getChdTypes(b->curr_u)[chd_sz+i];
				if (type > 0)
				{
					for (int ni = 0; ni < value().degree; ni++)
					{
						KeyLabel &kl = value().nbs_vector[ni];
						if ((label == kl.label) && 
							check_feasibility(b->mapping, ps_chd, kl.key.vID))
							b->unmarked_branches[chd_sz+i]
								.push_back(make_pair(kl.key.vID, 0));
					}
				}
				else
					b->unmarked_branches[chd_sz+i]
						.push_back(make_pair(-1, -1));
			}
		}
	}

	virtual void compute(MessageContainer &messages, WorkerParams &params)
	{
		SIQuery* query = (SIQuery*)getQuery();
		SIAgg* agg = (SIAgg*)get_aggregator();
		double t, t1, t2;

#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID
			 << " Worker ID " << id.wID << endl;
#endif

		if (id.vID < 0) //that newly built dummy vertex
		{
			for (int i = 0; i < messages.size(); i++)
			{
				SIMessage &msg = messages[i];
#ifdef DEBUG_MODE_MSG
				cout << "Message: " << i << endl;
				msg.print();
#endif
				if (msg.type == PSD_RESPONSE)
				{
					SIBranch *b = this->final_results[msg.u_index][msg.nrow];
					pair<int, int> p = make_pair(msg.vID, msg.curr_u);
					if (msg.curr_u == 0)
						b->unmarked_branches[msg.ncol].push_back(p);
					else
						b->marked_branches[msg.ncol].push_back(p);
				}
			}
			vote_to_halt();
			return;
		}

		// arrange messages
		vector<int> vector_u = query->getBucket(LEVEL, value().label);
		int n_u = vector_u.size();
		int curr_u;
		vector<vector<int>> messages_classifier = vector<vector<int>>(n_u);
		if (step_num() == 1)
		{ // initialize
			this->final_us.clear();
			this->final_results.clear();
			this->mapped_us.clear();

			if ((value().label != query->getLabel(query->root)))
			{
				vote_to_halt();
				return;
			}
		}
		else
		{
			for (int i = 0; i < messages.size(); i++)
			{
				SIMessage &msg = messages[i];
#ifdef DEBUG_MODE_MSG
				cout << "Message: " << i << endl;
				msg.print();
#endif
				if (msg.type == PSD_RESPONSE)
				{
					SIBranch *b = this->final_results[msg.u_index][msg.nrow];
					pair<int, int> p = make_pair(msg.vID, msg.curr_u);
					if (msg.curr_u == 0)
						b->unmarked_branches[msg.ncol].push_back(p);
					else
						b->marked_branches[msg.ncol].push_back(p);
				}
				else
				{
					int bucket_num = query->getBucketNumber(msg.curr_u);
					messages_classifier[bucket_num].push_back(i);
				}
			}
		}
		
		// main computation
		// cout << "main computation checked" << endl;
		for (int bucket_num = 0; bucket_num < n_u; bucket_num ++)
		{
			if (step_num() != 1 && messages_classifier[bucket_num].empty())
				continue;

			curr_u = vector_u[bucket_num];
			int conflict_number = 0;
			for (int mapped_u : mapped_us)
				conflict_number += query->getConflictNumber(curr_u, mapped_u);
			this->mapped_us.push_back(curr_u);

			vector<int> &next_us = query->getChildren(curr_u);
			int sz = next_us.size() + query->getPseudoChildren(curr_u).size();

			vector<int*>* passed_mappings = new vector<int*>();
			vector<int>* markers = new vector<int>();
			if (LEVEL == 0) markers->push_back(0);
			vector<int>* dummy_vs = new vector<int>();

			bool is_branch = query->isBranch(curr_u);
			bool is_pseudo = query->isPseudo(curr_u);
			bool is_leaf = next_us.empty();
			//cout << "is_branch : " << is_branch << " is_leaf : " << is_leaf << endl;

			// loop through messages and check feasibilities
			if (is_pseudo)
			{
				for (int msgi : messages_classifier[bucket_num])
				{
					SIMessage &msg = messages[msgi];
					send_messages(msg.wID, {msg.vID}, 
						SIMessage(PSD_RESPONSE, conflict_number, msg.u_index, 
							id.vID, id.wID, msg.nrow, msg.ncol));
				}
				continue;
			}

			if (is_branch)
			{
				// special case: root branch vertex
				if (step_num() == 1)
				{
					dummy_vs->push_back(id.vID);
					SIBranch* b = new SIBranch(NULL, id.vID, 0, curr_u, 0);
					addPsdChildren(b, 0, id.vID, id.wID, 0);
#ifdef DEBUG_MODE_BRANCH
					b->print();
#endif
					if (query->isLeaf(curr_u))
						this->final_us.push_back(curr_u);
					else
						this->final_us.push_back(-1); // curr_u = b->curr_u
					this->final_results.resize(1);
					this->final_results[0].push_back(b);
				}

				for (int msgi : messages_classifier[bucket_num])
				{
					SIMessage &msg = messages[msgi];
					for (int i = 0; i < msg.nrow; i++)
					{
						int *new_mapping = msg.mappings + i*msg.ncol;
						//cout << "new_mapping[0]: " << new_mapping[0] << endl;
						if (check_feasibility(new_mapping, curr_u, id.vID))
						{
							passed_mappings->push_back(new_mapping);
							markers->push_back(0); // zero out at dummy

							SIBranch* b = new SIBranch(new_mapping, id.vID,
								msg.ncol, curr_u, (*msg.markers)[i] + conflict_number);
							int dummyID = build_dummy_vertex(b);
							dummy_vs->push_back(dummyID);
							addPsdChildren(b, 0, dummyID, id.wID, 0);
#ifdef DEBUG_MODE_BRANCH
							b->print();
#endif
						}
					}
				}
			}
			else if (is_leaf)
			{
				int final_index = this->final_us.size();
				this->final_us.push_back(curr_u);
				this->final_results.push_back(vector<SIBranch*>());
				for (int msgi : messages_classifier[bucket_num])
				{
					SIMessage &msg = messages[msgi];
					for (int i = 0; i < msg.nrow; i++)
					{
						int *new_mapping = msg.mappings + i*msg.ncol;
						if (check_feasibility(new_mapping, curr_u, id.vID))
						{
							SIBranch* b = new SIBranch(new_mapping, id.vID,
								msg.ncol, curr_u, (*msg.markers)[i] + conflict_number);
							addPsdChildren(b, final_index, id.vID, id.wID,
								this->final_results[final_index].size());
							this->final_results[final_index].push_back(b);
#ifdef DEBUG_MODE_BRANCH
							b->print();
#endif
						}
					}
				}
			}
			else // not branch nor leaf
			{
				for (int msgi : messages_classifier[bucket_num])
				{
					SIMessage &msg = messages[msgi];
					for (int i = 0; i < msg.nrow; i++)
					{
						int *new_mapping = msg.mappings + i*msg.ncol;
						if (check_feasibility(new_mapping, curr_u, id.vID))
						{
							passed_mappings->push_back(new_mapping);
							markers->push_back((*msg.markers)[i] + conflict_number);
						}
					}
				}
			}

			//Continue mapping: send mappings to children
			if (!passed_mappings->empty() || step_num() == 1)
			{
				vector<vector<int>> neighbors_map = vector<vector<int>>(get_num_workers());
				int type = -1;

				for (int next_u_index = 0; next_u_index < next_us.size(); next_u_index++)
				{
					int next_u = next_us[next_u_index];

					// special for branch vertex
					vector<int> chd_constraint = vector<int>();
					bool include_self = true;
					if (is_branch)
					{
						chd_constraint = query->getChdConstraint(curr_u, next_u_index);
						include_self = query->getIncludeSelf(curr_u, next_u_index);
					}

					//Construct neighbors_map: 
				  	//Loop through neighbors and select out ones with right labels
					int next_label = query->getLabel(next_u);
					for (int i = 0; i < value().degree; ++i)
					{
						KeyLabel &kl = value().nbs_vector[i];
						if (kl.label == next_label)
							neighbors_map[kl.key.wID].push_back(kl.key.vID);
					}

					//Update out_message_buffer
					SIMessage out_message;
					if (!is_branch)
						type = MESSAGE_TYPES::OUT_MAPPING;
					else if (include_self)
						type = MESSAGE_TYPES::BMAPPING_W_SELF;
					else
						type = MESSAGE_TYPES::BMAPPING_WO_SELF;

					int nrow, ncol = query->getNCOL(curr_u);
					if (LEVEL == 0)
						nrow = 1;
					else
						nrow = passed_mappings->size();
					
					out_message = SIMessage(type, passed_mappings, 
						dummy_vs, next_u, nrow, ncol, id.vID, id.wID, markers,
						chd_constraint);
#ifdef DEBUG_MODE_MSG
					out_message.print();
#endif

					for (int wID = 0; wID < get_num_workers(); wID++)
					{
						if (neighbors_map[wID].empty())
							continue;

						if (wID == get_worker_id())
							send_messages(wID, neighbors_map[wID],
								copy_message(out_message));
						else
							send_messages(wID, neighbors_map[wID], out_message);
					}

					//Clear neighbors_map
					for (int i = 0; i < get_num_workers(); i++)
						neighbors_map[i].clear();
				}
			}
			// end of continue mapping of curr_u
		}
		// end of for curr_u loop
		vote_to_halt();
	}

	void build_branch(MessageContainer &messages, SIBranch* branch, int offset)
	{
		// set up branch + send or expand
		SIQuery* query = (SIQuery*)getQuery();
		SIAgg* agg = (SIAgg*)get_aggregator();
		vector<int> &branch_senders = query->getBranchSenders(branch->curr_u);
		int *p = branch->mapping;
		double t, t1;
		branch->mapping += offset;
		branch->ncol -= offset;

		// Phase I: Organize branches (Receive messages)
		// for msg in msgs: arrange msgs according to msg's u
		// marked messages
		for (int pi = 0; pi < messages.size(); pi++)
		{
			SIMessage &msg = messages[pi];
			//cout << "Receiving Message " << pi << endl;
			//msg.branch->print();
			branch->chd_pointers.push_back(msg.branch);
			
			if (msg.branch->ncol > 10 || msg.branch->ncol < 0)
			{
				cout << "msg error "  << pi << endl;
				msg.branch->print();
			}

			int u = (msg.branch)->curr_u;
			int si; // the state index corresponding to curr_u
			for (si = 0; si < branch_senders.size() && branch_senders[si] != u; si++);

			if (si == branch_senders.size())
			{
				cout << "ERROR!" << endl;
				msg.branch->print();
			}
			
			for (int ti : msg.branch->tree_indices)
				if (msg.branch->tree_markers[ti] == 0) // unmarked
					branch->unmarked_branches[si].push_back(make_pair(pi, ti));
				else // marked
					branch->marked_branches[si].push_back(make_pair(pi, ti));
		}

		// Phase II: Enumerate Trees
		int cv = query->getCAOCValue(branch->curr_u);

		if (!branch->enumerateTrees(cv)) // invalid branch
		{
#ifdef DEBUG_MODE_BRANCH
			cout << "The branch is invalid. " << endl;
			branch->print();
#endif
			return;
		}

#ifdef DEBUG_MODE_BRANCH
		branch->print();
#endif

		// Phase III: Send to dummy or expand
		if (offset == 0)
		{
			int k = query->getConflicts().size();
			for (int ti : branch->tree_indices)
			{
				vector<int> conflict_vs = vector<int>(k, -1);
				this->mapping_count += branch->expand(ti, conflict_vs);
				this->tree_count ++;
			}
#ifdef DEBUG_MODE_RESULT_COUNT
			cout << "this->mapping_count = " << this->mapping_count << endl;
#endif
			delete branch;
		}
		else
		{
			int vID = p[offset-2];
			int wID = p[offset-1];
			SIMessage out_msg = SIMessage(BRANCH_RESULT, branch);
			if (wID == get_worker_id())
				out_msg.is_delete = false;
			send_messages(wID, {vID}, out_msg);
		}
	}

	void enumerate(MessageContainer & messages)
	{
		SIQuery* query = (SIQuery*)getQuery();
		SIAgg* agg = (SIAgg*)get_aggregator();

#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID 
			 << " Worker ID " << id.wID << endl;
#endif
		bool to_halt = true;
		double t;

		if (step_num() == 1) // initialize
		{
			this->mapping_count = 0;
			this->tree_count = 0;
		}

		// might have multiple leaf u and at most one dummy u
		for (int i = 0 ; i < this->final_us.size(); i++)
		{
			int curr_u = this->final_us[i];
			vector<SIBranch*> final_result = this->final_results[i];
			if (curr_u >= 0) // leaf vertex
			{
				int branch_num = query->getBranchNumber(curr_u);
				if (branch_num + step_num() < query->max_branch_number + 1)
				{
					to_halt = false;
					continue;
				} else if (branch_num + step_num() > query->max_branch_number + 1)
					continue;
				
				int dummy_pos = query->getDummyPos(curr_u);
				int offset;
				if (dummy_pos < 0)
					offset = 0;
				else
					offset = dummy_pos + 2;
				for (int j = 0; j < final_result.size(); j++)
					build_branch(messages, final_result[j], offset);
			}
			else if (!messages.empty()) // dummy vertex
			{
				SIBranch *b = final_result[0];
				int dummy_pos = query->getDummyPos(b->curr_u);
				int offset;
				if (dummy_pos < 0)
					offset = 0;
				else
					offset = dummy_pos + 2;

				build_branch(messages, b, offset);
			}
		}

		agg->addMappingCount(this->mapping_count);
		agg->addTreeCount(this->tree_count);

		if (to_halt)
			vote_to_halt();
	}
};

//=============================================================================

class SIWorker:public Worker<SIVertex, SIQuery, SIAgg>
{
	char buf[100];

	public:
		// C version
		// input line format:
		// vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
		virtual SIVertex* toVertex(char* line)
		{
			char * pch;
			SIVertex* v = new SIVertex;
			
			pch = strtok(line, " \t");
			if (*pch == '#') return NULL;
			int id = atoi(pch);
			v->id = SIKey(id, id % _num_workers);

			pch = strtok(NULL, " \t");
			v->value().label = (int) *pch;

			SIKey key;
			while ((pch = strtok(NULL, " ")) != NULL)
			{
				id = atoi(pch);
				key = SIKey(id, id % _num_workers);
				pch = strtok(NULL, " ");
				v->value().nbs_vector.push_back(KeyLabel(key, (int) *pch));
				//v->value().nbs_set.insert(id);
			}
			size_t sz = v->value().nbs_vector.size();
			v->value().degree = sz;
			return v;
		}

		virtual void toline(SIVertex* v, BufferedWriter & writer)
		{
			/*
			SIQuery* query = (SIQuery*)getQuery();
			int ncol = query->num;
			for (size_t i = 0, j; i < (v->final_results[0])->size(); i++)
			{
				int* mapping = (*v->final_results[0])[i];
				sprintf(buf, "# Match\n");
				writer.write(buf);

#ifdef DEBUG_MODE_RESULT
				cout << "[DEBUG] Worker ID: " << get_worker_id() << endl;
				cout << "[DEBUG] Vertex ID: " << v->id.vID << endl;
				cout << "[DEBUG] Result: [";
				for (j = 0; j < ncol-1; j++)
					cout << mapping[j] << ",";
				cout << v->id.vID << "]" << endl;
#endif

				for (j = 0; j < ncol-1; j++)
				{
					sprintf(buf, "%d %d\n", 
						query->getID(query->dfs_order[j]), mapping[j]);
					writer.write(buf);
				}
				sprintf(buf, "%d %d\n", 
					query->getID(query->dfs_order[j]), v->id.vID);
				writer.write(buf);
			}
			*/
		}

		virtual void clear_messages(vector<SIMessage> &delete_messages)
		{
			for (SIMessage &msg : delete_messages)
			{
				switch (msg.type)
				{
					case BMAPPING_W_SELF:
					case BMAPPING_WO_SELF:
						vector<int>().swap(*msg.dummy_vs);
					case OUT_MAPPING:
						vector<int*>().swap(*msg.passed_mappings);
						//vector<int>().swap(*msg.markers);
						break;					
					case IN_MAPPING:
						delete[] msg.mappings;
						break;
					
					case BRANCH_RESULT:
						if (msg.is_delete)
						{
							delete msg.branch;
							msg.branch = NULL;
						}
						break;						
				}				
			}
			delete_messages.clear();
		}
};


void pregel_subgraph(const WorkerParams & params)
{
	SIWorker worker;
	//CCCombiner_pregel combiner;
	//if(use_combiner) worker.setCombiner(&combiner);

//=============================================================================
	// OFFLINE STAGE
	MPRINT("");
	init_timers();
	StartTimer(TOTAL_TIMER);

	// STAGE 1: Load data graph
	MPRINT("Loading data graph...")
	ResetTimer(STAGE_TIMER);
	worker.load_data(params);
	StopTimer(STAGE_TIMER);
	PrintTimer("Loading data graph time", STAGE_TIMER)

	// STAGE 2: Preprocessing
	MPRINT("Preprocessing...")
	ResetTimer(STAGE_TIMER);
	SIAgg agg;
	worker.setAggregator(&agg);
	worker.run_type(PREPROCESS, params, 1);
	StopTimer(STAGE_TIMER);
	PrintTimer("Preprocessing time", STAGE_TIMER)

	StopTimer(TOTAL_TIMER);
	PrintTimer("In total, offline time", TOTAL_TIMER)

//=============================================================================
	// ONLINE STAGE
	ResetTimer(TOTAL_TIMER);
	InitTimer(COMMUNICATION_TIMER);

	vector<string> files;
	if (params.input)
		dispatchMaster(params.query_path.c_str(), files);
	else
		for (const auto & entry : filesystem::directory_iterator(params.query_path))
        	files.push_back(entry.path());

	if (get_worker_id() == MASTER_RANK)
	{
		cout << "# queries = " << files.size() << endl << endl;
		cout << "Query No., order, "
			 << "|V(Q)|, |E(Q)|, depth, max_bn, conflicts, #mappings, #trees, "
		 	 << "compute_time, match_time, enum_time, comm_time, filename" << endl;
	}
	int query_index = 1;
	double total_comp_time = 0;
	long total_mapping_count = 0;
	int count = 0;
	for (string file : files)
	{
		for (string order : params.orders)
		{
			// Reset and load query
			SIQuery query;
			worker.setQuery(&query);
			SIAgg agg;
			worker.setAggregator(&agg);
			worker.load_query(file, params.input);
		
			ResetTimer(COMPUTE_TIMER);
			InitTimer(COMMUNICATION_TIMER);

			// Build query tree
			vector<int> properties = vector<int>(5); // |V|, |E|, depth, max_bn, conflicts
			worker.build_query_tree(order, params.pseudo, properties);
			
			// Subgraph matching
			ResetTimer(STAGE_TIMER);
			worker.run_type(MATCH, params, properties[2]+1);
			StopTimer(STAGE_TIMER);
			double match_time = get_timer(STAGE_TIMER);

			// Subgraph enumeration
			ResetTimer(STAGE_TIMER);
			worker.run_type(ENUMERATE, params, properties[3]+1);
			StopTimer(STAGE_TIMER);
			double enum_time = get_timer(STAGE_TIMER);
			long mapping_count = (*((AggMat*)global_agg))[0][0];
			long tree_count = (*((AggMat*)global_agg))[2][2];
			total_mapping_count += mapping_count;
			StopTimer(COMPUTE_TIMER);

			double comp_time = get_timer(COMPUTE_TIMER);
			total_comp_time += comp_time;
			count ++;
			double comm_time = get_timer(COMMUNICATION_TIMER);

			if (get_worker_id() == MASTER_RANK)
				cout << query_index << ", " 
					<< order << ", "
					<< properties[0] << ", "
					<< properties[1] << ", "
					<< properties[2] << ", "
					<< properties[3] << ", "
					<< properties[4] << ", "
					<< mapping_count << ", "
					<< tree_count << ", "
					<< comp_time << ", "
					<< match_time << ", "
					<< enum_time << ", "
					<< comm_time << ", "
					<< file << endl;
		}
		query_index ++;
	}

	StopTimer(TOTAL_TIMER);
	PrintTimer("\nIn total, online time", TOTAL_TIMER)
	if (get_worker_id() == MASTER_RANK)
	{
		cout << "Avg comp time: " << total_comp_time/count << endl;
		cout << "Total mapping count: " << total_mapping_count << endl;
	}

}
