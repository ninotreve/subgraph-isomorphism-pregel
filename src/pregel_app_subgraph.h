#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "utils/Query.h"
using namespace std;

#define LEVEL (step_num()-1)
#define START_TIMING(T) (T) = get_current_time();
#define STOP_TIMING(T, X, Y) this->timers[(X)][(Y)] += get_current_time() - (T);
#define MPRINT(str) \
	if (get_worker_id() == MASTER_RANK) \
		printf("%s\n", (str));

#define DEBUG_MODE_ACTIVE 1
#define DEBUG_MODE_MSG 1
#define DEBUG_MODE_PARTIAL_RESULT 1
#define DEBUG_MODE_RESULT 1


//input line format:
//  vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
//output line format:
//  # MATCH
//  query_vertexID \t data_vertexID

#include "SItypes/SIKey.h"
#include "SItypes/SIValue.h"
#include "SItypes/SIBranch.h"
#include "SItypes/SIQuery.h"
#include "SItypes/SIMessage.h"
#include "SItypes/SICandidate.h"
#include "SItypes/bloom_filter.h"

//===============================================================

// the first int for anc_u, the second int for curr_u's branch_num.
//typedef hash_map<int, map<int, vector<Mapping> > > mResult;

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
public:
	SICandidate *candidate;
	double timers[3][3];
	int mapping_count = 0;
	bool manual_active = true;

	// for dummy
	int *mapping;
	int ncol;
	bool is_dummy = false;
	
	// for leaves
	vector<int> final_us;
	vector<vector<int*>*> final_results; // for different curr_u

	void preprocess(MessageContainer & messages, WorkerParams &params)
	{
		if (step_num() == 1)
		{
			if (params.input)
			{
				//Construct neighbors_map: 
				vector<vector<int>> neighbors_map = vector<vector<int>>(get_num_workers());
				for (int i = 0; i < value().degree; ++i)
				{
					KeyLabel &kl = value().nbs_vector[i];
					neighbors_map[kl.key.wID].push_back(kl.key.vID);
				}

				//Send label and degree to neighbors
				SIMessage msg = SIMessage(LABEL_INFOMATION, id.vID, value().label);
				for (int wID = 0; wID < get_num_workers(); ++wID)
					send_messages(wID, neighbors_map[wID], msg);
			}

			//convert vector to set & build nblab_dist
			for (size_t i = 0; i < value().degree; ++i)
			{
				value().nbs_set.insert(value().nbs_vector[i].key.vID);
				if (value().nblab_dist.find(value().nbs_vector[i].label) 
					!= value().nblab_dist.end())
					value().nblab_dist[value().nbs_vector[i].label]++;
				else
					value().nblab_dist[value().nbs_vector[i].label] = 1;
			}
					
			vote_to_halt();
		}
		else
		{   // receive label and degree from neighbors, set up bloom filter
			// send neighbors to neighbors
			for (size_t i = 0; i < messages.size(); ++i)
			{
				SIMessage & msg = messages[i];
				if (msg.type == LABEL_INFOMATION)
				{
					for (size_t i = 0; i < value().degree; ++i)
					{
						if (value().nbs_vector[i].key.vID == msg.nrow)
							value().nbs_vector[i].label = msg.ncol;
					}
				}
			}
			vote_to_halt();
		}
	}

	bool check_feasibility(int *mapping, int query_u)
	{ // check vertex uniqueness and backward neighbors 
		SIQuery* query = (SIQuery*)getQuery();
		// check vertex uniqueness
		for (int &b_level : query->getBSameLabPos(query_u))
			if (this->id.vID == mapping[b_level])
				return false;

		// check backward neighbors
		for (int &b_level : query->getBNeighborsPos(query_u))
			if (! this->value().hasNeighbor(mapping[b_level]))
				return false;
		return true;
	}

	SIMessage copy_message(SIMessage msg)
	{
		int new_ncol = msg.ncol;
		switch (msg.type)
		{
			case BMAPPING_W_SELF:
				new_ncol ++;
			case BMAPPING_WO_SELF:
				new_ncol ++;
			case OUT_MAPPING:
				new_ncol ++;			
		}

		int *mappings = new int[msg.nrow * new_ncol];
		for (int i = 0, j; i < msg.nrow; i++)
		{
			for (j = 0; j < msg.ncol; j++)
			{
				mappings[i*new_ncol + j] = (*msg.send_mappings)[i][j];
			}
			if (msg.type == BMAPPING_W_SELF || msg.type == OUT_MAPPING)
			{
				mappings[i*new_ncol + j] = id.vID;
				j++;
			}
			if (msg.type == BMAPPING_W_SELF || msg.type == BMAPPING_WO_SELF)
			{
				mappings[i*new_ncol + j] = (*msg.dummies)[i][0];
				mappings[i*new_ncol + j + 1] = (*msg.dummies)[i][1];
			}
			cout << endl;
		}

		// Print out the copied message
		/*
		cout << "[Message]" << endl;
		cout << "nrow: " << msg.nrow << " ncol: " << new_ncol << endl;
		for (int i = 0; i < msg.nrow*new_ncol; i++)
			cout << mappings[i] << " ";
		cout << endl;
		*/

		return SIMessage(MESSAGE_TYPES::IN_MAPPING, mappings,
			msg.curr_u, msg.nrow, new_ncol, msg.is_delete);
	}

	virtual void compute(MessageContainer &messages, WorkerParams &params)
	{
		SIQuery* query = (SIQuery*)getQuery();
		double t, t1, t2;

#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID
			 << " Worker ID " << id.wID
			 << " Manual active: " << manual_active << endl;
#endif

		// initiate timing
		for (int i = 0; i < 3; i++)
			for (int j = 0; j < 3; j++)
				this->timers[i][j] = 0.0;

		if (this->is_dummy) //dummy vertex
		{
			for (int i = 0; i < 3; i++)
				for (int j = 0; j < 3; j++)
					this->timers[i][j] = 0.0;
			vote_to_halt();
			return;
		}

		// arrange messages
		START_TIMING(t);
		vector<int> vector_u = query->getBucket(LEVEL, value().label);
		int n_u = vector_u.size();
		vector<vector<int>> messages_classifier = vector<vector<int>>(n_u);
		if (step_num() == 1)
		{
			if (this->manual_active)
			{
				this->manual_active = false;
				if ((!params.filter) && 
					(value().label != query->getLabel(query->root)))
				{
					vote_to_halt();
					return;
				}
			}
			else
			{
				vote_to_halt();
				return;
			}
		}
		else
		{
			for (int i = 0; i < messages.size(); i++)
			{
				int bucket_num = query->getBucketNumber(messages[i].curr_u);
				messages_classifier[bucket_num].push_back(i);
			}
		}
		STOP_TIMING(t, 0, 0);

		// main computation
		START_TIMING(t);
		for (int bucket_num = 0; bucket_num < n_u; bucket_num ++)
		{
			int curr_u = vector_u[bucket_num];
			int ncol = query->getNCOL(curr_u);
			//Loop through messages and check feasibilities
			START_TIMING(t1);
			vector<int*>* passed_mappings = new vector<int*>();
			vector<int*>* send_mappings = new vector<int*>();
			vector<int*>* dummies = new vector<int*>();
			for (int msgi : messages_classifier[bucket_num])
			{
				SIMessage &msg = messages[msgi];
				if (ncol != msg.ncol)
					cout << "WRONG " << ncol << " " << msg.ncol << endl; 
				for (int i = 0; i < msg.nrow; i++)
					if (check_feasibility(msg.mappings + i*ncol, curr_u))
						passed_mappings->push_back(msg.mappings + i*ncol);
			}
			STOP_TIMING(t1, 0, 1);

			//Add dummy vertex for branch vertices
			// - turn on is_dummy
			bool is_branch = query->isBranch(curr_u), include_self;
			if (is_branch)
			{
				//Case 1: compressed_prefix.size() = 0, no constraint
				//We don't need to build dummy vertex; the dummy vertex is itself.
				//nrow=1, ncol=0+2
				//这里是不对的。对每个mapping，我们都要建立一个dummy。以后改。
				int *dummy_self = new int[2];
				dummy_self[0] = id.vID;
				dummy_self[1] = id.wID;
				send_mappings->push_back(dummy_self);
				dummies->push_back(dummy_self);
				this->is_dummy = true;
				//this->mapping = new int[1];
				//this->mapping[0] = id.vID;
				this->ncol = 0;
				this->final_us.push_back(curr_u);
				/*
				ncol = query->getCompressedPrefix(curr_u).size() + 2;
				for (int i = 0; i < passed_mappings->size(); i++)
				{
					SIVertex* v = new SIVertex;
					int dummyID = get_dummy_vertex_id();
					v->id = SIKey(dummyID, id.wID);
					v->value().label = id.vID;
					this->add_vertex(v);
					dummys.push_back(dummyID);
					dummys.push_back(id.wID);
				}
				*/
			}
			else
				send_mappings = passed_mappings;
			

			//Continue mapping
			//START_TIMING(t1);
			vector<int> &next_us = query->getChildren(curr_u);
			vector<int> &ps_labs = query->getPseudoLabel(curr_u);
			START_TIMING(t1);
			if (!passed_mappings->empty() || step_num() == 1)
			{
				START_TIMING(t2);
				vector<vector<int>> neighbors_map = vector<vector<int>>(get_num_workers());
				STOP_TIMING(t2, 2, 2);
				for (int next_u_index = 0; next_u_index < next_us.size(); next_u_index++)
				{
					int next_u = next_us[next_u_index];
					if (is_branch)
						include_self = query->getIncludeSelf(curr_u, next_u_index);
					//Construct neighbors_map: 
				  	//Loop through neighbors and select out ones with right labels
				    START_TIMING(t2);
					if (params.filter)
					{ //With filtering
						hash_set<SIKey> &keys = candidate->candidates[curr_u][next_u];
						auto it = keys.begin(); auto iend = keys.end();
						for (; it != iend; ++it)
							neighbors_map[it->wID].push_back(it->vID);
					}
					else
					{ //Without filtering
						int next_label = query->getLabel(next_u);
						for (int i = 0; i < value().degree; ++i)
						{
							KeyLabel &kl = value().nbs_vector[i];
							if (kl.label == next_label)
								neighbors_map[kl.key.wID].push_back(kl.key.vID);
						}
					}
					STOP_TIMING(t2, 0, 2);

					//Update out_message_buffer
					START_TIMING(t2);
					int type;
					if (!is_branch)
						type = MESSAGE_TYPES::OUT_MAPPING;
					else if (include_self)
						type = MESSAGE_TYPES::BMAPPING_W_SELF;
					else
						type = MESSAGE_TYPES::BMAPPING_WO_SELF;

					int nrow = (LEVEL != 0) ? send_mappings->size() : 1;
					SIMessage out_message = SIMessage(type, send_mappings, 
						dummies, next_u, nrow, ncol, id.vID);
					if (query->isLeaf(next_u))
						out_message.is_delete = false;
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
					STOP_TIMING(t2, 1, 0);

					//Clear neighbors_map
					START_TIMING(t2);
					for (int i = 0; i < get_num_workers(); i++)
						neighbors_map[i].clear();
					STOP_TIMING(t2, 1, 1);
				}

				//Leaf query vertex counting
				START_TIMING(t2);
				if (next_us.size() == 0)
				{
					this->manual_active = true;
					this->final_us.push_back(curr_u);
					this->final_results.push_back(send_mappings);
					for (int j = 0; j < passed_mappings->size(); j++)
					{
						cout << "Final mapping: " << endl;
						for (int k = 0; k < ncol; k++)
							cout << (*passed_mappings)[j][k] << " ";
						cout << id.vID << endl;
					}
				}
				STOP_TIMING(t2, 2, 1);
			}
			STOP_TIMING(t1, 2, 0);
		}
		STOP_TIMING(t, 1, 2);
		vote_to_halt();
	}

	void build_branch(MessageContainer &messages, int *mapping, int ncol,
		int curr_u, int offset)
	{ // set up branch + send or expand
		SIQuery* query = (SIQuery*)getQuery();
		cout << "ncol: " << ncol << endl;
		SIBranch *branch = new SIBranch(mapping+offset, id.vID, ncol-offset, curr_u);
		vector<int> &branch_senders = query->getBranchSenders(curr_u);
		int sz = branch_senders.size();
		branch->chd.resize(sz);

		// Phase I: Organize branches
		// 1.1. Receive messages
		// for msg in msgs: arrange msgs according to msg's u
		cout << "Number of messages: " << messages.size() << endl;
		for (SIMessage &msg: messages)
		{
			int u = (msg.branch)->curr_u;
			for (int i = 0; i < sz; i++)
				if (branch_senders[i] == u)
					branch->chd[i].push_back(msg.branch);
		}

		// 1.2. Pseudo children
		// continue_enum: send to dummies
		vector<int> &ps_labs = query->getPseudoLabel(curr_u);
		if (!ps_labs.empty())
		{
			branch->psd_chd.resize(ps_labs.size());
			for (int i = 0; i < value().degree; ++i)
			{
				KeyLabel &kl = value().nbs_vector[i];
				for (int j = 0; j < ps_labs.size(); j++)
				{
					if (ps_labs[j] == kl.label)
					{
						int u = query->getChildByLabel(curr_u, kl.label);
						bool flag = true;
						for (int &b_level : query->getBSameLabPos(u))
						{
							if (kl.key.vID == mapping[b_level])
							{
								flag = false;
								break;
							}
						}
						if (flag)
							branch->psd_chd[j].push_back(kl.key.vID);
					}
				}
			}
		}
		
		if (!branch->isValid())
			return;
		else
			branch->printBranch();

		// Phase II: Split branches (including psd_chd)
		/*
		Single out marked branches, and we obtain several branches
		*/
		

		// Phase III: Send to dummy or expand
		if (offset == 0)
		{
			this->mapping_count += branch->expand();
			cout << "branch->expand(): " << branch->expand() << endl;
			this->manual_active = true;
		}
		else
		{
			int vID = mapping[offset-2];
			int wID = mapping[offset-1];
			SIMessage out_msg = SIMessage(BRANCH_RESULT, branch);
			send_messages(wID, {vID}, out_msg);
		}
	}

	void enumerate(MessageContainer & messages)
	{
		SIQuery* query = (SIQuery*)getQuery();

#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID 
			 << " Manual active: " << manual_active << endl;
#endif

		if (manual_active && !is_dummy)
		{ // leaf vertex
			for (int i = 0 ; i < this->final_us.size(); i++)
			{
				int curr_u = this->final_us[i];
				int branch_num = query->getBranchNumber(curr_u);
				if (branch_num + step_num() != query->max_branch_number + 1)
					continue;

				vector<int*>* final_result = this->final_results[i];
				int dummy_pos = query->getDummyPos(curr_u);
				int ncol = query->getNCOL(curr_u);
				if (dummy_pos < 0)
				{
					for (int j = 0; j < final_result->size(); j++)
					{
						int* mapping = (*final_result)[j];
						build_branch(messages, mapping, ncol, curr_u, 0);
					}
				}
				else
				{
					for (int j = 0; j < final_result->size(); j++)
					{
						int* mapping = (*final_result)[j];
						build_branch(messages, mapping, ncol, curr_u, dummy_pos+2);
					}					
				}
			}
		}
		else if (is_dummy && !messages.empty())
		{ // dummy vertex
			int curr_u = this->final_us[0];
			vector<int*>* final_result = this->final_results[0];
			int dummy_pos = query->getDummyPos(curr_u);
			if (dummy_pos < 0)
				build_branch(messages, this->mapping, this->ncol,
					curr_u, 0);
			else
				build_branch(messages, this->mapping, this->ncol,
					curr_u, dummy_pos+2);			
		}
		vote_to_halt();
	}

//////////////////////////////////////////////////////////

	void check_candidates(hash_set<int> &invalid_set)
	{
		/*
		int u1, u2;
		candidate->fillInvalidSet(invalid_set);
		for (auto set_it = invalid_set.begin();
				set_it != invalid_set.end(); set_it ++)
		{
			u1 = *set_it;
			for (auto it = candidate->candidates[u1].begin();
					it != candidate->candidates[u1].end(); it++)
			{
				u2 = it->first;
				for (SIKey k : it->second)
				{
					SIMessage msg = SIMessage(CANDIDATE, id);
					msg.add_int(u2);
					msg.add_int(u1);
					send_message(k, msg);
#ifdef DEBUG_MODE_MSG
					cout << "[DEBUG] Superstep " << step_num()
							<< "\n\tMessage sent from " << id.vID
							<<	" to " << k.vID << "."
							<< "\n\tType: CANDIDATE. "
							<< "\n\tv_int: " << msg.v_int << endl;
#endif
				}
			}
		}
		*/
	}

	void filter(MessageContainer & messages)
	{
		/*
		SIQuery* query = (SIQuery*)getQuery();
		vector<int> temp_vec;
		int degree = value().nbs_vector.size();

		if (step_num() == 1)
		{ // initialize candidates
			this->candidate = new SICandidate();
			this->manual_active = true;
			query->LDFFilter(value().label, degree, candidate->cand_map);
			SIMessage msg = SIMessage(CANDIDATE, id);
			auto cand_it = candidate->cand_map.begin();
			for (; cand_it != candidate->cand_map.end();
					++ cand_it)
				msg.add_int(cand_it->first);

			if (!candidate->cand_map.empty())
			{
				for (size_t i = 0; i < degree; ++i)
				{
					send_message(value().nbs_vector[i].key, msg);
#ifdef DEBUG_MODE_MSG
					cout << "[DEBUG] Superstep " << step_num()
							<< "\n\tMessage sent from " << id.vID
							<<	" to " << value().nbs_vector[i].key.vID << "."
							<< "\n\tType: CANDIDATE. "
							<< "\n\tv_int: " << msg.v_int << endl;
#endif
				}

			}
			vote_to_halt();
		}
		else if (step_num() == 2)
		{ // initialize candidates
			for (size_t i = 0; i < messages.size(); i++)
			{
				SIMessage & msg = messages[i];
				auto cand_it = candidate->cand_map.begin();
				for (; cand_it != candidate->cand_map.end(); ++cand_it)
				{
					vector<int> &a = cand_it->second;
					set_intersection(a.begin(), a.end(), msg.v_int.begin(),
							msg.v_int.end(), back_inserter(temp_vec));
					for (int u : temp_vec)
					{
						candidate->candidates[cand_it->first][u].insert(msg.key);
					}

					temp_vec.clear();
				}
			}

			hash_set<int> invalid_set; // invalid candidates
			check_candidates(invalid_set); // includes sending message

			for (hash_set<int>::iterator set_it = invalid_set.begin();
					set_it != invalid_set.end(); set_it ++)
			{
				candidate->candidates.erase(*set_it);
			}
			vote_to_halt();
		}
		else if (this->manual_active)
		{ // filter candidates recursively
			for (SIMessage &msg : messages)
				candidate->candidates[msg.v_int[0]][msg.v_int[1]].erase(msg.key);

			hash_set<int> invalid_set; // invalid candidates
			check_candidates(invalid_set); // includes sending message

			for (auto set_it = invalid_set.begin();
					set_it != invalid_set.end(); set_it ++)
				candidate->candidates.erase(*set_it);

			this->manual_active = candidate->hasCandidates();
			vote_to_halt();
		}
		*/
	}

	void enumerate_new(MessageContainer & messages)
	{
		/*
#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID 
			 << " Manual active: " << manual_active << endl;
#endif
		SIQuery* query = (SIQuery*)getQuery();	

		if (step_num() > query->max_branch_number + 1)
		{
			vote_to_halt();
			return;
		}

		// send mappings from leaves for supersteps [1, max_branch_number]
		// if (step_num() > 0 && step_num() <= query->max_branch_number)
		if (step_num() == 1)
		{
			for (int i = 0; i < 3; i++)
				for (int j = 0; j < 3; j++)
					this->timers[i][j] = 0.0;

			if (!this->manual_active)
			{
				vote_to_halt();
				return;
			}

			double t = get_current_time();
			vector<int> vector_u = query->getBucket(query->max_level, value().label);
			if (vector_u.size() == 1)
			{
				if (step_num() == query->max_branch_number + 1)
				{
					this->results_count += this->bucket.size();
				}
				else
				{
					int curr_u = vector_u[0];
					int anc_u = query->getNearestBranchingAncestor(curr_u);
					for (int i = 0; i < this->bucket.size(); i++)
						continue_enum(SIBranch(this->bucket[i]), curr_u, anc_u);
					this->bucket.clear();
				}
			}
			else
			{
				for (int b = 0; b < this->buckets.size(); b++)
				{
					if (step_num() == query->max_branch_number + 1)
					{
						this->results_count += this->buckets[b].size();
					}
					else
					{
						int curr_u = vector_u[b];
						int anc_u = query->getNearestBranchingAncestor(curr_u);
						for (int i = 0; i < this->buckets[b].size(); i++)
							continue_enum(SIBranch(this->buckets[b][i]), curr_u, anc_u);
						
						this->buckets[b].clear();
					}
				}
			}
			this->timers[0][1] += get_current_time() - t;
		}
		else
		{
			double t = get_current_time();
			SIBranch b = SIBranch(id.partial_mapping);
			b.branches.resize(query->getChildren(id.vID).size());
			int i, j;
			vector<int> branch_u;
			for (i = 0; i < messages.size(); i++)
			{
				SIMessage & msg = messages[i];
				// find out where to store the branch
				for (j = 0; j < branch_u.size(); j++)
					if (branch_u[j] == msg.value) break;
				if (j == branch_u.size())
					branch_u.push_back(msg.value);
				b.branches[j].push_back(msg.branch);
			}
			// make sure every child sends you result!
			if (!b.isValid())
			{
				vote_to_halt();
				return;
			}
			if (step_num() == query->max_branch_number + 1)
			{
				double t0 = get_current_time();
				this->bucket = b.expand();
				this->timers[1][1] += get_current_time() - t0;
				this->results_count += this->bucket.size();
			}
			else
			{
				continue_enum(b, id.vID, query->getNearestBranchingAncestor(id.vID));
				this->bucket.clear();
			}
			this->timers[0][1] += get_current_time() - t;
		}
		*/
 		vote_to_halt();
		 
	}

	void enumerate_old(MessageContainer & messages)
	{
		/*
		// return the number of mappings
		SIQuery* query = (SIQuery*)getQuery();

		hash_map<Mapping, mResult> join_results;
		hash_map<Mapping, mResult>::iterator join_it;
		Mapping prefix, tail;
		vector<Mapping> temp_results;
		int num, curr_u, anc_u;
		SIKey to_send;

		// join operations for supersteps [2, max_branch_number + 1]
		if (step_num() > 1 && step_num() < query->max_branch_number + 2)
		{
			// sort out all the messages, classify them according to prefix
			// and curr_u, store in join_results
			for (size_t i = 0; i < messages.size(); i++)
			{
				SIMessage & msg = messages[i];
				curr_u = msg.value;
				anc_u = query->getNearestBranchingAncestor(curr_u);

				Mapping & mapping = msg.mapping;
				for (int j = 0; j <= query->getLevel(anc_u); j++)
					prefix.push_back(mapping[j]);
				for (size_t j = query->getLevel(anc_u) + 1;
						j < mapping.size(); j++)
					tail.push_back(mapping[j]);

				num = query->getDFSNumber(curr_u);
				join_results[prefix][anc_u][num].push_back(tail);
				prefix.clear();
				tail.clear();
			}

			// for each prefix and its tails, perform cross join.
			for (join_it = join_results.begin();
					join_it != join_results.end();
					join_it ++)
			{
				prefix = join_it->first;
				for (mResult::iterator it = join_it->second.begin();
						it != join_it->second.end(); it++)
				{
					anc_u = it->first;
					// make sure every child sends you result!
					if ((int) it->second.size() ==
							query->getChildren(anc_u).size())
					{
						temp_results.push_back(prefix);
						for (map<int, vector<Mapping> >::iterator
								i = it->second.begin();
								i != it->second.end(); i++)
						{
							temp_results = joinVectors(
									temp_results, i->second);
						}
						this->results[anc_u].insert(
								this->results[anc_u].end(),
								temp_results.begin(), temp_results.end());
						temp_results.clear();
					}
				}
			}
		}

		// send messages for supersteps [1, max_branch_number]
		if (step_num() < query->max_branch_number + 1)
		{
			for (Result::iterator it = this->results.begin();
					it != this->results.end(); it++)
			{
				curr_u = it->first;
				num = query->getBranchNumber(curr_u);
				if (num != 0 && num > query->max_branch_number - step_num()
						&& !it->second.empty())
				{ // non-empty to guarantee that query vertex is a leaf
					anc_u = query->getNearestBranchingAncestor(curr_u);
					for (size_t i = 0; i < it->second.size(); i++)
					{
						to_send = it->second[i][query->getLevel(anc_u)];
						send_message(to_send,
							SIMessage(BRANCH_RESULT, it->second[i], curr_u));
#ifdef DEBUG_MODE_MSG
					cout << "[DEBUG] Message sent from " << id.vID << " to "
							<< to_send.vID << ".\n\t"
							<< "Type: BRANCH RESULT. \n\t"
							<< "curr_u: " << curr_u << endl;
#endif
					}
					this->results[curr_u].clear();
				}
			}
		}
		else
			vote_to_halt();
		*/
	}
};

//=============================================================================

typedef vector<vector<double>> AggMat;

class SIAgg : public Aggregator<SIVertex, AggMat, AggMat>
{
	// uniform aggregator for candidates and mappings
	// agg_mat[u1, u1] = candidate(u1);
	// agg_mat[u1, u2] = sum_i(|C'_{u1, vi}(u2)|), u1 < u2
	// agg_mat[0, 0] = # mappings
public:
	AggMat agg_mat;

    virtual void init(int type)
    {
		if (type == FILTER)
		{
			SIQuery* query = (SIQuery*)getQuery();
			int n = query->nodes.size();
			agg_mat.resize(n);
			for (int i = 0; i < n; ++i)
			{
				agg_mat[i].resize(i+1); // lower-triangular matrix
				for (int j = 0; j <= i; ++j)
					agg_mat[i][j] = 0.0;
			}
		}
		else
		{
			agg_mat.resize(3);
			for (int i = 0; i < 3; ++i)
			{
				agg_mat[i].resize(3);
				for (int j = 0; j < 3; ++j)
					agg_mat[i][j] = 0.0;
			}
		}
		
    }

    virtual void stepPartial(SIVertex* v, int type)
    {
    	if (type == FILTER && v->manual_active)
    	{
			int u1, u2;
			auto iend = v->candidate->candidates.end();
    		for (auto it = v->candidate->candidates.begin(); it != iend; ++it)
    		{
    			u1 = it->first;
    			agg_mat[u1][u1] += 1;
				auto jend = it->second.end();
    			for (auto jt = it->second.begin(); jt != jend; ++jt)
    			{
    				u2 = jt->first;
    				if (u1 > u2)
    					agg_mat[u1][u2] += jt->second.size();
    			}
    		}
    	}
    	else if (type == ENUMERATE)
    	{
			if (v->manual_active)
    			agg_mat[0][0] += v->mapping_count;

			for (int i = 0; i < 3; i++)
				for (int j = 0; j < 3; j++)
					if (i != 0 || j != 0)
						agg_mat[i][j] += v->timers[i][j];
    	}
		else
    	{
			for (int i = 0; i < 3; i++)
				for (int j = 0; j < 3; j++)
					agg_mat[i][j] += v->timers[i][j];
    	}
    }
    virtual void stepFinal(AggMat* part)
    {
    	for (int i = 0; i < part->size(); ++i)
			for (int j = 0; j < (*part)[i].size(); ++j)
    			agg_mat[i][j] += (*part)[i][j];
    }
    virtual AggMat* finishPartial()
    {
    	return &agg_mat;
    }
    virtual AggMat* finishFinal()
    {
    	return &agg_mat;
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
		virtual SIVertex* toVertex(char* line, const WorkerParams & params)
		{
			char * pch;
			SIVertex* v = new SIVertex;
			bool default_format = params.input;

			if (default_format)
			{
				pch = strtok(line, " ");
				if (*pch == '#') return NULL;
				int id = atoi(pch);
				v->id = SIKey(id, id % _num_workers);

				pch = strtok(NULL, " ");
				v->value().label = atoi(pch);

				pch = strtok(NULL, " ");
				int num = atoi(pch);
				v->value().degree = num;
				SIKey key;
				for (int k = 0; k < num; ++k)
				{
					pch = strtok(NULL, " ");
					id = atoi(pch);
					key = SIKey(id, id % _num_workers);
					v->value().nbs_vector.push_back(KeyLabel(key, 0));
					//v->value().nbs_set.insert(id);
				}
			}
			else
			{
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
			}
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
			SIQuery* query = (SIQuery*)getQuery();
			if (query->max_level == LEVEL)
				//We are going to use this later, don't free memory.
				return;

			for (SIMessage &msg : delete_messages)
			{
				if (!msg.is_delete)
					continue;
				if (msg.type == OUT_MAPPING)
					vector<int*>().swap(*msg.send_mappings);
				else
					delete[] msg.mappings;
			}
			delete_messages.clear();
		}
};

/*
class CCCombiner_pregel:public Combiner<VertexID>
{
	public:
		virtual void combine(VertexID & old, const VertexID & new_msg)
		{
			if(old>new_msg) old=new_msg;
		}
};
*/

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

	SIQuery query;
	worker.setQuery(&query);

	SIAgg agg;
	worker.setAggregator(&agg);

	// STAGE 1: Load data graph
	MPRINT("Loading data graph...")
	ResetTimer(STAGE_TIMER);
	worker.load_data(params);
	StopTimer(STAGE_TIMER);
	PrintTimer("Loading data graph time", STAGE_TIMER)

	// STAGE 2: Preprocessing
	MPRINT("Preprocessing...")
	ResetTimer(STAGE_TIMER);
	if (params.preprocess)
		worker.run_type(PREPROCESS, params);
	StopTimer(STAGE_TIMER);
	PrintTimer("Preprocessing time", STAGE_TIMER)

	StopTimer(TOTAL_TIMER);
	PrintTimer("In total, offline time", TOTAL_TIMER)

//=============================================================================
	// ONLINE STAGE
	MPRINT("");
	ResetTimer(TOTAL_TIMER);

	// STAGE 1: Load query graph
	MPRINT("Loading query graph...")
	ResetTimer(STAGE_TIMER);
	worker.load_query(params.query_path);
	StopTimer(STAGE_TIMER);
	PrintTimer("Loading query graph time", STAGE_TIMER)

	//=============== The most important timer starts here!!! =================
	StartTimer(COMPUTE_TIMER);

	// STAGE 2: Filtering
	MPRINT("Filtering...")
	ResetTimer(STAGE_TIMER);
	if (params.filter)
	{
		wakeAll();
		worker.run_type(FILTER, params);
	}
	StopTimer(STAGE_TIMER);
	PrintTimer("Filtering time", STAGE_TIMER)

	// STAGE 3: Build query tree
	MPRINT("Building query tree...")
	ResetTimer(STAGE_TIMER);
	worker.build_query_tree(params.order);
	StopTimer(STAGE_TIMER);
	PrintTimer("Building query tree time", STAGE_TIMER)

	// STAGE 4: Subgraph matching
	MPRINT("**Subgraph matching**")
	ResetTimer(STAGE_TIMER);
	wakeAll();
	worker.run_type(MATCH, params);
	StopTimer(STAGE_TIMER);
	PrintTimer("Subgraph matching time", STAGE_TIMER)

	if (_my_rank == MASTER_RANK)
	{
		cout << "[Detailed report]" << endl;
		auto mat = *((AggMat*)global_agg);
		cout << "1. Arrange messages: " <<
			mat[0][0] << " s" << endl;
		cout << "2. Main Computation: " <<
			mat[1][2] << " s" << endl;
		cout << "2.1. Check feasibility: " <<
			mat[0][1] << " s" << endl;
		cout << "2.2. Continue mapping: " <<
			mat[2][0] << " s" << endl;	
		cout << "2.2.0. Initiate neighbor map: " <<
			mat[2][2] << " s" << endl;		
		cout << "2.2.1. Construct neighbor map: " <<
			mat[0][2] << " s" << endl;
		cout << "2.2.2. Update out messages buffer: " <<
			mat[1][0] << " s" << endl;
		cout << "2.2.3. Clear out neighbor map: " <<
			mat[1][1] << " s" << endl;
		cout << "2.2.4. Leaf query vertex counting: " <<
			mat[2][1] << " s" << endl;
	}

	// STAGE 5: Subgraph enumeration
	MPRINT("**Subgraph enumeration**")
	ResetTimer(STAGE_TIMER);
	wakeAll();
	worker.run_type(ENUMERATE, params);
	StopTimer(STAGE_TIMER);
	PrintTimer("Subgraph enumeration time", STAGE_TIMER)

	StopTimer(COMPUTE_TIMER);
	//=============== The most important timer stops here!!! =================

	// STAGE 6: Dumping
	MPRINT("Dumping results...")
	ResetTimer(STAGE_TIMER);
	//worker.dump_graph(params.output_path, params.force_write);
	StopTimer(STAGE_TIMER);
	PrintTimer("Dumping results time", STAGE_TIMER)

	StopTimer(TOTAL_TIMER);
	PrintTimer("In total, online time", TOTAL_TIMER)

	if (_my_rank == MASTER_RANK)
	{
		cout << "================ Final Report ===============" << endl;
		cout << "Mapping count: " <<
				(long) (*((AggMat*)global_agg))[0][0] << endl;
	}

	PrintTimer("COMPUTE Time", COMPUTE_TIMER);

}
