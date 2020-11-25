#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "utils/Query.h"
using namespace std;

#define NROW (msg.int2)
#define NCOL (step_num()-1)
#define START_TIMING(T) (T) = get_current_time();
#define STOP_TIMING(T, X, Y) this->timers[(X)][(Y)] += get_current_time() - (T);

/*
#define DEBUG_MODE_ACTIVE 1
#define DEBUG_MODE_MSG 1
#define DEBUG_MODE_PARTIAL_RESULT 1
#define DEBUG_MODE_RESULT 1
*/

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
	bool manual_active = true;

	vector<int*>* final_results;

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

			//convert vector to set
			for (size_t i = 0; i < value().degree; ++i)
				value().nbs_set.insert(value().nbs_vector[i].key.vID);
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
						if (value().nbs_vector[i].key.vID == msg.int1)
							value().nbs_vector[i].label = msg.int2;
					}
				}
			}
			vote_to_halt();
		}
	}

	bool check_feasibility(int *mapping, int query_u)
	{ // check vertex uniqueness and backward neighbors 
		double t = get_current_time();
		SIQuery* query = (SIQuery*)getQuery();
		// check vertex uniqueness
		for (int b_level : query->getBSameLabPos(query_u))
		{
			if (this->id.vID == mapping[b_level])
			{
				this->timers[2][0] += get_current_time() - t;
				return false;
			}
		}

		this->timers[2][0] += get_current_time() - t;
		// check backward neighbors
		for (int b_level : query->getBNeighborsPos(query_u))
		{
			if (! this->value().hasNeighbor(mapping[b_level]))
			{
				this->timers[2][1] += get_current_time() - t;
				return false;
			}
		}
		this->timers[2][1] += get_current_time() - t;
		return true;
	}

	virtual void compute(MessageContainer &messages, WorkerParams &params)
	{
		SIQuery* query = (SIQuery*)getQuery();
		double t, t1;

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

		if (this->id.vID < 0) //dummy vertex
		{
			for (int i = 0; i < 3; i++)
				for (int j = 0; j < 3; j++)
					this->timers[i][j] = 0.0;
			vote_to_halt();
			return;
		}

		// arrange messages
		START_TIMING(t);
		vector<int> vector_u = query->getBucket(NCOL, value().label);
		int n_u = vector_u.size();
		vector<vector<int>> messages_classifier = vector<vector<int>>(n_u);
		if (step_num() == 1 && this->manual_active)
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
			for (int i = 0; i < messages.size(); i++)
			{
				int bucket_num = query->getBucketNumber(messages[i].int1);
				messages_classifier[bucket_num].push_back(i);
			}
		}
		STOP_TIMING(t, 0, 0);

		// main computation
		START_TIMING(t);
		for (int bucket_num = 0; bucket_num < n_u; bucket_num ++)
		{
			int curr_u = vector_u[bucket_num];
			//Loop through messages and check feasibilities
			START_TIMING(t1);
			vector<int*>* passed_mappings = new vector<int*>();
			for (int msgi : messages_classifier[bucket_num])
			{
				SIMessage &msg = messages[msgi];
				for (int i = 0; i < NROW; i++)
				{
					if (check_feasibility(msg.mappings + i*NCOL, curr_u))
					{
						passed_mappings->push_back(msg.mappings + i*NCOL);
						/* add_flag, need to be modified.
						if (params.enumerate && query->isBranch(vector_u[0]))
						{
							SIVertex* v = new SIVertex;
							v->id = SIKey(vector_u[0], id.wID, mapping);
							this->add_vertex(v);
						}
						*/
					}
				}
			}
			STOP_TIMING(t1, 0, 1);

			//Continue mapping
			vector<int> &next_us = query->getChildren(curr_u);
			if (!passed_mappings->empty() || step_num() == 1)
			{
				vector<vector<int>> neighbors_map = vector<vector<int>>(get_num_workers());
				for (int next_u : next_us)
				{
					//Construct neighbors_map: 
				  	//Loop through neighbors and select out ones with right labels
				    START_TIMING(t1);
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
					STOP_TIMING(t1, 0, 2);

					//Update out_message_buffer
					START_TIMING(t1);
					for (int wID = 0; wID < get_num_workers(); wID++)
					{
						if (neighbors_map[wID].empty())
							continue;

						if (wID == get_worker_id())
						{ //Copy the message
							int nrow = (NCOL != 0) ? passed_mappings->size() : 1;
							int *mappings = new int[nrow * (NCOL+1)];
							for (int i = 0, j; i < nrow; i++)
							{
								for (j = 0; j < NCOL; j++)
									mappings[i*(NCOL+1)+j] = (*passed_mappings)[i][j];
								mappings[i*(NCOL+1)+j] = id.vID;
							}
							send_messages(wID, neighbors_map[wID],
								SIMessage(IN_MAPPING, next_u, nrow, mappings));
						}
						else
						{
							send_messages(wID, neighbors_map[wID],
								SIMessage(OUT_MAPPING, id.vID, next_u, 
									passed_mappings));
						}
					}
					STOP_TIMING(t1, 1, 0);

					//Clear neighbors_map
					START_TIMING(t1);
					for (int i = 0; i < get_num_workers(); i++)
						neighbors_map[i].clear();
					STOP_TIMING(t1, 1, 1);
				}
				if (next_us.size() == 0)
				{ //Leaf query vertex
					this->manual_active = true;
					this->final_results = passed_mappings;
				}
			}
		}
		STOP_TIMING(t, 1, 2);
		vote_to_halt();
	}

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
/*
	void continue_enum(SIBranch b, int curr_u, int anc_u)
	{
		
		double t = get_current_time();
		SIQuery* query = (SIQuery*)getQuery();	
		Mapping &m = b.p;
		Mapping m1, m2;
		int j;
		SIKey to_key = m[query->getLevel(anc_u)];
		for (j = 0; j <= query->getLevel(anc_u); j++)
			m1.push_back(m[j]);
		for (; j < (int) m.size(); j++)
			m2.push_back(m[j]);
		b.p = m2;
		send_message(SIKey(anc_u, to_key.wID, m1), SIMessage(BRANCH, b, curr_u));
#ifdef DEBUG_MODE_MSG
		cout << "[DEBUG] Superstep " << step_num()
		 	<< "\n\tMessage sent from (leaf) " << id.vID
			<<	" to <" << to_key.vID << ", " << m1 << ">."
			<< "\n\tType: BRANCH. "
			<< "\n\tMapping: " << m2
			<< ", curr_u: " << curr_u << endl;
#endif
		this->timers[1][0] += get_current_time() - t;
		
	}	
*/
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
    			agg_mat[0][0] += v->final_results->size();

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
			if (!v->manual_active)
				return;

			SIQuery* query = (SIQuery*)getQuery();
			int ncol = query->num;
			for (size_t i = 0, j; i < v->final_results->size(); i++)
			{
				int* mapping = (*v->final_results)[i];
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
		}

		virtual void clear_messages(vector<SIMessage> &delete_messages)
		{
			SIQuery* query = (SIQuery*)getQuery();
			if (query->max_level == NCOL)
				//We are going to use this later, don't free memory.
				return;

			for (SIMessage &msg : delete_messages)
			{
				if (msg.type == OUT_MAPPING)
					vector<int*>().swap(*msg.passed_mappings);
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

	init_timers();
	start_timer(TOTAL_TIMER);

	SIQuery query;
	worker.setQuery(&query);

	SIAgg agg;
	worker.setAggregator(&agg);

	double time, load_time = 0.0, compute_time = 0.0, dump_time = 0.0,
			offline_time = 0.0, online_time = 0.0;

	time = worker.load_data(params);
	load_time += time;

	if (params.preprocess)
		worker.run_type(PREPROCESS, params);

	stop_timer(TOTAL_TIMER);
	offline_time = get_timer(TOTAL_TIMER);
	reset_timer(TOTAL_TIMER);

	time = worker.load_query(params.query_path);
	load_time += time;

	start_timer(COMPUTE_TIMER);
	if (params.filter)
	{
		wakeAll();
		worker.run_type(FILTER, params);
	}

	time = worker.build_query_tree(params.order);

	wakeAll();
	worker.run_type(MATCH, params);

	if (_my_rank == MASTER_RANK)
	{
		auto mat = *((AggMat*)global_agg);
		cout << "1. Arrange messages: " <<
			mat[0][0] << " s" << endl;
		cout << "2. Check feasibility: " <<
			mat[0][1] << " s" << endl;
		cout << "\t - Check vertex uniqueness: " <<
			mat[2][0] << " s" << endl;
		cout << "\t - Check non-tree edge: " <<
			mat[2][1] << " s" << endl;
		cout << "3. Construct neighbor map: " <<
			mat[0][2] << " s" << endl;
		cout << "4. Update out messages buffer: " <<
			mat[1][0] << " s" << endl;
		cout << "5. Clear out neighbor map: " <<
			mat[1][1] << " s" << endl;
		cout << "Main Computation: " <<
			mat[1][2] << " s" << endl;
	}

	wakeAll();
	worker.run_type(ENUMERATE, params);
	stop_timer(COMPUTE_TIMER);
	compute_time = get_timer(COMPUTE_TIMER);

	if (_my_rank == MASTER_RANK)
	{
		auto mat = *((AggMat*)global_agg);
		cout << "From start to end: total time: " <<
			mat[0][1] << " s" << endl;
		cout << " - continue enumerating: " <<
			mat[1][0] << " s" << endl;
		cout << " - expand time: " <<
			mat[1][1] << " s" << endl;
		cout << "Sync Time: " <<
			mat[2][2] << " s" << endl;
	}

	time = worker.dump_graph(params.output_path, params.force_write);
	dump_time += time;

	stop_timer(TOTAL_TIMER);
	online_time = get_timer(TOTAL_TIMER);
	reset_timer(TOTAL_TIMER);

	if (_my_rank == MASTER_RANK)
	{
		cout << "================ Final Report ===============" << endl;
		cout << "Mapping count: " <<
				(long) (*((AggMat*)global_agg))[0][0] << endl;
		cout << "Load time: " << load_time << " s." << endl;
		cout << "Dump time: " << dump_time << " s." << endl;
		cout << "Compute time: " << compute_time << " s." << endl;
		cout << "Offline time: " << offline_time << " s." << endl;
		cout << "Online time: " << online_time << " s." << endl;
	}
}
