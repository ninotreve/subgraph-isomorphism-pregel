#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "utils/Query.h"
using namespace std;

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
typedef hash_map<int, map<int, vector<Mapping> > > mResult;

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
public:
	SICandidate *candidate;
	double timers[3][3];
	vector<Mapping> bucket;
	vector<vector<Mapping>> buckets;
	int results_count = 0;
	bool manual_active = false;

	void preprocess(MessageContainer & messages)
	{  // use bloom filter to store neighbors' edges.
		size_t sz = value().nbs_vector.size();

		if (step_num() == 1)
		{ // send label and degree to neighbors
			SIMessage msg1 = SIMessage(LABEL_INFOMATION, id, value().label);
			for (size_t i = 0; i < sz; ++i)
			{
				send_message(value().nbs_vector[i].key, msg1);
			}
		}
		else
		{   // receive label and degree from neighbors, set up bloom filter
			// send neighbors to neighbors
			for (size_t i = 0; i < messages.size(); ++i)
			{
				SIMessage & msg = messages[i];
				if (msg.type == LABEL_INFOMATION)
				{
					for (size_t i = 0; i < sz; ++i)
					{
						if (value().nbs_vector[i].key == msg.key)
							value().nbs_vector[i].label = msg.value;
					}
				}
			}
			vote_to_halt();
		}
	}

	bool check_feasibility(vector<SIKey> &mapping, int query_u)
	{ // check vertex uniqueness and backward neighbors 
		double t = get_current_time();
		SIQuery* query = (SIQuery*)getQuery();
		// check vertex uniqueness
		for (int b_level : query->getBSameLabPos(query_u))
		{
			if (this->id == mapping[b_level])
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

	bool continue_mapping(vector<Mapping> &mappings, int &curr_u, bool filter_flag)
	{ // Add current vertex to mapping;
		// Send messages to neighbors with right label.
		// if add_flag, add a dummy vertex for each mapping.
		SIQuery* query = (SIQuery*)getQuery();
		double t = get_current_time();

#ifdef DEBUG_MODE_PARTIAL_RESULT
		cout << "[Result] Current query vertex: " << curr_u << 
		" Partial mapping: " << mappings[0] << endl;
#endif

		vector<int> next_us = query->getChildren(curr_u);
		if (next_us.size() == 0)
		{ // leaf query vertex
			this->manual_active = true;
			return false; 
		}
		else
		{
			if (filter_flag)
			{ // with filtering
				for (int next_u : next_us)
				{
					hash_set<SIKey> &keys = candidate->candidates[curr_u][next_u];
					auto it = keys.begin(); auto iend = keys.end();
					for (; it != iend; ++it)
					{
						SIMessage msg = SIMessage(MAPPING, mappings, curr_u);
						send_message(*it, msg);
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Message sent from " << id.vID << " to "
								<< it->vID << ". \n\t"
								<< "Type: MAPPING. \n\t"
								<< "Mapping: " << msg.mappings[0] << endl;
#endif
					}
				}
			}
			else
			{ // without filtering
				int next_label;
				for (int i = 0; i < value().degree; ++i)
				{
					KeyLabel &kl = value().nbs_vector[i];
					for (int next_u : next_us)
					{		
						next_label = query->getLabel(next_u);							
						if (kl.label == next_label)
						{ // check for label and uniqueness
							SIMessage msg = SIMessage(MAPPING, mappings, curr_u);
							send_message(kl.key, msg);
#ifdef DEBUG_MODE_MSG
							cout << "[DEBUG] Message sent from " << id.vID << " to "
								<< kl.key.vID << ". \n\t"
								<< "Type: MAPPING. \n\t"
								<< "Mapping: " << msg.mappings[0] << endl;
#endif
						}
					}
				}
			}
		}
		this->timers[1][2] += get_current_time() - t;
		return true;
	}

	virtual void compute(MessageContainer &messages, WorkerParams &params)
	{
		SIQuery* query = (SIQuery*)getQuery();

#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID
			 << " Manual active: " << manual_active << endl;
#endif

		if (step_num() == 1)
		{
			for (int i = 0; i < 3; i++)
				for (int j = 0; j < 3; j++)
					this->timers[i][j] = 0.0;

			double t = get_current_time();
			int curr_u = query->root;
			if (!params.filter || this->manual_active)
			{
				this->manual_active = false;
				if (value().label == query->getLabel(curr_u))
				{
					this->timers[0][0] += get_current_time() - t;
					double t0 = get_current_time();
					Mapping mapping = {id};
					bucket.push_back(mapping);
					this->timers[0][1] += get_current_time() - t0;
					//add_flag
					t0 = get_current_time();
					if (params.enumerate && query->isBranch(curr_u))
					{
						SIVertex* v = new SIVertex;
						v->id = SIKey(curr_u, id.wID, mapping);
						this->add_vertex(v);
					}
					this->timers[0][2] += get_current_time() - t0;
					t0 = get_current_time();
					if (continue_mapping(bucket, curr_u, params.filter))
						bucket.clear();
					this->timers[1][0] += get_current_time() - t0;
				}
			}
			this->timers[1][1] += get_current_time() - t;
		}
		else
		{
			if (!this->id.partial_mapping.empty()) // dummy vertex
			{
				for (int i = 0; i < 3; i++)
					for (int j = 0; j < 3; j++)
						this->timers[i][j] = 0.0;
				vote_to_halt();
				return;
			}

			double t = get_current_time();
			//Decide the number of u to be mapped to
			vector<int> vector_u = query->getBucket(step_num()-1, value().label);
			int n_u = vector_u.size();
			if (n_u == 1)
			{
				//Loop through messages
				for (SIMessage &msg : messages)
				{
					for (Mapping &mapping : msg.mappings)
					{
						if (check_feasibility(mapping, vector_u[0]))
						{
							double t1 = get_current_time();
							mapping.push_back(id);
							bucket.push_back(mapping);
							this->timers[0][1] += get_current_time() - t1;
							// add_flag
							t1 = get_current_time();
							if (params.enumerate && query->isBranch(vector_u[0]))
							{
								SIVertex* v = new SIVertex;
								v->id = SIKey(vector_u[0], id.wID, mapping);
								this->add_vertex(v);
							}
							this->timers[0][2] += get_current_time() - t1;
						}
					}
				}
				
				double t2 = get_current_time();
				//Send bucket of mappings to every feasible neighbor
				if (!bucket.empty())
					if (continue_mapping(bucket, vector_u[0], params.filter))
						bucket.clear();
				this->timers[1][0] += get_current_time() - t2;
			}
			else if (n_u > 1)
			{
				buckets.resize(n_u);
				//Loop through messages
				for (SIMessage &msg : messages)
				{
					vector<int> children = query->getChildren(msg.value);
					for (int curr_u : children)
					{
						int bucket_num = query->getBucketNumber(curr_u);
						for (Mapping &mapping : msg.mappings)
						{
							if (check_feasibility(mapping, curr_u))
							{
								double t1 = get_current_time();
								mapping.push_back(id);
								buckets[bucket_num].push_back(mapping);
								this->timers[0][1] += get_current_time() - t1;
								// add_flag
								t1 = get_current_time();
								if (params.enumerate && query->isBranch(curr_u))
								{
									SIVertex* v = new SIVertex;
									v->id = SIKey(curr_u, id.wID, mapping);
									this->add_vertex(v);
								}
								this->timers[0][2] += get_current_time() - t1;
							}
						}
					}
				}

				double t2 = get_current_time();
				//Send bucket of mappings to every feasible neighbor
				for (int i = 0; i < n_u; i++)
				{
					if (!buckets[i].empty())
						if (continue_mapping(buckets[i], vector_u[i], params.filter))
							buckets[i].clear();
				}
				this->timers[1][0] += get_current_time() - t2;
			}
			this->timers[1][1] += get_current_time() - t;
		}
		vote_to_halt();
	}

	void check_candidates(hash_set<int> &invalid_set)
	{
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
	}

	void filter(MessageContainer & messages)
	{
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
	}

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

	void enumerate_new(MessageContainer & messages)
	{
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
    		agg_mat[0][0] += v->results_count;
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
					if (num > 20)
						v->value().nbs_set.insert(key);
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
				}
				size_t sz = v->value().nbs_vector.size();
				v->value().degree = sz;
				if (sz > 20)
				{
					for (size_t i = 0; i < sz; ++i)
						v->value().nbs_set.insert(v->value().nbs_vector[i].key);
				}
			}
			return v;
		}

		virtual void toline(SIVertex* v, BufferedWriter & writer)
		{
			hash_map<int, vector<Mapping> >::iterator it;
			SIQuery* query = (SIQuery*)getQuery();
			for (size_t i = 0; i < v->bucket.size(); i++)
			{
				Mapping & mapping = v->bucket[i];
				sprintf(buf, "# Match\n");
				writer.write(buf);

#ifdef DEBUG_MODE_RESULT
				cout << "[DEBUG] Worker ID: " << get_worker_id() << endl;
				cout << "[DEBUG] Vertex ID: " << v->id.vID << endl;
				cout << "[DEBUG] Result: " << mapping << endl;
#endif
				for (size_t j = 0; j < mapping.size(); j++)
				{
					sprintf(buf, "%d %d\n", 
						query->getID(query->dfs_order[j]), mapping[j].vID);
					writer.write(buf);
				}
			}
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

	if (params.input || params.preprocess)
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
		cout << "From start to end: total time: " <<
			mat[1][1] << " s" << endl;
		cout << "1. To be selected in the first step: " <<
			mat[0][0] << " s" << endl;
		cout << "1. To be selected (check feasibility): " <<
			mat[2][1] << " s" << endl;
		cout << " - Check label uniqueness: " <<
			mat[2][0] << " s" << endl;
		cout << " - Check backward neighbors: " <<
			mat[2][1] - mat[2][0] << " s" << endl;
		cout << "2. Append current vertex id: " <<
			mat[0][1] << " s" << endl;
		cout << "3. Add dummy vertex: " <<
			mat[0][2] << " s" << endl;	
		cout << "4. Continue mapping and clear bucket: " <<
			mat[1][0] << " s" << endl;
		cout << " - Continue mapping: " <<
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
				(*((AggMat*)global_agg))[0][0] << endl;
		cout << "Load time: " << load_time << " s." << endl;
		cout << "Dump time: " << dump_time << " s." << endl;
		cout << "Compute time: " << compute_time << " s." << endl;
		cout << "Offline time: " << offline_time << " s." << endl;
		cout << "Online time: " << online_time << " s." << endl;
	}
}
