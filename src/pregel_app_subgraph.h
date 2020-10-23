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
#include "SItypes/bloom_filter.h"

//===============================================================

typedef hash_map<int, hash_set<SIKey> > Candidate;
// the first int for anc_u, the second int for curr_u's branch_num.
typedef hash_map<int, map<int, vector<Mapping> > > mResult;

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
public:
	// used in the preprocessing step:
	bloom_filter bfilter;

	// used in the filtering step:
	// typedef hash_map<int, hash_set<SIKey> > Candidate;
	// candidates[curr_u][next_u] = vector<SIKey>
	hash_map<int, Candidate> candidates;
	// curr_u: vector<next_u>
	hash_map<int, vector<int> > cand_map;

	vector<Mapping> bucket;
	vector<vector<Mapping>> buckets;
	int results_count = 0;
	bool manual_active = false;

	void preprocess(MessageContainer & messages, bool bloom_filter)
	{  // use bloom filter to store neighbors' edges.
		size_t sz = value().nbs_vector.size();

		if (step_num() == 1)
		{ // send label and degree to neighbors
			SIMessage msg1 = SIMessage(LABEL_INFOMATION, id, value().label);
			SIMessage msg2 = SIMessage(DEGREE, id, value().degree);
			for (size_t i = 0; i < sz; ++i)
			{
				send_message(value().nbs_vector[i].key, msg1);
				if (bloom_filter)
				{
					send_message(value().nbs_vector[i].key, msg2);
				}
			}
		}
		
		if (step_num() == 2)
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
				else // DEGREE
				{
					bfilter.add_projected_element_count(msg.value);
					bfilter.init(0.01, 0xA5A5A5A5);
				}
			}
			if (!bloom_filter) vote_to_halt();
		}

		if (step_num() >= 2 && bloom_filter)
		{ // fill in bloom filter and send one edge to neighbors
			for (size_t i = 0; i < messages.size(); ++i)
			{
				SIMessage & msg = messages[i];
				if (msg.type == NEIGHBOR_PAIR)
					bfilter.insert(msg.p_int);
			}
			int index = step_num() - 2; // start from 0
			if (index < sz)
			{
				int me = this->id.vID;
				int nb = value().nbs_vector[index].key.vID;
				for (size_t i = 0; i < sz; ++i)
				{
					if (i != index)
					{
						SIMessage msg = SIMessage(NEIGHBOR_PAIR, 
							make_pair(me, nb));
						send_message(value().nbs_vector[i].key, msg);
					}
				}
			}
			else vote_to_halt();
		}
	}

	bool check_feasibility(vector<SIKey> &mapping, int query_u)
	{ // check vertex uniqueness and backward neighbors 
		SIQuery* query = (SIQuery*)getQuery();
		// check vertex uniqueness
		for (int b_level : query->getBSameLabPos(query_u))
			if (this->id == mapping[b_level])
				return false;

		// check backward neighbors
		for (int b_level : query->getBNeighborsPos(query_u))
			if (! this->value().hasNeighbor(mapping[b_level]))
				return false;
		
		return true;
	}

	bool continue_mapping(vector<Mapping> &mappings, int &curr_u, bool filter_flag)
	{ // Add current vertex to mapping;
		// Send messages to neighbors with right label.
		// if add_flag, add a dummy vertex for each mapping.
		SIQuery* query = (SIQuery*)getQuery();

#ifdef DEBUG_MODE_PARTIAL_RESULT
		cout << "[Result] Current query vertex: " << curr_u << 
		" Partial mapping: " << mappings[0] << endl;
#endif

		vector<int> next_us = query->getChildren(curr_u);
		if (next_us.size() == 0)
		{ // leaf query vertex
			this->results_count += mappings.size();
			this->manual_active = true;
			return false; 
		}
		else
		{
			if (filter_flag)
			{ // with filtering
				for (int next_u : next_us)
				{
					hash_set<SIKey> &keys = candidates[curr_u][next_u];
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
			return true;
		}
	}

	virtual void compute(MessageContainer &messages, WorkerParams &params)
	{
		SIQuery* query = (SIQuery*)getQuery();
		if (!this->id.partial_mapping.empty())
		{
			vote_to_halt();
			return;
		}

#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID << endl;
#endif

		if (step_num() == 1)
		{
			int curr_u = query->root;
			if (value().label == query->getLabel(curr_u))
			{
				Mapping mapping = {id};
				bucket.push_back(mapping);
				//add_flag
				if (params.enumerate && query->isBranch(curr_u))
				{
					SIVertex* v = new SIVertex;
					v->id = SIKey(curr_u, id.wID, mapping);
					this->add_vertex(v);
				}
				if (continue_mapping(bucket, curr_u, params.filter))
					bucket.clear();
			}
		}
		else
		{
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
							mapping.push_back(id);
							bucket.push_back(mapping);
							// add_flag
							if (params.enumerate && query->isBranch(vector_u[0]))
							{
								SIVertex* v = new SIVertex;
								v->id = SIKey(vector_u[0], id.wID, mapping);
								this->add_vertex(v);
							}
						}
					}
				}
					
				//Send bucket of mappings to every feasible neighbor
				if (!bucket.empty())
					if (continue_mapping(bucket, vector_u[0], params.filter))
						bucket.clear();
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
								mapping.push_back(id);
								buckets[bucket_num].push_back(mapping);
								// add_flag
								if (params.enumerate && query->isBranch(curr_u))
								{
									SIVertex* v = new SIVertex;
									v->id = SIKey(curr_u, id.wID, mapping);
									this->add_vertex(v);
								}
							}
						}
					}
				}

				//Send bucket of mappings to every feasible neighbor
				for (int i = 0; i < n_u; i++)
				{
					if (!buckets[i].empty())
						if (continue_mapping(buckets[i], vector_u[i], params.filter))
							buckets[i].clear();
				}
			}
		}
		vote_to_halt();
	}

	void check_candidates(hash_set<int> &invalid_set)
	{
		int u1, u2;
		for (auto cand_it = cand_map.begin(); cand_it != cand_map.end();
				cand_it ++)
		{
			u1 = cand_it->first;
			for (int u : cand_it->second)
			{
				if (candidates[u1][u].empty())
				{
					invalid_set.insert(u1);
					break;
				}
			}
		}

		for (hash_set<int>::iterator set_it = invalid_set.begin();
				set_it != invalid_set.end(); set_it ++)
		{
			u1 = *set_it;
			for (Candidate::iterator it = candidates[u1].begin();
					it != candidates[u1].end(); it++)
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
			query->LDFFilter(value().label, degree, cand_map);
			SIMessage msg = SIMessage(CANDIDATE, id);
			for (auto cand_it = cand_map.begin(); cand_it != cand_map.end();
					++ cand_it)
				msg.add_int(cand_it->first);

			if (!cand_map.empty())
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
				for (auto cand_it = cand_map.begin(); cand_it != cand_map.end();
						++cand_it)
				{
					vector<int> &a = cand_it->second;
					set_intersection(a.begin(), a.end(), msg.v_int.begin(),
							msg.v_int.end(), back_inserter(temp_vec));
					for (int u : temp_vec)
						candidates[cand_it->first][u].insert(msg.key);

					temp_vec.clear();
				}
			}

			hash_set<int> invalid_set; // invalid candidates
			check_candidates(invalid_set); // includes sending message

			for (hash_set<int>::iterator set_it = invalid_set.begin();
					set_it != invalid_set.end(); set_it ++)
			{
				candidates.erase(*set_it);
			}
			vote_to_halt();
		}
		else
		{ // filter candidates recursively
			for (SIMessage &msg : messages)
				candidates[msg.v_int[0]][msg.v_int[1]].erase(msg.key);

			hash_set<int> invalid_set; // invalid candidates
			check_candidates(invalid_set); // includes sending message

			for (auto set_it = invalid_set.begin();
					set_it != invalid_set.end(); set_it ++)
				candidates.erase(*set_it);

			vote_to_halt();
		}
	}

	void continue_enum(SIBranch b, int curr_u, int anc_u)
	{
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
	}

	void enumerate_new(MessageContainer & messages)
	{
#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID << endl;
#endif
		SIQuery* query = (SIQuery*)getQuery();	
		if (step_num() == 1 && !this->manual_active
			|| step_num() > query->max_branch_number)
		{
			vote_to_halt();
			return;
		}	

		// send mappings from leaves for supersteps [1, max_branch_number]
		// if (step_num() > 0 && step_num() <= query->max_branch_number)
		if (step_num() == 1)
		{
			vector<int> vector_u = query->getBucket(query->max_level, value().label);
			if (vector_u.size() == 1)
			{
				int curr_u = vector_u[0];
				int anc_u = query->getNearestBranchingAncestor(curr_u);
				for (int i = 0; i < this->bucket.size(); i++)
					continue_enum(SIBranch(this->bucket[i]), curr_u, anc_u);
			}
			else
			{
				for (int b = 0; b < this->buckets.size(); b++)
				{
					int curr_u = vector_u[b];
					int anc_u = query->getNearestBranchingAncestor(curr_u);
					for (int i = 0; i < this->buckets[b].size(); i++)
						continue_enum(SIBranch(this->buckets[b][i]), curr_u, anc_u);
				}
			}
		}
			/*
			for (auto it = this->results.begin(); it != iend; ++it)
			{
				curr_u = it->first;
				num = query->getBranchNumber(curr_u);
				if (num != 0 && num > query->max_branch_number - step_num()
						&& !it->second.empty())
				{ // non-empty to guarantee that query vertex is a leaf
					anc_u = query->getNearestBranchingAncestor(curr_u);
					for (size_t i = 0; i < it->second.size(); i++)
					{
						Mapping m1, m2;
						Mapping & m = it->second[i];
						to_key = m[query->getLevel(anc_u)];
						for (j = 0; j <= query->getLevel(anc_u); j++)
							m1.push_back(m[j]);
						for (; j < (int) m.size(); j++)
							m2.push_back(m[j]);
						SIBranch b = SIBranch(m2);
						send_message(SIKey(anc_u, to_key.wID, m1),
							SIMessage(BRANCH, b, curr_u));
#ifdef DEBUG_MODE_MSG
					cout << "[DEBUG] Superstep " << step_num()
							<< "\n\tMessage sent from (leaf) " << id.vID
							<<	" to <" << to_key.vID << ", " << m1 << ">."
							<< "\n\tType: BRANCH. "
							<< "\n\tMapping: " << m2
							<< ", curr_u: " << curr_u << endl;
#endif
					}
					delete_set.insert(curr_u);
				} // end of if
			} // end of for loop

			for (hash_set<int>::iterator it = delete_set.begin();
					it != delete_set.end(); ++it)
			{
				this->results.erase(*it);
			}
			this->results_count = 0;
		}
		*/
		else
		{
			vector<int> children_u = query->getChildren(id.vID);
			SIBranch b = SIBranch(id.partial_mapping);
			b.branches.resize(children_u.size());
			int i = 0, j = 0;
			for (; i < messages.size(); i++)
			{
				SIMessage & msg = messages[i];
				// find out where to store the branch
				for (; j < children_u.size(); j++)
					if (children_u[j] == msg.value) break;
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
				this->bucket = b.expand();
				this->results_count += this->bucket.size();
			}
			else
			{
				continue_enum(b, id.vID, query->getNearestBranchingAncestor(id.vID));
			}
		}
/*
		// receive msg and join for supersteps [2, max_branch_number + 1]
		// send msg to ancestor for supersteps [2, max_branch_number]
		if (step_num() > 1 && step_num() <= query->max_branch_number + 1)
		{
			// hash_map<curr_u, map<chd_u's DFS number, vector<SIBranch> > >
			hash_map<int, map<int, vector<SIBranch> > > u_children;
			hash_map<int, map<int, vector<SIBranch> > >::iterator it1;
			map<int, vector<SIBranch> >::iterator it2;
			for (size_t i = 0; i < messages.size(); i++)
			{
				SIMessage & msg = messages[i];
				curr_u = query->getNearestBranchingAncestor(msg.value);
				num = query->getDFSNumber(msg.value);
				u_children[curr_u][num].push_back(msg.branch);
			}

			for (it1 = u_children.begin(); it1 != u_children.end();
					it1++)
			{
				Mapping m1, m2;
				Mapping &m = id.partial_mapping;
				curr_u = it1->first;

				// make sure every child sends you result!
				if ((int) it1->second.size() !=
						query->getChildren(curr_u).size())
					continue;

				if (step_num() == query->max_branch_number + 1)
				{
					SIBranch b = SIBranch(m);
					for (it2 = it1->second.begin();
							it2 != it1->second.end(); it2++)
						b.addBranch(it2->second);
					this->results[curr_u] = b.expand();
					this->results_count += this->results[curr_u].size();
				}
				else
				{
					anc_u = query->getNearestBranchingAncestor(curr_u);
					to_key = m[query->getLevel(anc_u)];
					for (j = 0; j <= query->getLevel(anc_u); j++)
						m1.push_back(m[j]);
					for (; j < (int) m.size(); j++)
						m2.push_back(m[j]);
					SIBranch b = SIBranch(m2);
					for (it2 = it1->second.begin();
							it2 != it1->second.end(); it2++)
						b.addBranch(it2->second);
					send_message(SIKey(anc_u, to_key.wID, m1),
							SIMessage(BRANCH, b, curr_u));
#ifdef DEBUG_MODE_MSG
				cout << "[DEBUG] Superstep " << step_num()
						<< "\n\tMessage sent from (branching) " << id.vID
						<<	" to <" << to_key.vID << ", " << m1 << ">. \n\t"
						<< "Type: BRANCH. \n\t"
						<< "Mapping: " << m2 << endl;
#endif
				}
			}
		}

		if (step_num() >= query->max_branch_number)
		{
			vote_to_halt();
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

typedef vector<vector<int>> AggMat;

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
					agg_mat[i][j] = 0;
			}
		}
		else if (type == ENUMERATE)
		{
			agg_mat.resize(1);
			agg_mat[0].resize(1);
			agg_mat[0][0] = 0;
		}
    }

    virtual void stepPartial(SIVertex* v, int type)
    {
    	if (type == FILTER)
    	{
        	int u1, u2;
			auto iend = v->candidates.end();
    		for (auto it = v->candidates.begin(); it != iend; ++it)
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
			offline_time = 0.0, online_time = 0.0, sync_time;

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
	sync_time = worker.run_type(MATCH, params);

	wakeAll();
	worker.run_type(ENUMERATE, params);
	stop_timer(COMPUTE_TIMER);
	compute_time = get_timer(COMPUTE_TIMER);

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

	cout << "============ Load Balance Report ==============" << endl;
	cout << "Rank: " << _my_rank << " Sync_time: " << sync_time << endl;
}
