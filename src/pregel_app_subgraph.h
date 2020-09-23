#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "utils/Query.h"
using namespace std;

//#define DEBUG_MODE_ACTIVE 1
//#define DEBUG_MODE_MSG 1
//#define DEBUG_MODE_PARTIAL_RESULT 1
//#define DEBUG_MODE_RESULT 1

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

//===============================================================

typedef hash_map<int, hash_set<SIKey> > Candidate;
typedef hash_map<int, vector<Mapping> > Result;
// the first int for anc_u, the second int for curr_u's branch_num.
typedef hash_map<int, map<int, vector<Mapping> > > mResult;

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
	public:
		// used in the filtering step:
		// typedef hash_map<int, hash_set<SIKey> > Candidate;
		// candidates[curr_u][next_u] = vector<SIKey>
		hash_map<int, Candidate> candidates;
		// curr_u: vector<next_u>
		hash_map<int, vector<int> > cand_map;

		// used in the final step:
		// key = query_vertex, value = vector of path mapping
		int root_query_vertex;
		// typedef hash_map<int, vector<Mapping> > Result;
		Result results;

		void continue_mapping(vector<SIKey> &mapping, int &curr_u,
				bool add_flag, bool filter_flag)
		{ // Add current vertex to mapping;
		  // Send messages to neighbors with label of next_u(next query vertex)
		  // if add_flag, add a dummy vertex for each mapping.
			hash_map<SIKey, int>::iterator it;
			SIQuery* query = (SIQuery*)getQuery();

			mapping.push_back(id);
#ifdef DEBUG_MODE_PARTIAL_RESULT
			cout << "[Result] Current query vertex: " << curr_u << " Partial mapping: " << mapping << endl;
#endif
			if (add_flag)
			{
				SIVertex* v = new SIVertex;
				v->id = SIKey(id.vID, id.wID, mapping);
				this->add_vertex(v);
			}

			int children_num = query->getChildrenNumber(curr_u);
			if (children_num == 0)
			{ // leaf query vertex
				this->results[curr_u].push_back(mapping);
			}
			else
			{
				int next_u;
				for (int i = 0; i < children_num; i++)
				{ // for every child (next_u) in query graph
					next_u = query->getChildID(curr_u, i);
					SIMessage msg = SIMessage(MAPPING, mapping, next_u);
					if (filter_flag)
					{ // with filtering
						hash_set<SIKey> &keys = candidates[curr_u][next_u];
						for (auto it = keys.begin(); it != keys.end(); it++)
						{
							if (notContains(mapping, *it))
							{
								send_message(*it, msg);
#ifdef DEBUG_MODE_MSG
							cout << "[DEBUG] Message sent from " << id.vID << " to "
								 << it->vID << ". \n\t"
								 << "Type: MAPPING. \n\t"
								 << "Mapping: " << msg.mapping << endl;
#endif
							}
						}
					}
					else
					{ // without filtering
						hash_map<SIKey, int> & nbs = value().neighbors;
						for (it = nbs.begin(); it != nbs.end(); it++)
						{
							if (it->second == query->getLabel(next_u) &&
								notContains(mapping, it->first))
							{ // check for label and uniqueness
								send_message(it->first, msg);
#ifdef DEBUG_MODE_MSG
							cout << "[DEBUG] Message sent from " << id.vID << " to "
								 << it->first.vID << ". \n\t"
								 << "Type: MAPPING. \n\t"
								 << "Mapping: " << msg.mapping << endl;
#endif
							}
						}
					}
				}
			}
		}

		virtual void preprocess(MessageContainer & messages)
		{
			if (step_num() == 1)
			{ // send label info to neighbors
				SIMessage msg = SIMessage(LABEL_INFOMATION, id, value().label);
				size_t sz = value().nbs_vector.size();
				for (size_t i < 0; i < sz; ++i)
					send_message(value().nbs_vector[i], msg);
				vote_to_halt();
			}
			else // if (step_num() == 2)
			{   // receive label info from neighbors
				for (size_t i = 0; i < messages.size(); ++i)
				{
					SIMessage & msg = messages[i];
					size_t sz = value().nbs_vector.size();
					for (size_t i < 0; i < sz; ++i)
					{
						if (value().nbs_vector[i].key == msg.key)
							value().nbs_vector[i].label = msg.value;
					}
				}
				vote_to_halt();
			}
		}

		virtual void compute(MessageContainer &messages, WorkerParams &params)
		{
			hash_map<SIKey, int> & nbs = value().neighbors;
			SIQuery* query = (SIQuery*)getQuery();
			bool add_flag; // add a dummy vertex for each mapping

#ifdef DEBUG_MODE_ACTIVE
				cout << "[DEBUG] STEP NUMBER " << step_num()
					 << " ACTIVE Vertex ID " << id.vID << endl;
#endif

			if (step_num() == 1)
			{
				int root_u = query->root;
				add_flag = params.enumerate && query->isBranch(root_u);
				if (params.filter)
				{ // with filtering
					int root_label = query->getLabel(root_u);
					if (value().label == root_label) //!candidates[root_u].empty()
					{
						vector<SIKey> mapping;
						continue_mapping(mapping, root_u, add_flag,
								params.filter);
					}
				}
				else
				{ // without filtering, map with vertices with same label
					int root_label = query->getLabel(root_u);
					if (value().label == root_label)
					{
						vector<SIKey> mapping;
						continue_mapping(mapping, root_u, add_flag,
								params.filter);
					}
				}
				vote_to_halt();
			}
			else
			{   // check if backward neighbors in neighbors
				int curr_level, nb_level;
				bool flag;
				vector<int> b_nbs;
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					b_nbs = query->getBNeighbors(msg.value);
					curr_level = query->getLevel(msg.value);
					flag = true;
					for (int b_nb : b_nbs)
					{
						nb_level = query->getLevel(b_nb);
						if ((nb_level != curr_level - 1) &&
							(nbs.find(msg.mapping[nb_level]) == nbs.end()))
						{
							flag = false;
							break;
						}
					}
					if (flag)
					{
						add_flag = params.enumerate &&
								query->isBranch(msg.value);
						continue_mapping(msg.mapping, msg.value,
								add_flag, params.filter);
					}
				}
				vote_to_halt();
			}
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
			hash_map<SIKey, int> & nbs = value().neighbors;
			hash_map<SIKey, int>::iterator it;
			vector<int> temp_vec;

			if (step_num() == 1)
			{ // initialize candidates
				query->LDFFilter(value().label, nbs.size(), cand_map);
				SIMessage msg = SIMessage(CANDIDATE, id);
				for (hash_map<int, vector<int> >::iterator cand_it
						= cand_map.begin(); cand_it != cand_map.end();
						cand_it ++)
					msg.add_int(cand_it->first);

				if (!cand_map.empty())
				{
					for (it = nbs.begin(); it != nbs.end(); it++)
					{
						send_message(it->first, msg);
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Superstep " << step_num()
							 << "\n\tMessage sent from " << id.vID
							 <<	" to " << it->first.vID << "."
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
					for (hash_map<int, vector<int> >::iterator cand_it
							= cand_map.begin(); cand_it != cand_map.end();
							cand_it ++)
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

		void enumerate_new(MessageContainer & messages)
		{
			// return the number of mappings
			SIQuery* query = (SIQuery*)getQuery();
			hash_map<int, vector<Mapping> >::iterator it;
			int num, curr_u, anc_u, j;
			SIKey to_key;
			// hash_map<curr_u, map<chd_u's DFS number, vector<SIBranch> > >
			hash_map<int, map<int, vector<SIBranch> > > u_children;
			hash_map<int, map<int, vector<SIBranch> > >::iterator it1;
			map<int, vector<SIBranch> >::iterator it2;

			hash_set<int> delete_set; // hash map keys to be deleted

			// send mappings from leaves for supersteps [1, max_branch_number]
			if (step_num() > 0 && step_num() <= query->max_branch_number)
			{
				for (it = this->results.begin(); it != this->results.end();
						it++)
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
							send_message(SIKey(to_key.vID, to_key.wID, m1),
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
			}

			// receive msg and join for supersteps [2, max_branch_number + 1]
			// send msg to ancestor for supersteps [2, max_branch_number]
			if (step_num() > 1 && step_num() <= query->max_branch_number + 1)
			{
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
							query->getChildrenNumber(curr_u))
						continue;

					if (step_num() == query->max_branch_number + 1)
					{
						SIBranch b = SIBranch(m);
						for (it2 = it1->second.begin();
								it2 != it1->second.end(); it2++)
							b.addBranch(it2->second);
						this->results[curr_u] = b.expand();
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
						send_message(SIKey(to_key.vID, to_key.wID, m1),
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

			if (this->results.empty()
					|| step_num() >= query->max_branch_number)
			{
				vote_to_halt();
			}

		}

		void enumerate_old(MessageContainer & messages)
		{
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
								query->getChildrenNumber(anc_u))
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

		}
};

//=============================================================================

typedef hash_map<pair<int, int>, int> AggMap;

class SIAgg : public Aggregator<SIVertex, AggMap, AggMap>
{
	// uniform aggregator for candidates and mappings
	// agg_map[u1, u1] = candidate(u1);
	// agg_map[u1, u2] = sum_i(|C'_{u1, vi}(u2)|), u1 < u2
	// agg_map[0, 0] = # mappings
public:
	AggMap agg_map;

    virtual void init()
    {
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
    			agg_map[make_pair(u1, u1)] += 1;
				auto jend = it->second.end();
    			for (auto jt = it->second.begin(); jt != jend; ++jt)
    			{
    				u2 = jt->first;
    				if (u1 < u2)
    					agg_map[make_pair(u1, u2)] += jt->second.size();
    			}
    		}
    	}
    	else if (type == ENUMERATE)
    	{
    		for (auto it = v->results.begin(); it != v->results.end(); it++)
    			agg_map[make_pair(0, 0)] += it->second.size();
    	}
    }
    virtual void stepFinal(AggMap* part)
    {
    	for (auto it = part->begin(); it != part->end(); it++)
    		agg_map[it->first] += it->second;
    }
    virtual AggMap* finishPartial()
    {
    	return &agg_map;
    }
    virtual AggMap* finishFinal()
    {
    	return &agg_map;
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
		virtual SIVertex* toVertex(char* line, bool default_format)
		{
			char * pch;
			SIVertex* v = new SIVertex;

			if (default_format)
			{
				pch = strtok(line, " ");
				int id = atoi(pch);
				v->id = SIKey(id, id % _num_workers);

				pch = strtok(NULL, " ");
				v->value().label = atoi(pch);

				pch = strtok(NULL, " ");
				int num = atoi(pch);
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
			for (it = v->results.begin(); it != v->results.end(); it++)
			{
				vector<Mapping> & results = it->second;
				for (size_t i = 0; i < results.size(); i++)
				{
					Mapping & mapping = results[i];
					sprintf(buf, "# Match\n");
					writer.write(buf);

#ifdef DEBUG_MODE_RESULT
					cout << "[DEBUG] Worker ID: " << get_worker_id() << endl;
					cout << "[DEBUG] Vertex ID: " << v->id.vID << endl;
					cout << "[DEBUG] Result: " << mapping << endl;
#endif
					for (size_t j = 0; j < mapping.size(); j++)
					{
						sprintf(buf, "%d %d\n", query->dfs_order[j],
								mapping[j].vID);
						writer.write(buf);
					}
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

	if (params.input)
	{
		time = worker.run_type(PREPROCESS, params);
		compute_time += time;
	}

	stop_timer(TOTAL_TIMER);
	offline_time = get_timer(TOTAL_TIMER);
	reset_timer(TOTAL_TIMER);

	time = worker.load_query(params.query_path);
	load_time += time;

	start_timer(COMPUTE_TIMER);
	if (params.filter)
	{
		wakeAll();
		time = worker.run_type(FILTER, params);
	}

	time = worker.build_query_tree(params.order);

	wakeAll();
	time = worker.run_type(MATCH, params);

	wakeAll();
	time = worker.run_type(ENUMERATE, params);
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
				(*((AggMap*)global_agg))[make_pair(0, 0)] << endl;
		cout << "Load time: " << load_time << " s." << endl;
		cout << "Dump time: " << dump_time << " s." << endl;
		cout << "Compute time: " << compute_time << " s." << endl;
		cout << "Offline time: " << offline_time << " s." << endl;
		cout << "Online time: " << online_time << " s." << endl;
	}
}
