#include "basic/pregel-dev.h"
#include "utils/type.h"
#include "utils/Query.h"
using namespace std;

#define LEVEL (step_num()-1)
#define START_TIMING(T) (T) = get_current_time();
#define STOP_TIMING(A, T, X, Y) (A)->addTime((X), (Y), get_current_time() - (T));
#define MPRINT(str) \
	if (get_worker_id() == MASTER_RANK) \
		printf("%s\n", (str));

/*
#define DEBUG_MODE_ACTIVE 1
#define DEBUG_MODE_BRANCH 1
#define DEBUG_MODE_MSG 1 
#define DEBUG_MODE_RESULT_COUNT 1
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
#include "SItypes/SIAggregator.h"
#include "SItypes/SIMessage.h"
#include "SItypes/SICandidate.h"
#include "SItypes/bloom_filter.h"

//===============================================================

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
	/* This class is designed for a single vertex in the data graph. 
	 * It comprises of the key (vertexID and workerID), the value 
	 * (label, neighbors) and additional information (final mappings
	 * when the vertex is mapped to a leaf query vertex, mapped query
	 * vertices, etc.).
	 * */

public:
	// number of final mappings
	long mapping_count = 0;	
	// the following two vectors have the same length
	// for leaf vertex, final_u > 0; for dummy vertex, final_u < 0
	vector<int> final_us;
	// for different final_u, including markers, unmarked/marked branches
	vector<vector<SIBranch*>> final_results;
	// a list of mapped query vertices, recorded for conflicts
	vector<int> mapped_us;

	void preprocess(MessageContainer & messages, WorkerParams &params)
	{
		/* This function is called prior to subgraph matching.
		 * It basically convert large neighbor vector to set
		 * to accelerate neighbor finding. */
		if (value().degree > 10)
			for (int i = 0; i < value().degree; ++i)
				value().nbs_set.insert(value().nbs_vector[i].key.vID);
		vote_to_halt();
	}

	void filter(MessageContainer & messages)
	{
		/* Not implemented yet. */
		return;
	}

	bool check_feasibility(int *mapping, int query_u, int vID)
	{ 
		/* This function checks vertex uniqueness and backward neighbors,
		 * used when local intersection is enabled. */
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
		/* This function builds a dummy vertex whenever branch query
		 * vertex is encountered, the dummy vertex comprises of the
		 * newly built branch(RCT). */
		int dummyID = create_dummy_vertex_id();
		SIVertex* v = new SIVertex;
		v->id = SIKey(dummyID, id.wID);
		v->final_us.push_back(-1); // curr_u = b->curr_u
		v->final_results.resize(1);
		v->final_results[0].push_back(b);
		this->add_vertex(v);
		return dummyID;
	}

	SIMessage copy_message(SIMessage msg)
	{
		/* This function is used when message is not transferred to
		 * another machine, i.e. the dest vertex and the src vertex
		 * reside in the same machine. */
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

		// DEBUG: Print out the copied message
		/*
		cout << "[Message]" << endl;
		cout << "nrow: " << msg.nrow << " ncol: " << new_ncol << endl;
		for (int i = 0; i < msg.nrow*new_ncol; i++)
			cout << mappings[i] << " ";
		cout << endl;
		*/

#if NONLOCAL_INTERSECTION
		return SIMessage(MESSAGE_TYPES::IN_MAPPING, mappings,
			msg.curr_u, msg.nrow, new_ncol, msg.is_delete, msg.markers,
			msg.neighbors);
#else
		return SIMessage(MESSAGE_TYPES::IN_MAPPING, mappings,
			msg.curr_u, msg.nrow, new_ncol, msg.is_delete, msg.markers);
#endif
	}

	void addPsdChildren(SIBranch *b, int u_index, int msg_vID, int msg_wID, 
		int result_index)
	{
		/* This function adds pseudo children into branch.
		 * If there exists a conflict then it sends a psd request
		 * to neighbors. 
		 * Some input arguments are listed below:
		 * - u_index: the index in final_us
		 * - result_index: the index in final_results[u_index]
		 * - msg_vID, msg_wID: where the response messages are sent to
		 * */
		SIQuery* query = (SIQuery*)getQuery();
		vector<int> &ps_chds = query->getPseudoChildren(b->curr_u);
		int chd_sz = query->getChildren(b->curr_u).size();
		for (int i = 0; i < ps_chds.size(); i++)
		{
			int ps_chd = ps_chds[i];
			int label = query->getLabel(ps_chd);
			if (query->hasConflict(ps_chd))
			{   // send a message of psd_request
#if ONE_COPY_TRANSMISSION
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
#else
				for (int j = 0; j < value().degree; ++j)
				{
					KeyLabel &kl = value().nbs_vector[j];
					if ((label == kl.label) && 
						check_feasibility(b->mapping, ps_chd, kl.key.vID))
						send_messages(kl.key.wID, {kl.key.vID}, 
							SIMessage(PSD_REQUEST, ps_chd, u_index,
							msg_vID, msg_wID, result_index, chd_sz+i));
				}
#endif
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
		/* The main function in the path sketch exploration stage.
		 * According to the property of the query vertex, this function 
		 * does the following work:
		 * - If the query vertex is a branch vertex, it builds an RCT, builds
		 *   a dummy vertex to store the RCT, and sends message to neighbors.
		 * - If the query vertex is a leaf vertex, it stores all the path 
		 *   mappings and stop matching.
		 * - Otherwise, it sends message to neighbors to continue matching.
		 * */
		SIQuery* query = (SIQuery*)getQuery();
		SIAgg* agg = (SIAgg*)get_aggregator();
		double t, t1, t2;

#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID
			 << " Worker ID " << id.wID
			 << endl;
#endif

		// This if clause is only intended for newly built dummy vertices.
		// After receiving psd_response they can halt.
		if (id.vID < 0) 
		{
			for (int i = 0; i < messages.size(); i++)
			{
				SIMessage &msg = messages[i];
#ifdef DEBUG_MODE_MSG
				cout << "Received Message: " << i << endl;
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

		/* For ordinary data vertices, the path sketch exploration stage
		 * starts here. */
		/* The first step is to arrange messages to put them into different
		 * bins of query u's. */
		START_TIMING(t);
		vector<int> vector_u = query->getBucket(LEVEL, value().label);
		int n_u = vector_u.size();
		int curr_u;
		vector<vector<int>> messages_classifier = vector<vector<int>>(n_u);
		if (step_num() == 1)
		{
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
				SIMessage &msg = messages[i];
#ifdef DEBUG_MODE_MSG
				cout << "Received Message: " << i << endl;
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
		STOP_TIMING(agg, t, 1, 2);

		/* The second step is to respond according to the messages received,
		 * by different query vertices. This is the main computation in this
		 * function. */
		START_TIMING(t);
		for (int bucket_num = 0; bucket_num < n_u; bucket_num ++)
		{
			if (step_num() != 1 && messages_classifier[bucket_num].empty())
				continue;

			curr_u = vector_u[bucket_num];
			int ncol = query->getNCOL(curr_u);
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

			// The property of current query vertex
			bool is_branch = query->isBranch(curr_u);
			bool is_pseudo = query->isPseudo(curr_u);
			bool is_leaf = next_us.empty();

			START_TIMING(t1);
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

#if NONLOCAL_INTERSECTION
// In this case, message only has 1 nrow, and we don't need to check_feasibility
			if (LEVEL == 0)
			{
				SIMessage start_msg = SIMessage(MESSAGE_TYPES::IN_MAPPING, nullptr,
					curr_u, 1, 0, true, markers, vector<vector<int>>());
				messages.push_back(start_msg);
				messages_classifier[bucket_num].push_back(0);
			}
			if (is_branch)
			{
				markers->push_back(0); // zero out at dummy

				for (int msgi : messages_classifier[bucket_num])
				{
					SIMessage &msg = messages[msgi];
					int *new_mapping = msg.mappings;
					passed_mappings->push_back(new_mapping);
					
					SIBranch* b = new SIBranch(new_mapping, id.vID,
						msg.ncol, curr_u, conflict_number);
					int dummyID = build_dummy_vertex(b);
					dummy_vs->push_back(dummyID);
					addPsdChildren(b, 0, dummyID, id.wID, 0);
					agg->addMappingCount(0, 0, msg.ncol);
	
					int type = -1;
					
					for (int next_u_index = 0; next_u_index < next_us.size(); 
						next_u_index++)
					{
						int next_u = next_us[next_u_index];
						SIMessage out_message;
						vector<int> chd_constraint = vector<int>();
						bool include_self = true;
						chd_constraint = query->getChdConstraint(curr_u, next_u_index);
						include_self = query->getIncludeSelf(curr_u, next_u_index);
						if (include_self)
							type = MESSAGE_TYPES::BMAPPING_W_SELF;
						else
							type = MESSAGE_TYPES::BMAPPING_WO_SELF;
						
						msg.neighbors.push_back(vector<int>());
						vector<int> &pv = msg.neighbors.back();
						for (KeyLabel &kl: value().nbs_vector)
							pv.push_back(kl.key.vID);
						out_message = SIMessage(type, passed_mappings, 
							dummy_vs, next_u, 1, ncol, id.vID, id.wID, markers,
							chd_constraint, msg.neighbors);

						// Need to satisfy all the below conditions to send:
						// 1. the next v has the correct label
						// 2. the next v has not been used.
						// 3. the next v lies in the all the neighbors							
						for (int i = 0; i < value().degree; ++i)
						{
							KeyLabel &kl = value().nbs_vector[i];
							bool flag = true;
							// Condition 1
							if (kl.label != query->getLabel(next_u))
								flag = false;
							// Condition 2
							for (int i = 0; i < msg.ncol; i++)
								if (kl.key.vID == msg.mappings[i])
									flag = false;
							// Condition 3
							for (int b_level : query->getBNeighborsPos(next_u))
							{
								vector<int> &v = msg.neighbors[b_level];
								if (find(v.begin(), v.end(), kl.key.vID) == v.end())
									flag = false;
							}
							
							// Only send the messages when conditions are met	
							if (flag)
								if (kl.key.wID == get_worker_id())
									send_messages(kl.key.wID, {kl.key.vID},
										copy_message(out_message));
								else
									send_messages(kl.key.wID, {kl.key.vID}, out_message);
						}
						dummy_vs->clear();
						passed_mappings->clear();
					}//end of next_u loop		
				}//end of message loop
			}
			else if (is_leaf)
			{
				START_TIMING(t2);
				int final_index = this->final_us.size();
				this->final_us.push_back(curr_u);
				this->final_results.push_back(vector<SIBranch*>());
				for (int msgi : messages_classifier[bucket_num])
				{
					SIMessage &msg = messages[msgi];
					int *new_mapping = msg.mappings;
					SIBranch* b = new SIBranch(new_mapping, id.vID,
						msg.ncol, curr_u, (*msg.markers)[0] + conflict_number);
					addPsdChildren(b, final_index, id.vID, id.wID,
						this->final_results[final_index].size());
					this->final_results[final_index].push_back(b);
					agg->addMappingCount(0, 0, msg.ncol);
					for (vector<int> &nbr : msg.neighbors)
						agg->addMappingCount(0, 0, nbr.size());
				}
				STOP_TIMING(agg, t2, 2, 1);

			}
			else // not branch nor leaf
			{
				START_TIMING(t2);
				for (int msgi : messages_classifier[bucket_num])
				{
					SIMessage &msg = messages[msgi];
					int *new_mapping = msg.mappings;
					passed_mappings->push_back(new_mapping);
					markers->push_back((*msg.markers)[0] + conflict_number);

					int next_u = next_us[0];
					vector<int> chd_constraint = vector<int>();
					bool include_self = true;
					int type = MESSAGE_TYPES::OUT_MAPPING;
					msg.neighbors.push_back(vector<int>());
					vector<int> &pv = msg.neighbors.back();
					for (KeyLabel &kl: value().nbs_vector)
						pv.push_back(kl.key.vID);
					SIMessage out_message = SIMessage(type, passed_mappings, 
						dummy_vs, next_u, 1, ncol, id.vID, id.wID, markers,
						chd_constraint, msg.neighbors);

					// Need to satisfy all the below conditions to send:
					// 1. the next v has the correct label
					// 2. the next v has not been used.
					// 3. the next v lies in the all the neighbors							
					for (int i = 0; i < value().degree; ++i)
					{
						KeyLabel &kl = value().nbs_vector[i];
						bool flag = true;
						// Condition 1
						if (kl.label != query->getLabel(next_u))
							flag = false;
						// Condition 2
						for (int i = 0; i < msg.ncol; i++)
							if (kl.key.vID == msg.mappings[i])
								flag = false;
						// Condition 3
						for (int b_level : query->getBNeighborsPos(next_u))
						{
							vector<int> &v = msg.neighbors[b_level];
							if (find(v.begin(), v.end(), kl.key.vID) == v.end())
								flag = false;
						}
	
						if (flag)
							if (kl.key.wID == get_worker_id())
								send_messages(kl.key.wID, {kl.key.vID},
									copy_message(out_message));
							else
								send_messages(kl.key.wID, {kl.key.vID}, out_message);
					}
					dummy_vs->clear();
					passed_mappings->clear();					
				}//end of message loop
			}//end of if is_branch/is_leaf loop
#else
			// if LOCAL_INTERSECTION ENABLED
			if (is_branch)
			{
				START_TIMING(t2);

				// special case: root branch vertex
				if (step_num() == 1)
				{
					dummy_vs->push_back(id.vID);
					SIBranch* b = new SIBranch(NULL, id.vID, 0, curr_u, 0);
					addPsdChildren(b, 0, id.vID, id.wID, 0);
# ifdef DEBUG_MODE_BRANCH
					b->print();
# endif
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
						if (check_feasibility(new_mapping, curr_u, id.vID))
						{
							passed_mappings->push_back(new_mapping);
							markers->push_back(0); // zero out at dummy

							SIBranch* b = new SIBranch(new_mapping, id.vID,
								msg.ncol, curr_u, (*msg.markers)[i] + conflict_number);
							int dummyID = build_dummy_vertex(b);
							dummy_vs->push_back(dummyID);
							addPsdChildren(b, 0, dummyID, id.wID, 0);
# ifdef DEBUG_MODE_BRANCH
							b->print();
# endif
							agg->addMappingCount(0, 0, msg.ncol);
						}
					}
				}
				STOP_TIMING(agg, t2, 2, 0);
			}
			else if (is_leaf)
			{
				START_TIMING(t2);
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
# ifdef DEBUG_MODE_BRANCH
							b->print();
# endif
							agg->addMappingCount(0, 0, msg.ncol);
						}
					}
				}
				STOP_TIMING(agg, t2, 2, 1);
			}
			else // not branch nor leaf
			{
				START_TIMING(t2);
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
				STOP_TIMING(agg, t2, 2, 2);
			}
			STOP_TIMING(agg, t1, 0, 2);

			// Continue mapping: send mappings to children
			START_TIMING(t1);
			if (!passed_mappings->empty() || step_num() == 1)
			{
# if ONE_COPY_TRANSMISSION
				vector<vector<int>> neighbors_map = vector<vector<int>>(get_num_workers());
# endif
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

					// Construct the message
					START_TIMING(t2);
					SIMessage out_message;
					if (!is_branch)
						type = MESSAGE_TYPES::OUT_MAPPING;
					else if (include_self)
						type = MESSAGE_TYPES::BMAPPING_W_SELF;
					else
						type = MESSAGE_TYPES::BMAPPING_WO_SELF;
					int nrow;
					if (LEVEL == 0)
						nrow = 1;
					else
						nrow = passed_mappings->size();
					out_message = SIMessage(type, passed_mappings, 
						dummy_vs, next_u, nrow, ncol, id.vID, id.wID, markers,
						chd_constraint);

					int next_label = query->getLabel(next_u);
# if ONE_COPY_TRANSMISSION
					for (int i = 0; i < value().degree; ++i)
					{
						KeyLabel &kl = value().nbs_vector[i];
						if (kl.label == next_label)
							neighbors_map[kl.key.wID].push_back(kl.key.vID);
					}
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
# else
					for (int i = 0; i < value().degree; ++i)
					{
						KeyLabel &kl = value().nbs_vector[i];
						if (kl.label == next_label)
							if (kl.key.wID == get_worker_id())
								send_messages(kl.key.wID, {kl.key.vID},
									copy_message(out_message));
							else
								send_messages(kl.key.wID, {kl.key.vID}, out_message);
					}
# endif
					STOP_TIMING(agg, t2, 1, 1);

# ifdef DEBUG_MODE_MSG
					cout << "Send out message" << endl;
					out_message.print();
# endif
				} //end of for loop of next_u	
			} //end of continue mapping of curr_u
			STOP_TIMING(agg, t1, 1, 0);
#endif
		}//end of for curr_u loop
		STOP_TIMING(agg, t, 0, 1);
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
		START_TIMING(t);
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
		STOP_TIMING(agg, t, 0, 2);

		// Phase II: Enumerate Trees
		START_TIMING(t);
		int cv = query->getCAOCValue(branch->curr_u);

		if (!branch->enumerateTrees(cv)) // invalid branch
		{
#ifdef DEBUG_MODE_BRANCH
			cout << "The branch is invalid. " << endl;
			branch->print();
#endif
			agg->addMappingCount(2, 2, branch->computeSize());
			STOP_TIMING(agg, t, 1, 1);
			return;
		}
		STOP_TIMING(agg, t, 1, 0);

#ifdef DEBUG_MODE_BRANCH
		branch->print();
#endif

		// Phase III: Send to dummy or expand
		if (offset == 0)
		{
			START_TIMING(t1);
			int k = query->getConflicts().size();
			for (int ti : branch->tree_indices)
			{
				vector<int> conflict_vs = vector<int>(k, -1);
				int n = branch->expand(ti, conflict_vs);
				this->mapping_count += n;
				//cout << "&& ti = " << ti << " n = " << n << endl;
			}
#ifdef DEBUG_MODE_RESULT_COUNT
			cout << "this->mapping_count = " << this->mapping_count << endl;
#endif
			STOP_TIMING(agg, t1, 1, 2);
		}
		else
		{
			START_TIMING(t1);
			int vID = p[offset-2];
			int wID = p[offset-1];
			SIMessage out_msg = SIMessage(BRANCH_RESULT, branch);
			if (wID == get_worker_id())
				out_msg.is_delete = false;
			send_messages(wID, {vID}, out_msg);
#ifdef DEBUG_MODE_MSG
			cout << "message send to " << vID << endl;
#endif
			STOP_TIMING(agg, t1, 2, 0);
		}
	}

	void enumerate(MessageContainer & messages)
	{
		SIQuery* query = (SIQuery*)getQuery();
		SIAgg* agg = (SIAgg*)get_aggregator();

#ifdef DEBUG_MODE_ACTIVE
		cout << "[DEBUG] STEP NUMBER " << step_num()
			 << " ACTIVE Vertex ID " << id.vID 
			 << " Worker ID " << id.wID
			 << endl;
#endif
		bool to_halt = true;
		double t;

		// might have multiple leaf u and at most one dummy u
		for (int i = 0 ; i < this->final_us.size(); i++)
		{
			int curr_u = this->final_us[i];
			//cout << "curr_u = " << curr_u << endl;
			
			vector<SIBranch*> final_result = this->final_results[i];
			if (curr_u >= 0) // leaf vertex
			{
				START_TIMING(t);
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
				STOP_TIMING(agg, t, 2, 1);
			}
			else if (!messages.empty()) // dummy vertex
			{
				START_TIMING(t);
				SIBranch *b = final_result[0];
				int dummy_pos = query->getDummyPos(b->curr_u);
				int offset;
				if (dummy_pos < 0)
					offset = 0;
				else
					offset = dummy_pos + 2;

				build_branch(messages, b, offset);	
				STOP_TIMING(agg, t, 0, 1);
			}
		}

		agg->addMappingCount(0, 0, this->mapping_count);

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
		// vertexID labelID \t neighbor1 neighbor1ID neighbor2 neighbor2ID ...
		virtual SIVertex* toVertex(char* line)
		{
			char * pch;
			SIVertex* v = new SIVertex;

			pch = strtok(line, " \t");
			if (*pch == '#') return NULL;
			int id = atoi(pch);
			v->id = SIKey(id, id % _num_workers);

			pch = strtok(NULL, " \t");
			v->value().label = atoi(pch); //(int) *pch;

			SIKey key;
			while ((pch = strtok(NULL, " ")) != NULL)
			{
				id = atoi(pch);
				key = SIKey(id, id % _num_workers);
				pch = strtok(NULL, " ");
				v->value().nbs_vector.push_back(KeyLabel(key, atoi(pch))); //(int) *pch));
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
						break;
					case IN_MAPPING:
						if (msg.is_delete)
							delete[] msg.mappings;
						break;
					case BRANCH_RESULT:
						if (msg.is_delete)
							delete msg.branch;
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
	worker.run_type(PREPROCESS, params, 1);
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
	worker.load_query(params.query_path, params.input);
	StopTimer(STAGE_TIMER);
	PrintTimer("Loading query graph time", STAGE_TIMER)

	//=============== The most important timer starts here!!! =================
	StartTimer(COMPUTE_TIMER);

	// STAGE 2: Filtering

	// STAGE 3: Build query tree
	MPRINT("Building query tree...")
	ResetTimer(STAGE_TIMER);
	int depth, bn;
	worker.build_query_tree(params.order, depth, bn);
	if (_my_rank == MASTER_RANK)
		cout << "depth = " << depth << " max branch number = " << bn << endl;
	StopTimer(STAGE_TIMER);
	PrintTimer("Building query tree time", STAGE_TIMER)

	// STAGE 4: Subgraph matching
	MPRINT("**Subgraph matching**")
	ResetTimer(STAGE_TIMER);
	worker.run_type(MATCH, params, depth+1);
	StopTimer(STAGE_TIMER);
	PrintTimer("Subgraph matching time", STAGE_TIMER)

	if (_my_rank == MASTER_RANK)
	{
		cout << "[Detailed report]" << endl;
		auto mat = *((AggMat*)global_agg);
		cout << "1. Arrange messages: " <<
			mat[1][2] << " s" << endl;
		cout << "2. Main Computation: " <<
			mat[0][1] << " s" << endl;
		cout << "2.1. Loop through messages and check feasibility: " <<
			mat[0][2] << " s" << endl;
		cout << "2.1.1. For branch vertices: " <<
			mat[2][0] << " s" << endl;		
		cout << "2.1.2. For leaf vertices: " <<
			mat[2][1] << " s" << endl;
		cout << "2.1.3. For not-branch-nor-leaf vertices: " <<
			mat[2][2] << " s" << endl;
		cout << "2.2. Continue mapping: " <<
			mat[1][0] << " s" << endl;	
		cout << "2.2.1. Send messages " <<
			mat[1][1] << " s" << endl;
	}

	if (_my_rank == MASTER_RANK)
	{
		cout << "================ Intermediate Report ===============" << endl;
		double num = (*((AggMat*)global_agg))[0][0] / 256; //* 4 / 1024;
		string unit = "KB";
		if (num > 1024)
		{
			num /= 1024;
			unit = "MB";
		}
		cout << "Intermediate result size: " << num << " " << unit << endl;
		cout << endl;
	}

	// STAGE 5: Subgraph enumeration
	MPRINT("**Subgraph enumeration**")
	ResetTimer(STAGE_TIMER);
	worker.run_type(ENUMERATE, params, bn+1);
	StopTimer(STAGE_TIMER);
	PrintTimer("Subgraph enumeration time", STAGE_TIMER)

	if (_my_rank == MASTER_RANK)
	{
		cout << "[Detailed report]" << endl;
		auto mat = *((AggMat*)global_agg);
		cout << "a) For leaf vertices: " <<
			mat[2][1] << " s" << endl;
		cout << "b) For dummy vertices: " <<
			mat[0][1] << " s" << endl;
		cout << "==build_branch==" << endl;
		cout << "1. Organize branches: " <<
			mat[0][2] << " s" << endl;	
		cout << "2. Enumerate trees: " <<
			mat[1][0] << " s" << endl;		
		cout << "3.1. Send to dummies: " <<
			mat[2][0] << " s" << endl;
		cout << "3.2. Expand: " <<
			mat[1][2] << " s" << endl;
		cout << "==Waste Report==" << endl;
		cout << "- Wasting time: " << mat[1][1] << " s" << endl;
		cout << "- Wasting space: " << mat[2][2]/256 << " KB" << endl;
	}

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
