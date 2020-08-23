#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

#define DEBUG_MODE 1
//#define DEBUG_MODE_ACTIVE 1
#define DEBUG_MODE_MSG 1

//input line format:
//  type \t vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
//output line format:
//  # MATCH
//  query_vertexID \t data_vertexID

//--------------------SIKey = <VertexID, isQuery>--------------------

struct SIKey {
    VertexID vID;
    bool isQuery;

    SIKey()
    {
    }

    SIKey(int v1, bool v2)
    {
        this->vID = v1;
        this->isQuery = v2;
    }

    void set(int v1, bool v2)
    {
        this->vID = v1;
        this->isQuery = v2;
    }

    inline bool operator<(const SIKey& rhs) const
    {
        return (vID < rhs.vID);
    }

    inline bool operator>(const SIKey& rhs) const
    {
        return (vID > rhs.vID);
    }

    inline bool operator==(const SIKey& rhs) const
    {
        return (vID == rhs.vID) && (isQuery == rhs.isQuery);
    }

    inline bool operator!=(const SIKey& rhs) const
    {
        return (vID != rhs.vID) || (isQuery != rhs.isQuery);
    }

    int hash()
    {
        size_t seed = 0;
        hash_combine(seed, vID);
        hash_combine(seed, (int) isQuery);
        return seed % ((unsigned int)_num_workers);
    }
};

ibinstream& operator<<(ibinstream& m, const SIKey& v)
{
    m << v.vID;
    m << v.isQuery;
    return m;
}

obinstream& operator>>(obinstream& m, SIKey& v)
{
    m >> v.vID;
    m >> v.isQuery;
    return m;
}

class SIKeyHash {
public:
    inline int operator()(SIKey key)
    {
        return key.hash();
    }
};

namespace __gnu_cxx {
	template <>
	struct hash<SIKey> {
		size_t operator()(SIKey key) const
		{
			size_t seed = 0;
			hash_combine(seed, key.vID);
			hash_combine(seed, (int) key.isQuery);
			return seed;
		}
	};
}

//------------SIValue = <label, hash_map<neighbors, labels>>-------------------

struct SIValue
{
	int label;
	hash_map<VertexID, int> neighbors;
};

ibinstream & operator<<(ibinstream & m, const SIValue & v){
	m << v.label << v.neighbors;
	return m;
}

obinstream & operator>>(obinstream & m, SIValue & v){
	m >> v.label >> v.neighbors;
	return m;
}

//--------SIMessage = <type, vertex, info, backward_neighbors, mapping>--------
//  e.g. <type = LABEL_INFOMATION, vertex, label>
//	e.g. <type = NEXT_QUERY_VERTEX, vertex = max query vertex, label, b_nbs>
//	e.g. <type = MAPPING, label, b_nbs>

struct SIMessage
{
	int type;
	VertexID vertex;
	int label;
	vector<VertexID> b_nbs; // backward_neighbors
	vector<VertexID> mapping;

	SIMessage()
	{
	}

	SIMessage(int type, VertexID vertex, int label)
	{ // for label information
		this->type = type;
		this->vertex = vertex;
		this->label = label;
	}

	SIMessage(int type, vector<VertexID> b_nbs, vector<VertexID> mapping)
	{ // for mapping
		this->type = type;
		this->b_nbs = b_nbs;
		this->mapping = mapping;
	}
};

ibinstream & operator<<(ibinstream & m, const SIMessage & v){
	m << v.type << v.vertex << v.label << v.b_nbs << v.mapping;
	return m;
}

obinstream & operator>>(obinstream & m, SIMessage & v){
	m >> v.type >> v.vertex >> v.label >> v.b_nbs >> v.mapping;
	return m;
}

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 1,
	NEXT_QUERY_VERTEX = 2,
	MAPPING = 3
};

// Overloading << operator of vector, print like Python, for debug purpose

ostream & operator << (ostream & os, const vector<int> & v)
{
	os << "[";
	for (int i = 0; i < v.size(); i++)
	{
		os << v[i];
		if (i != (v.size() - 1)) os << ", ";
	}
	os << "]";
	return os;
}

//=============================================================================

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
	public:
		vector<vector<VertexID>> results; // only used in the final step

		bool containsNeighbors(vector<VertexID> & backward_neighbors,
				vector<VertexID> & mapping)
		{ // Check if all backward edges are satisfied.
			hash_map<VertexID, int> & nbs = value().neighbors;
			int index;
			for (int i = 0; i < backward_neighbors.size(); i++)
			{
				index = backward_neighbors[i] - 1;
				if (nbs.find(mapping[index]) == nbs.end())
					return false;
			}
			return true;
		}

		void continue_mapping(vector<VertexID> & mapping)
		{ /* Add current vertex to mapping;
		   * Send messages to mapped vertices that are neighboring to
		   * the next query vertex.
		   */
			mapping.push_back(id.vID);
#ifdef DEBUG_MODE
				cout << "[Result] Partial mapping:  " << mapping << endl;
#endif
			vector<VertexID> b_nbs = ((SIMessage*)getAgg())->b_nbs; // copy
			int query_size = ((SIMessage*)getAgg())->vertex;
			if (mapping.size() >= query_size)
			{
				results.push_back(mapping);
#ifdef DEBUG_MODE
				cout << "[Result] Final mapping:  " << mapping << endl;
#endif
			}
			else
			{
				int index, num = b_nbs.size();
				if (num == 0)
				{
					cout << "Error: query graph not connected." << endl;
					forceTerminate();
				}
				else
				{
					index = b_nbs[num-1] - 1; // the last backward neighbor
					b_nbs.pop_back();

					SIKey to_send = SIKey(mapping[index], false);
					SIMessage msg = SIMessage(MAPPING, b_nbs, mapping);
					send_message(to_send, msg);
#ifdef DEBUG_MODE_MSG
					cout << "[DEBUG] Message sent from " << id.vID << " to "
						 << to_send.vID << ". \n\t"
						 << "Type: MAPPING. \n\t"
						 << "Backward Neighbors: " << msg.b_nbs << ". \n\t"
						 << "Mapping: " << msg.mapping << endl;
#endif
				}
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			hash_map<VertexID, int> & nbs = value().neighbors;

			/* Query vertices do nothing. Don't halt them because we will need
			 * them in the aggregator.
			 */
			if (!id.isQuery)
			{
#ifdef DEBUG_MODE_ACTIVE
				cout << "[DEBUG] STEP NUMBER " << step_num()
					 << " ACTIVE Vertex ID " << id.vID << endl;
#endif
				if (step_num() == 1)
				{ // send label info to neighbors
					SIMessage msg = SIMessage(LABEL_INFOMATION, id.vID,
							value().label);
					hash_map<VertexID, int>::iterator it;
					for (it = nbs.begin(); it != nbs.end(); it++)
						send_message(SIKey(it->first, false), msg);
				}
				else if (step_num() == 2)
				{ // receive label info from neighbors, and start matching
					for (int i = 0; i < messages.size(); i++)
					{
						SIMessage & msg = messages[i];
						if (msg.type == LABEL_INFOMATION)
							nbs[msg.vertex] = msg.label;
					}
					// start mapping with vertices with same label
					int query_label = ((SIMessage*)getAgg())->label;
					if (value().label != query_label)
						vote_to_halt();
				}
				else if (step_num() % 2 == 1)
				{ // if if backward neighbors in neighbors, continue mapping.
					if (messages.size() == 0)
					{
						vector<VertexID> mapping;
						continue_mapping(mapping);
					}
					else
					{
						for (int i = 0; i < messages.size(); i++)
						{
							SIMessage & msg = messages[i];
							if (containsNeighbors(msg.b_nbs, msg.mapping))
								continue_mapping(msg.mapping);
						}
					}
					vote_to_halt();
				}
				else
				{ /* receive messages of MESSAGE_FRAGMENT,
				   * forward them to neighbors of specific labels. */
					int query_label = ((SIMessage*)getAgg())->label;
					hash_map<VertexID, int>::iterator it;
					for (int i = 0; i < messages.size(); i++)
					{
						SIMessage & msg = messages[i];
						if (msg.type == MAPPING)
						{
							for (it = nbs.begin(); it != nbs.end(); it++)
							{
								if (it->second == query_label)
								{
									send_message(SIKey(it->first, false), msg);
#ifdef DEBUG_MODE_MSG
									cout << "[DEBUG] Message sent from " << id.vID << " to "
										 << it->first << ". \n\t"
										 << "Type: FWD: MAPPING. \n\t"
										 << "Backward Neighbors: " << msg.b_nbs << ". \n\t"
										 << "Mapping: " << msg.mapping << endl;
#endif
								}
							}
						}
					} // end of for loop
					vote_to_halt();
				} // end of if step_num % 2 == 0
			} // end of if !id.isQuery
		}
};

//====================================
// Use aggregator to notify data vertices of next query vertex.
// At round (2*i-2), stores the neighbors of query vertex i.
// At round (2*i-1), stores the label of query vertex i.
// Also stores the maximum query vertex in msg.vertex.

class SIAgg:public Aggregator<SIVertex, SIMessage, SIMessage>
{
	private:
		SIMessage msg;
	public:
		virtual void init() {
			msg.vertex = 0;
			msg.type = 0;
			msg.b_nbs.clear();
		}

		virtual void stepPartial(SIVertex* v)
		{
			if (v->id.isQuery)
			{
				msg.vertex = v->id.vID;

				if (step_num() % 2 == 1 && v->id.vID == (step_num() + 1) / 2)
				{
					msg.type = NEXT_QUERY_VERTEX;
					msg.label = v->value().label;
				}
				else if (step_num() % 2 == 0 && v->id.isQuery &&
						v->id.vID == (step_num() / 2) + 1)
				{
					msg.type = NEXT_QUERY_VERTEX;
					hash_map<VertexID, int> & nbs = v->value().neighbors;
					hash_map<VertexID, int>::iterator it;
					for (it = nbs.begin(); it != nbs.end(); it++)
					{
						msg.b_nbs.push_back(it->first);
					}
				}
			}
		}

		virtual void stepFinal(SIMessage* received_msg)
		{
			int max_qv = (received_msg->vertex > msg.vertex) ?
						  received_msg->vertex : msg.vertex;

			if (received_msg->type == NEXT_QUERY_VERTEX)
				msg = *received_msg;

			msg.vertex = max_qv;
		}

		virtual SIMessage* finishPartial(){ return &msg; }
		virtual SIMessage* finishFinal()
		{
			if (step_num() > 2 * msg.vertex)
				forceTerminate();
			return &msg;
		}
};

//=============================================================================

class SIWorker:public Worker<SIVertex, SIAgg>
{
	char buf[100];

	public:
		// C version
		// input line format:
		// type \t vertexID labelID numOfNeighbors neighbor1 neighbor2 ...
		virtual SIVertex* toVertex(char* line)
		{
			char * pch;
			SIVertex* v = new SIVertex;

			pch = strtok(line, "\t");
			bool isQuery = (*pch == 'Q');

			pch = strtok(NULL, " ");
			VertexID vID = atoi(pch);
			v->id = SIKey(vID, isQuery);

			pch = strtok(NULL, " ");
			v->value().label = atoi(pch);

			pch = strtok(NULL, " ");
			int num = atoi(pch);
			for (int k = 0; k < num; k++)
			{
				pch=strtok(NULL, " ");
				int neighbor = atoi(pch);
				// for queries, we only push back neighbors with smaller IDs
				if (!(isQuery && vID < neighbor))
					v->value().neighbors[neighbor] = 0;
			}
			return v;
		}

		virtual void toline(SIVertex* v, BufferedWriter & writer)
		{
			vector<vector<VertexID>> & results = v->results;
			for (int i = 0; i < results.size(); i++)
			{
				vector<VertexID> & mapping = results[i];
				sprintf(buf, "# Match\n");
				writer.write(buf);

				for (int j = 0; j < mapping.size(); j++)
				{
					sprintf(buf, "%d %d\n", (j+1), mapping[j]);
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

void pregel_subgraph(string in_path, string out_path, bool force_write)
{
	SIWorker worker;
	//CCCombiner_pregel combiner;
	//if(use_combiner) worker.setCombiner(&combiner);

	SIAgg agg;
	worker.setAggregator(&agg);
	worker.run_load_graph(in_path);
	worker.run_compute();
	worker.run_dump_graph(out_path, force_write);
}
