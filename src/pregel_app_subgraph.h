#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

#define DEBUG_MODE 1
//#define DEBUG_MODE_MSG 1

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

//-----------SIMessage = <type, vertex, info, vector_of_vertex>--------------
//      type = LABEL_INFOMATION, vertex = vertex, info = label
//		type = NEXT_QUERY_VERTEX, vertex = max query vertex,
//								  info = label, vector = its neighbors
//		type = MESSAGE_FRAGMENT, info = number of messages, vector = mapping

struct SIMessage
{
	int type;
	VertexID vertex;
	int info;
	vector<VertexID> vec;

	SIMessage()
	{
	}

	SIMessage(int type, VertexID vertex, int info)
	{ // for label information
		this->type = type;
		this->vertex = vertex;
		this->info = info;
	}

	SIMessage(int type, int info, vector<VertexID> vec)
	{ // for message fragment
		this->type = type;
		this->info = info;
		this->vec = vec;
	}
};

ibinstream & operator<<(ibinstream & m, const SIMessage & v){
	m << v.type << v.vertex << v.info << v.vec;
	return m;
}

obinstream & operator>>(obinstream & m, SIMessage & v){
	m >> v.type >> v.vertex >> v.info >> v.vec;
	return m;
}

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 1,
	NEXT_QUERY_VERTEX = 2,
	MESSAGE_FRAGMENT = 3
};

//------------------Overloading hash and << of vector------------------------

namespace __gnu_cxx {
	template <>
	struct hash<vector<int>> {
		size_t operator()(vector<int> v) const
		{
			size_t seed = 0;
			for (int i = 0; i < v.size(); i++)
			{
				hash_combine(seed, v[i]);
			}
			return seed;
		}
	};
}

ostream & operator << (ostream & os, const vector<int> & v)
{ // print like Python, for debug purpose
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

		vector<vector<VertexID>> match(MessageContainer & messages)
		{ // Match MESSAGE_FRAGMENT
			vector<vector<VertexID>> mappings;
			hash_map<vector<VertexID>, int> map;
			for (int i = 0; i < messages.size(); i++)
			{
				SIMessage & msg = messages[i];
				if (map.find(msg.vec) == map.end())
					map[msg.vec] = msg.info - 1;
				else
					map[msg.vec]--;

				hash_map<vector<VertexID>, int>::iterator it;
				for (it = map.begin(); it != map.end(); it++)
					if (it->second == 0)
					{
						mappings.push_back(it->first);
					}

			}
			return mappings;
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
			vector<VertexID> & req = ((SIMessage*)getAgg())->vec;
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
				int num = req.size();
				int index;
				for (int i = 0; i < num; i++)
				{
					index = req[i] - 1;
					SIMessage msg = SIMessage(MESSAGE_FRAGMENT, num, mapping);
					send_message(SIKey(mapping[index], false), msg);
#ifdef DEBUG_MODE_MSG
					cout << "[DEBUG] Message sent from " << id.vID << " to "
						 << mapping[index] << ". \n\t"
						 << "Type: MESSAGE_FRAGMENT. \n\t"
						 << "Number: " << msg.info << ". \n\t"
						 << "Mapping: " << msg.vec << endl;
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
#ifdef DEBUG_MODE
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
							nbs[msg.vertex] = msg.info;
					}
					// start mapping with vertices with same label
					int query_label = ((SIMessage*)getAgg())->info;
					if (value().label != query_label)
						vote_to_halt();
				}
				else if (step_num() % 2 == 1)
				{ // if messages are matched, continue mapping.
					if (messages.size() == 0)
					{
						vector<VertexID> mapping;
						continue_mapping(mapping);
					}
					else
					{
						vector<vector<VertexID>> mappings = match(messages);
						for (int i = 0; i < mappings.size(); i++)
						{
							vector<VertexID> & mapping = mappings[i];
							continue_mapping(mapping);
						}
					}
					vote_to_halt();
				}
				else
				{ /* receive messages of MESSAGE_FRAGMENT,
				   * forward them to neighbors of specific labels. */
					int query_label = ((SIMessage*)getAgg())->info;
					hash_map<VertexID, int>::iterator it;
					for (int i = 0; i < messages.size(); i++)
					{
						SIMessage & msg = messages[i];
						if (msg.type == MESSAGE_FRAGMENT)
						{
							for (it = nbs.begin(); it != nbs.end(); it++)
							{
								if (it->second == query_label)
								{
									send_message(SIKey(it->first, false), msg);
#ifdef DEBUG_MODE_MSG
									cout << "[DEBUG] Message sent from " << id.vID << " to "
										 << it->first << ". \n\t"
										 << "Type: FWD: MESSAGE_FRAGMENT. \n\t"
										 << "Number: " << msg.info << ". \n\t"
										 << "Mapping: " << msg.vec << endl;
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
			msg.vec.clear();
		}

		virtual void stepPartial(SIVertex* v)
		{
			if (v->id.isQuery)
			{
				msg.vertex = v->id.vID;

				if (step_num() % 2 == 1 && v->id.vID == (step_num() + 1) / 2)
				{
					msg.type = NEXT_QUERY_VERTEX;
					msg.info = v->value().label;
				}
				else if (step_num() % 2 == 0 && v->id.isQuery &&
						v->id.vID == (step_num() / 2) + 1)
				{
					msg.type = NEXT_QUERY_VERTEX;
					hash_map<VertexID, int> & nbs = v->value().neighbors;
					hash_map<VertexID, int>::iterator it;
					for (it = nbs.begin(); it != nbs.end(); it++)
					{
						msg.vec.push_back(it->first);
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

void pregel_subgraph(string in_path, string out_path, bool use_combiner)
{
	WorkerParams params;
	params.input_path = in_path;
	params.output_path = out_path;

	SIWorker worker;
	//CCCombiner_pregel combiner;
	//if(use_combiner) worker.setCombiner(&combiner);

	SIAgg agg;
	worker.setAggregator(&agg);
	worker.run(params);
}
