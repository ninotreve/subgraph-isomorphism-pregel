#include "basic/pregel-dev.h"
#include "utils/type.h"
using namespace std;

#define DEBUG_MODE 1
#define DEBUG_MODE_ACTIVE 1
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

//--------SIMessage = <type, vertex, label, mapping>--------
//  e.g. <type = LABEL_INFOMATION, vertex, label>
//	e.g. <type = MAPPING, mapping, vertex = next_u>

struct SIMessage
{
	int type;
	VertexID vertex;
	int label;
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

	SIMessage(int type, vector<int> mapping, VertexID next_u)
	{ // for mapping
		this->type = type;
		this->mapping = mapping;
		this->vertex = next_u;
	}
};

ibinstream & operator<<(ibinstream & m, const SIMessage & v){
	m << v.type << v.vertex << v.label << v.mapping;
	return m;
}

obinstream & operator>>(obinstream & m, SIMessage & v){
	m >> v.type >> v.vertex >> v.label >> v.mapping;
	return m;
}

enum MESSAGE_TYPES {
	LABEL_INFOMATION = 1,
	MAPPING = 2
};

//=============================================================================
struct SINode
{
	int id;
	int label;
	vector<int> nbs;

	// for depth-first search
	bool visited;
	int parent;
	int level; // root: level 0
	vector<int> children;
	vector<int> b_nbs; // backward neighbors

	SINode() {}

	SINode(int id, int label)
	{
		this->id = id;
		this->label = label;
		this->visited = false;
	}

	void add_edge(int other)
	{
		this->nbs.push_back(other);
	}
};

ibinstream& operator<<(ibinstream& m, const SINode& node)
{
    m << node.id << node.label << node.nbs << node.visited << node.parent
      << node.level << node.children << node.b_nbs;
    return m;
}

obinstream& operator>>(obinstream& m, SINode& node)
{
    m >> node.id >> node.label >> node.nbs >> node.visited >> node.parent
      >> node.level >> node.children >> node.b_nbs;
    return m;
}

//==========================================================================
// Overloading << operator of vector, print like Python, for debug purpose

ostream & operator << (ostream & os, const vector<int> & v)
{
	os << "[";
	for (size_t i = 0; i < v.size(); i++)
	{
		os << v[i];
		if (i != (v.size() - 1)) os << ", ";
	}
	os << "]";
	return os;
}

// Overloading << operator of SINode for debug purpose.
ostream & operator << (ostream & os, const SINode & node)
{
	os << "ID: " << node.id << endl;
	os << "Label: " << node.label << endl;
	os << "Neighbors: " << node.nbs << endl;
	os << "Backward neighbors: " << node.b_nbs << endl;
	return os;
}

//===========================================================

class SIQuery:public Query<SINode>
{
public:
	int root;
	hash_map<int, SINode> nodes;

	virtual void addNode(char* line)
	{
		char * pch;

		pch = strtok(line, "\t");
		//bool isQuery = (*pch == 'Q');

		pch = strtok(NULL, " ");
		int id = atoi(pch);

		pch = strtok(NULL, " ");
		int label = atoi(pch);

		this->nodes[id] = SINode(id, label);

		pch = strtok(NULL, " ");
		int num = atoi(pch);
		for (int k = 0; k < num; k++)
		{
			pch=strtok(NULL, " ");
			int neighbor = atoi(pch);
			if (id > neighbor)
			{
				this->nodes[id].add_edge(neighbor);
				this->nodes[neighbor].add_edge(id);
			}
		}
		// cout << "Node " << id << " Label " << label << endl;
	}

	virtual void printOrder()
	{
		SINode* curr = &this->nodes[this->root];
		while (curr)
		{
			cout << "Level " << curr->level << endl;
			cout << *curr << "It has " << curr->children.size() << " children." << endl;
			if (curr->children.empty())
				break;
			else
			{
				cout << "The first of it is: " << endl;
				curr = &this->nodes[curr->children[0]];
			}
		}
	}

	void dfs(int currID, int parentID, bool isRoot)
	{
		// recursive function to implement depth-first search.
		// only called when current node is not visited.
		SINode* curr = &this->nodes[currID];
		curr->visited = true;
		if (isRoot)
			curr->level = 0;
		else
		{
			curr->parent = parentID;
			SINode* parent = & this->nodes[parentID];
			parent->children.push_back(currID);
			curr->level = parent->level + 1;
		}

		// we must have two loops to avoid descendants being visited
		// by other descendants.
		for (vector<int>::iterator it = curr->nbs.begin();
				it != curr->nbs.end(); it++)
			if (this->nodes[*it].visited)
				curr->b_nbs.push_back(*it);
		for (vector<int>::iterator it = curr->nbs.begin();
				it != curr->nbs.end(); it++)
			if (! this->nodes[*it].visited)
				this->dfs(*it, currID, false);
	}

	int getLabel(int id)
	{
		return this->nodes[id].label;
	}

	int getLevel(int id)
	{
		return this->nodes[id].level;
	}

	int getChildrenNumber(int id)
	{
		return (int) this->nodes[id].children.size();
	}

	int getChildID(int id, int index)
	{
		return this->nodes[id].children[index];
	}

	vector<int> getBNeighbors(int id)
	{
		return this->nodes[id].b_nbs;
	}

};

ibinstream & operator<<(ibinstream & m, const SIQuery & q){
	m << q.root << q.nodes;
	return m;
}

obinstream & operator>>(obinstream & m, SIQuery & q){
	m >> q.root >> q.nodes;
	return m;
}

//===============================================================

class SIVertex:public Vertex<SIKey, SIValue, SIMessage, SIKeyHash>
{
	public:
		vector<vector<VertexID>> results; // only used in the final step

		void continue_mapping(vector<VertexID> &mapping, int &curr_u)
		{ // Add current vertex to mapping;
		  // Send messages to neighbors with label of next_u(next query vertex).
			hash_map<VertexID, int> & nbs = value().neighbors;
			hash_map<VertexID, int>::iterator it;
			SIQuery* query = (SIQuery*)getQuery();

			mapping.push_back(id.vID);
#ifdef DEBUG_MODE
			cout << "[Result] Partial mapping:  " << mapping << endl;
#endif

			int children_num = query->getChildrenNumber(curr_u);
			if (children_num == 0)
			{ // leaf query vertex
				results.push_back(mapping);
#ifdef DEBUG_MODE
				cout << "[Result] Final mapping:  " << mapping << endl;
#endif
			}
			else
			{
				int next_u;
				for (int i = 0; i < children_num; i++)
				{
					next_u = query->getChildID(curr_u, i);
					SIMessage msg = SIMessage(MAPPING, mapping, next_u);
					for (it = nbs.begin(); it != nbs.end(); it++)
					{
						if (it->second == query->getLabel(next_u))
						{
							send_message(SIKey(it->first, false), msg);
#ifdef DEBUG_MODE_MSG
						cout << "[DEBUG] Message sent from " << id.vID << " to "
							 << it->first << ". \n\t"
							 << "Type: MAPPING. \n\t"
							 << "Mapping: " << msg.mapping << endl;
#endif
						}
					}
				}
			}
		}

		virtual void compute(MessageContainer & messages)
		{
			hash_map<VertexID, int> & nbs = value().neighbors;
			hash_map<VertexID, int>::iterator it;
			SIQuery* query = (SIQuery*)getQuery();

#ifdef DEBUG_MODE_ACTIVE
				cout << "[DEBUG] STEP NUMBER " << step_num()
					 << " ACTIVE Vertex ID " << id.vID << endl;
#endif
			if (step_num() == 1)
			{ // send label info to neighbors
				SIMessage msg = SIMessage(LABEL_INFOMATION, id.vID,
						value().label);
				for (it = nbs.begin(); it != nbs.end(); it++)
					send_message(SIKey(it->first, false), msg);
			}
			else if (step_num() == 2)
			{   // receive label info from neighbors
				for (size_t i = 0; i < messages.size(); i++)
				{
					SIMessage & msg = messages[i];
					if (msg.type == LABEL_INFOMATION)
						nbs[msg.vertex] = msg.label;
				}

				// start mapping with vertices with same label
				int root_u = query->root;
				int root_label = query->getLabel(root_u);
				if (value().label == root_label)
				{
					vector<VertexID> mapping;
					continue_mapping(mapping, root_u);
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
					b_nbs = query->getBNeighbors(msg.vertex);
					curr_level = query->getLevel(msg.vertex);
					flag = true;
					for (size_t i = 0; i < b_nbs.size(); i++)
					{
						nb_level = query->getLevel(b_nbs[i]);
						if ((nb_level != curr_level - 1) &&
								(nbs.find(msg.mapping[nb_level]) == nbs.end()))
						{
							flag = false;
							break;
						}
					}
					if (flag) continue_mapping(msg.mapping, msg.vertex);
				}
				vote_to_halt();
			}
		}
};

/*====================================
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
*/

//=============================================================================

class SIWorker:public Worker<SIVertex, SIQuery>
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
			for (size_t i = 0; i < results.size(); i++)
			{
				vector<VertexID> & mapping = results[i];
				sprintf(buf, "# Match\n");
				writer.write(buf);

				for (size_t j = 0; j < mapping.size(); j++)
				{
					sprintf(buf, "%d %d\n", (int)(j+1), mapping[j]);
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

void pregel_subgraph(string data_path, string query_path, string out_path,
		bool force_write)
{
	SIWorker worker;
	//CCCombiner_pregel combiner;
	//if(use_combiner) worker.setCombiner(&combiner);

	//SIAgg agg;
	//worker.setAggregator(&agg);

	SIQuery query;
	worker.setQuery(&query);

	worker.load_data(data_path);
	worker.load_query(query_path);
	worker.run_compute();
	worker.dump_graph(out_path, force_write);
}
