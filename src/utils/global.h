#ifndef GLOBAL_H
#define GLOBAL_H

#include <mpi.h>
#include <stddef.h>
#include <limits.h>
#include <string>
#include <ext/hash_set>
#include <ext/hash_map>
#define hash_map __gnu_cxx::hash_map
#define hash_set __gnu_cxx::hash_set
#include <assert.h> //for ease of debug
using namespace std;

//============================
///worker info
#define MASTER_RANK 0

int _my_rank;
int _num_workers;
inline int get_worker_id()
{
    return _my_rank;
}
inline int get_num_workers()
{
    return _num_workers;
}

void init_workers()
{
    MPI_Init(NULL, NULL);
    MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
    MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
}

void worker_finalize()
{
    MPI_Finalize();
}

void worker_barrier()
{
    MPI_Barrier(MPI_COMM_WORLD);
}

//------------------------
// worker parameters

struct WorkerParams {
    string input_path;
    string output_path;
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    WorkerParams()
    {
        force_write = true;
        native_dispatcher = false;
    }
};

struct MultiInputParams {
    vector<string> input_paths;
    string output_path;
    bool force_write;
    bool native_dispatcher; //true if input is the output of a previous blogel job

    MultiInputParams()
    {
        force_write = true;
        native_dispatcher = false;
    }

    void add_input_path(string path)
    {
        input_paths.push_back(path);
    }
};

//============================
//general types
typedef int VertexID;

//============================
//global variables (original)
int global_step_num;
inline int step_num()
{
    return global_step_num;
}

int global_phase_num;
inline int phase_num()
{
    return global_phase_num;
}

void* global_message_buffer = NULL;
inline void set_message_buffer(void* mb)
{
    global_message_buffer = mb;
}
inline void* get_message_buffer()
{
    return global_message_buffer;
}

void* global_combiner = NULL;
inline void set_combiner(void* cb)
{
    global_combiner = cb;
}
inline void* get_combiner()
{
    return global_combiner;
}

void* global_aggregator = NULL;
inline void set_aggregator(void* ag)
{
    global_aggregator = ag;
}
inline void* get_aggregator()
{
    return global_aggregator;
}

void* global_agg = NULL; //for aggregator, FinalT of last round
inline void* getAgg()
{
    return global_agg;
}

int global_vnum = 0;
inline int& get_vnum()
{
    return global_vnum;
}
int global_active_vnum = 0;
inline int& active_vnum()
{
    return global_active_vnum;
}

enum COMPUTE_TYPES {
    PREPROCESS = 0,
    MATCH = 1,
    ENUMERATE = 2
};

enum BITS {
    HAS_MSG_ORBIT = 0,
    FORCE_TERMINATE_ORBIT = 1,
    WAKE_ALL_ORBIT = 2
};
//currently, only 3 bits are used, others can be defined by users
char global_bor_bitmap;

void clearBits()
{
    global_bor_bitmap = 0;
}

void setBit(int bit)
{
    global_bor_bitmap |= (2 << bit);
}

int getBit(int bit, char bitmap)
{
    return ((bitmap & (2 << bit)) == 0) ? 0 : 1;
}

void hasMsg()
{
    setBit(HAS_MSG_ORBIT);
}

void wakeAll()
{
    setBit(WAKE_ALL_ORBIT);
}

void forceTerminate()
{
    setBit(FORCE_TERMINATE_ORBIT);
}

//====================================================
//Set up a pointer to query in global.h
//so that it can be used anywhere in the program

void* global_query = NULL;

inline void* getQuery()
{
    return global_query;
}

//====================================================
// Self-defined function

bool notContains(vector<int> & v, int x)
{
	for (vector<int>::iterator it = v.begin(); it != v.end(); it++)
	{
		if (*it == x) return false;
	}
	return true;
}

bool notContainsDuplicate(vector<int> & v)
{
	hash_set<int> s = hash_set<int>(v.begin(), v.end());
	return (s.size() == v.size());
}

vector<vector<VertexID>> joinVectors(vector<VertexID> & head_v,
		vector<vector<VertexID> > & v1,
		vector<vector<VertexID> > & v2)
{
	// recursive function to join vectors
	vector<vector<VertexID>> results;
	vector<VertexID> v;
	for (size_t i = 0; i < v1.size(); i++)
	{
		for (size_t j = 0; j < v2.size(); j++)
		{
			v = head_v;
			v.insert(v.end(), v1[i].begin(), v1[i].end());
			v.insert(v.end(), v2[j].begin(), v2[j].end());
			if (notContainsDuplicate(v))
				results.push_back(v);
		}
	}

	return results;
}

//====================================================
//Ghost threshold
int global_ghost_threshold;

void set_ghost_threshold(int tau)
{
    global_ghost_threshold = tau;
}

//====================================================
#define ROUND 11 //for PageRank

#endif
