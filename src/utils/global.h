#ifndef GLOBAL_H
#define GLOBAL_H

#include <mpi.h>
#include <stddef.h>
#include <limits.h>
#include <string>
#include <map>
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
typedef int uID;

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
    ENUMERATE = 2,
    FILTER = 3
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

template <class T>
bool notContains(vector<T> & v, T x)
{
	size_t size = v.size();
	for (size_t i = 0; i < size; i++)
	{
		if (v[i] == x) return false;
	}
	return true;
}

template <class T>
bool notContainsDuplicate(vector<T> & v)
{
	hash_set<T> s = hash_set<T>(v.begin(), v.end());
	return (s.size() == v.size());
}


//========================================================================

/* OptionKeyword:
    DataPath = 0,           // -d, the data file path (folder)
    QueryPath = 1,     		// -q, the query file path (folder)
    OutputPath = 2,      	// -out, output path (folder)
    Partition = 3,        	// -partition, the strategy of partitioning
    Preprocess = 4,     	// -preprocess, the strategy of preprocessing
    Filter = 5,             // -filter, the strategy of filtering
    Strategy = 6,           // -strategy, matching by path or by tree
    Order = 7,             	// -order, the priority in deciding match order
    Enumeration = 8, 		// -enumeration, the strategy of enumeration
    Report = 9,				// -report, detailed report or concise report
    Input = 10				// -input, default or g-thinker
*/

#define OPTIONS 11

class MatchingCommand{
    vector<string> tokens;
    vector<string> options_key;
    vector<string> options_value;

    const string getCommandOption(const string &option) const
    {
        vector<string>::const_iterator itr;
        itr = find(tokens.begin(), tokens.end(), option);
        if (itr != tokens.end() && ++itr != tokens.end())
            return *itr;
        return "";
    }

    void processOptions()
    {
        for (int i = 0; i < OPTIONS; i++)
        	options_value.push_back(getCommandOption(options_key[i]));
    }

public:
    MatchingCommand(const int argc, char **argv)
    {
    	options_key = {"-d", "-q", "-out", "-partition", "-preprocess",
    			"-filter", "-strategy", "-order", "-enumeration",
    			"-report", "-input"};
    	for (int i = 1; i < argc; ++i)
            tokens.push_back(std::string(argv[i]));
        processOptions();
    }

    string getDataPath() { return options_value[0]; }
    string getQueryPath() { return options_value[1]; }
    string getOutputPath() { return options_value[2]; }

    string getPartition() { return options_value[3]; }
    bool getFilterMethod() {
    	return (options_value[5] != "off");
    }
    string getOrderMethod() { return options_value[7]; }
    bool getEnumerateMethod() {
    	return (options_value[8] != "old");
    }
    bool getReport() {
    	return (options_value[9] != "long");
    }
    bool getInputFormat() {
    	return (options_value[10] != "g-thinker");
    }
};

//------------------------
// worker parameters

struct WorkerParams {
    string data_path;
    string query_path;
    string output_path;
    bool force_write;

    string partition;
    bool filter;
    string order;
    bool enumerate;
    bool report;
    bool input;

    WorkerParams()
    {
        force_write = true;
    }

    WorkerParams(MatchingCommand &command, bool fw)
    {
        data_path = command.getDataPath();
        query_path = command.getQueryPath();
        output_path = command.getOutputPath();
        force_write = fw;
        partition = command.getPartition();
        filter = command.getFilterMethod();
        order = command.getOrderMethod();
        enumerate = command.getEnumerateMethod();
        report = command.getReport();
        input = command.getInputFormat();
    }
};

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


