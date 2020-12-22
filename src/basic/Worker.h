#ifndef WORKER_H
#define WORKER_H

#include <vector>
#include "../utils/global.h"
#include "MessageBuffer.h"
#include <string>
#include "../utils/communication.h"
#include "../utils/ydhdfs.h"
#include "../utils/Combiner.h"
#include "../utils/Aggregator.h"
#include "../utils/Query.h"
using namespace std;

template <class VertexT, class QueryT, class AggregatorT = DummyAgg> //user-defined VertexT
class Worker {
    typedef vector<VertexT*> VertexContainer;
    typedef typename VertexContainer::iterator VertexIter;

    typedef typename VertexT::KeyType KeyT;
    typedef typename VertexT::MessageType MessageT;
    typedef typename VertexT::HashType HashT;

    typedef MessageBuffer<VertexT> MessageBufT;
    typedef typename MessageBufT::MessageContainerT MessageContainerT;
    typedef typename MessageBufT::Map Map;
    typedef typename MessageBufT::MapIter MapIter;

    typedef typename AggregatorT::PartialType PartialT;
    typedef typename AggregatorT::FinalType FinalT;

public:
    Worker()
    {
        //init_workers();//put to run.cpp
        message_buffer = new MessageBuffer<VertexT>;
        global_message_buffer = message_buffer;
        active_count = 0;
        combiner = NULL;
        global_combiner = NULL;
        aggregator = NULL;
        global_aggregator = NULL;
        global_agg = NULL;
    }

    void setCombiner(Combiner<MessageT>* cb)
    {
        combiner = cb;
        global_combiner = cb;
    }

    void setAggregator(AggregatorT* ag)
    {
        aggregator = ag;
        global_aggregator = ag;
        global_agg = new FinalT;
    }

    void setQuery(QueryT* qr)
    {
        global_query = qr;
    }

    virtual ~Worker()
    {
        for (size_t i = 0; i < vertexes.size(); i++)
            delete vertexes[i];
        delete message_buffer;
        if (getAgg() != NULL)
            delete (FinalT*)global_agg;
        //worker_finalize();//put to run.cpp
        worker_barrier(); //newly added for ease of multi-job programming in run.cpp
    }

    //==================================
    //sub-functions
    void sync_graph()
    {
        //ResetTimer(4);
        //set send buffer
        vector<VertexContainer> _loaded_parts(_num_workers);
        for (size_t i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            _loaded_parts[hash(v->id)].push_back(v);
        }
        //exchange vertices to add
        all_to_all(_loaded_parts);

        //delete sent vertices
        for (size_t i = 0; i < vertexes.size(); i++) {
            VertexT* v = vertexes[i];
            if (hash(v->id) != _my_rank)
                delete v;
        }
        vertexes.clear();
        //collect vertices to add
        for (int i = 0; i < _num_workers; i++) {
            vertexes.insert(vertexes.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
        }
        _loaded_parts.clear();
        //StopTimer(4);
        //PrintTimer("Reduce Time",4);
    };

    //old implementation
    /*
		void active_compute()
		{
			active_count=0;
			MessageBufT* mbuf=(MessageBufT*)get_message_buffer();
			Map & msgs=mbuf->get_messages();
			MessageContainerT empty;
			for(VertexIter it=vertexes.begin(); it!=vertexes.end(); it++)
			{
				KeyT vid=(*it)->id;
				MapIter mit=msgs.find(vid);
				if(mit->second->size()==0)
				{
					if((*it)->is_active())
					{
						(*it)->compute(empty);
						AggregatorT* agg=(AggregatorT*)get_aggregator();
						if(agg!=NULL) agg->stepPartial(*it);
						if((*it)->is_active()) active_count++;
					}
				}
				else
				{
					(*it)->activate();
					(*it)->compute(*(mit->second));
					mit->second->clear();//clear used msgs
					AggregatorT* agg=(AggregatorT*)get_aggregator();
					if(agg!=NULL) agg->stepPartial(*it);
					if((*it)->is_active()) active_count++;
				}
			}
		}

		void all_compute()
		{
			active_count=0;
			MessageBufT* mbuf=(MessageBufT*)get_message_buffer();
			Map & msgs=mbuf->get_messages();
			MessageContainerT empty;
			for(VertexIter it=vertexes.begin(); it!=vertexes.end(); it++)
			{
				KeyT vid=(*it)->id;
				MapIter mit=msgs.find(vid);
				(*it)->activate();
				if(mit->second->size()==0) (*it)->compute(empty);
				else{
					(*it)->compute(*(mit->second));
					mit->second->clear();//clear used msgs
				}
				AggregatorT* agg=(AggregatorT*)get_aggregator();
				if(agg!=NULL) agg->stepPartial(*it);
				if((*it)->is_active()) active_count++;
			}
		}
		*/

    void active_compute(int type, WorkerParams params, int wakeAll)
    {
        active_count = 0;
        MessageBufT* mbuf = (MessageBufT*)get_message_buffer();
        vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
        //AggregatorT* agg=(AggregatorT*)get_aggregator();
        for (size_t i = 0; i < vertexes.size(); i++) {
        	if (wakeAll == 1) vertexes[i]->activate();

            if  (
            	(vertexes[i]->is_active() && v_msgbufs[i].size() == 0)
            	||
            	(v_msgbufs[i].size() != 0)
            	)
            {
                switch (type)
				{
				case PREPROCESS:
                    vertexes[i]->preprocess(v_msgbufs[i], params);
					break;
                case FILTER:
                	vertexes[i]->filter(v_msgbufs[i]);
					break;
				case MATCH:
					vertexes[i]->compute(v_msgbufs[i], params);
					break;
				case ENUMERATE:
					vertexes[i]->enumerate(v_msgbufs[i]);
					break;
				}
                //clear used msgs
                v_msgbufs[i].clear();
                
				if (vertexes[i]->is_active())
					active_count++;
            }
        }
    }

    inline void add_vertex(VertexT* vertex)
    {
        vertexes.push_back(vertex);
        if (vertex->is_active())
            active_count++;
    }

    void agg_sync()
    {
        AggregatorT* agg = (AggregatorT*)get_aggregator();
        if (agg != NULL) {
            if (_my_rank != MASTER_RANK) { //send partialT to aggregator
                //gathering PartialT
                PartialT* part = agg->finishPartial();
                //------------------------ strategy choosing BEGIN ------------------------
                StartTimer(COMMUNICATION_TIMER);
                StartTimer(SERIALIZATION_TIMER);
                ibinstream m;
                m << part;
                int sendcount = m.size();
                StopTimer(SERIALIZATION_TIMER);
                int total = all_sum(sendcount);
                StopTimer(COMMUNICATION_TIMER);
                //------------------------ strategy choosing END ------------------------
                if (total <= AGGSWITCH)
                    slaveGather(*part);
                else {
                    send_ibinstream(m, MASTER_RANK);
                }
                //scattering FinalT
                slaveBcast(*((FinalT*)global_agg));
            } else {
                //------------------------ strategy choosing BEGIN ------------------------
                int total = all_sum(0);
                //------------------------ strategy choosing END ------------------------
                //gathering PartialT
                if (total <= AGGSWITCH) {
                    vector<PartialT*> parts(_num_workers);
                    masterGather(parts);
                    for (int i = 0; i < _num_workers; i++) {
                        if (i != MASTER_RANK) {
                            PartialT* part = parts[i];
                            agg->stepFinal(part);
                            delete part;
                        }
                    }
                } else {
                    for (int i = 0; i < _num_workers; i++) {
                        if (i != MASTER_RANK) {
                            obinstream um = recv_obinstream(i);
                            PartialT* part;
                            um >> part;
                            agg->stepFinal(part);
                            delete part;
                        }
                    }
                }
                //scattering FinalT
                FinalT* final = agg->finishFinal();
                //cannot set "global_agg=final" since MASTER_RANK works as a slave, and agg->finishFinal() may change
                *((FinalT*)global_agg) = *final; //deep copy
                masterBcast(*((FinalT*)global_agg));
            }
        }
    }

    //user-defined graphLoader ==============================
    virtual VertexT* toVertex(char* line, const WorkerParams & params) = 0;

    void load_vertex(VertexT* v)
    { //called by load_graph
        if (v != NULL)
            add_vertex(v);
    }

    void load_graph(const char* inpath, const WorkerParams & params)
    {
        hdfsFS fs = getHdfsFS();
        hdfsFile in = getRHandle(inpath, fs);
        LineReader reader(fs, in);
        while (true) {
            reader.readLine();
            if (!reader.eof())
                load_vertex(toVertex(reader.getLine(), params));
            else
                break;
        }
        hdfsCloseFile(fs, in);
        hdfsDisconnect(fs);
        //cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
    }

    void load_query_graph(const char* inpath)
	{
		hdfsFS fs = getHdfsFS();
		hdfsFile in = getRHandle(inpath, fs);
		LineReader reader(fs, in);
		while (true) {
			reader.readLine();
			if (!reader.eof())
				((QueryT*) global_query)->addNode(reader.getLine());
			else
				break;
		}
		hdfsCloseFile(fs, in);
		hdfsDisconnect(fs);
		cout<<"Worker "<<_my_rank<<": \""<<inpath<<"\" loaded"<<endl;//DEBUG !!!!!!!!!!
	}
    //=======================================================

    //user-defined graphDumper ==============================
    virtual void toline(VertexT* v, BufferedWriter& writer) = 0; //this is what user specifies!!!!!!

    //user-defined message clearer ==========================
    virtual void clear_messages(vector<MessageT> &delete_messages) = 0;

    void dump_partition(const char* outpath)
    {
        hdfsFS fs = getHdfsFS();
        BufferedWriter* writer = new BufferedWriter(outpath, fs, _my_rank);

        for (VertexIter it = vertexes.begin(); it != vertexes.end(); it++) {
            writer->check();
            toline(*it, *writer);
        }
        delete writer;
        hdfsDisconnect(fs);
    }
    //=======================================================

    // run the worker, load the data graph
    void load_data(const WorkerParams & params)
    {
    	const string& input_path = params.data_path;
        
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(input_path.c_str()) == -1)
            {
                cout << "Input path doesn't exist." << endl;
                exit(-1);
            }
        }

        //dispatch splits
        vector<vector<string> >* arrangement;
        if (_my_rank == MASTER_RANK) {
            arrangement = dispatchRan(input_path.c_str());
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str(), params);
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str(), params);
        }

        // reassigning worker id
        if (params.partition == "complete")
        {
            // insert code here
        }

        //send vertices according to hash_id (reduce)
        sync_graph();

        message_buffer->init(vertexes);
        //barrier for data loading
        worker_barrier(); //@@@@@@@@@@@@@
    }

    //====================================================================

    // load the query graph by MASTER and broadcast to SLAVEs, return load time
    void load_query(const string& input_path)
	{
		//check path + init
    	if (_my_rank == MASTER_RANK) {
			if (dirCheck(input_path.c_str()) == -1)
				exit(-1);
		}
		
		if (_my_rank == MASTER_RANK)
		{
			// read query from HDFS
			vector<string> files;
			dispatchMaster(input_path.c_str(), files);
			for (vector<string>::iterator it = files.begin();
			                 it != files.end(); it++)
			{
				load_query_graph(it->c_str());
			}
			masterBcast(*((QueryT*) global_query));
		}
		else
		{
			slaveBcast(*((QueryT*) global_query));
		}

		//debug
		cout << "------------Debug Worker " << _my_rank << "-------------" << endl;
		((QueryT*) global_query)->printOrder();

		//barrier for query loading
		worker_barrier();
	}

    //====================================================================

    void build_query_tree(const string &order)
	{
    	QueryT* query = (QueryT*) global_query;
		query->init(order);

		//debug
		//cout << "------------Debug Worker " << _my_rank << "-------------" << endl;
        //((QueryT*) global_query)->printOrder();
		if (_my_rank == MASTER_RANK)
			((QueryT*) global_query)->printOrder();

		//barrier for query tree build
		worker_barrier(); 
	}

    //=========================================================

    // run preprocess, match or enumerate, return compute time
    void run_type(int type, const WorkerParams & params)
    {
        ResetTimer(WORKER_TIMER);
        InitTimer(ACTIVE_COMPUTE_TIMER);
        InitTimer(REDUCE_MESSAGE_TIMER);
        InitTimer(SYNC_MESSAGE_TIMER);
        InitTimer(SYNC_TIMER);
        InitTimer(COMMUNICATION_TIMER);
        InitTimer(SERIALIZATION_TIMER);
        InitTimer(TRANSFER_TIMER);

        //supersteps
        global_step_num = 0;
        long long step_msg_num;
        long long step_vadd_num;
        long long global_msg_num = 0;
        long long global_vadd_num = 0;
        AggregatorT* agg = (AggregatorT*)get_aggregator();
        agg->init(type);

        vector<MessageT> delete_messages;
        
        while (true) 
        {
            global_step_num++;
            ResetTimer(SUPERSTEP_TIMER);

            // stopping criteria for MATCH and ENUMRATE
            char bits_bor = all_bor(global_bor_bitmap);
            if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                break;
            get_vnum() = all_sum(vertexes.size());
            int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
            if (wakeAll == 0) {
                active_vnum() = all_sum(active_count);
                if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                    break; //all_halt AND no_msg, note that received msgs are not freed
            } else
                active_vnum() = get_vnum();
            clearBits();
            
            StartTimer(ACTIVE_COMPUTE_TIMER);
            active_compute(type, params, wakeAll);
            StopTimer(ACTIVE_COMPUTE_TIMER);
            
            StartTimer(REDUCE_MESSAGE_TIMER);
            //message_buffer->combine();
            step_msg_num = master_sum_LL(message_buffer->get_total_msg());
            step_vadd_num = master_sum_LL(message_buffer->get_total_vadd());
            if (_my_rank == MASTER_RANK) {
                global_msg_num += step_msg_num;
                global_vadd_num += step_vadd_num;
            }
            StopTimer(REDUCE_MESSAGE_TIMER);
            
            StartTimer(SYNC_MESSAGE_TIMER);
            vector<vector<msgpair<MessageT>>> &out_messages = 
                message_buffer->out_messages.getBufs();

            //Messages sent to other machines: memory can be freed once they are sent
            for (int wID = 0; wID < out_messages.size(); wID++)
                if (wID != get_worker_id())
                    for (int j = 0; j < out_messages[wID].size(); j++)
                        delete_messages.push_back(out_messages[wID][j].msg);
            
            //Sync Messages. After this, received msgs will no longer be used
            vector<VertexT*>& to_add = message_buffer->sync_messages();

            //Free memory (received msgs in the last step + sent msgs in this step) 
            //unless for the final step
            clear_messages(delete_messages);

            //Distribute received msgs to each vertex
            message_buffer->distribute_messages(delete_messages);
            StopTimer(SYNC_MESSAGE_TIMER);

            for (size_t i = 0; i < to_add.size(); i++)
                add_vertex(to_add[i]);
            to_add.clear();

            //===================
            StartTimer(SYNC_TIMER);
            worker_barrier();
            StopTimer(SYNC_TIMER);
            StopTimer(SUPERSTEP_TIMER);
            if (_my_rank == MASTER_RANK && !params.report && type == MATCH) {
                cout << "Superstep " << global_step_num << " done."
                	 << "Time elapsed: " << get_timer(SUPERSTEP_TIMER) << " seconds" << endl;
                cout << "#msgs: " << step_msg_num << ", #vadd: " << step_vadd_num << endl;
            }
        } // end of while loop
        ResetTimer(AGG_TIMER);
        for (size_t i = 0; i < vertexes.size(); i++)
        {
			agg->stepPartial(vertexes[i], type);
        }
        agg_sync();
        StopTimer(AGG_TIMER);

        StartTimer(SYNC_TIMER);
        worker_barrier();
        StopTimer(SYNC_TIMER);

        StopTimer(WORKER_TIMER);
        if (_my_rank == MASTER_RANK && !params.report && type == MATCH)
    	{
            cout << "Subgraph matching done. " << endl;
            PrintTimer("Total Computational Time", WORKER_TIMER);
            PrintTimer(" - Active Compute Time", ACTIVE_COMPUTE_TIMER);
            PrintTimer(" - Sync Message Time", SYNC_MESSAGE_TIMER);
            PrintTimer(" - Agg Time", AGG_TIMER);
            PrintTimer(" - Sync Time (load imbalance)", SYNC_TIMER);
            PrintTimer(" - Communication Time", COMMUNICATION_TIMER);
            PrintTimer("    - Serialization Time", SERIALIZATION_TIMER);
            PrintTimer("    - Transfer Time", TRANSFER_TIMER);
            cout << "Total #msgs=" << global_msg_num << ", "
                "Total #vadd=" << global_vadd_num << endl;
    	}
    }

    void dump_graph(const string& output_path, bool force_write)
    {
    	//check path + init
        if (_my_rank == MASTER_RANK) {
            cout << "output path: " << output_path << endl;
            if (dirCheck(output_path.c_str(), force_write) == -1)
                exit(-1);
        }
        dump_partition(output_path.c_str());
    }

    /* original run
    void run(const WorkerParams& params)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
                exit(-1);
        }
        init_timers();

        //dispatch splits
        ResetTimer(WORKER_TIMER);
        vector<vector<string> >* arrangement;
        if (_my_rank == MASTER_RANK) {
            arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
        }

        //send vertices according to hash_id (reduce)
        sync_graph();

        message_buffer->init(vertexes);
        //barrier for data loading
        worker_barrier(); //@@@@@@@@@@@@@
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        init_timers();
        ResetTimer(WORKER_TIMER);
        //supersteps
        global_step_num = 0;
        long long step_msg_num;
        long long step_vadd_num;
        long long global_msg_num = 0;
        long long global_vadd_num = 0;
        while (true) {
            global_step_num++;
            ResetTimer(4);
            //===================
            char bits_bor = all_bor(global_bor_bitmap);
            if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                break;
            get_vnum() = all_sum(vertexes.size());
            int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
            if (wakeAll == 0) {
                active_vnum() = all_sum(active_count);
                if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                    break; //all_halt AND no_msg
            } else
                active_vnum() = get_vnum();
            //===================
            AggregatorT* agg = (AggregatorT*)get_aggregator();
            if (agg != NULL)
                agg->init();
            //===================
            clearBits();
            if (wakeAll == 1)
                all_compute();
            else
                active_compute();
            message_buffer->combine();
            step_msg_num = master_sum_LL(message_buffer->get_total_msg());
            step_vadd_num = master_sum_LL(message_buffer->get_total_vadd());
            if (_my_rank == MASTER_RANK) {
                global_msg_num += step_msg_num;
                global_vadd_num += step_vadd_num;
            }
            vector<VertexT*>& to_add = message_buffer->sync_messages();
            agg_sync();
            for (int i = 0; i < to_add.size(); i++)
                add_vertex(to_add[i]);
            to_add.clear();
            //===================
            worker_barrier();
            StopTimer(4);
            if (_my_rank == MASTER_RANK) {
                cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                cout << "#msgs: " << step_msg_num << ", #vadd: " << step_vadd_num << endl;
            }
        }
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        if (_my_rank == MASTER_RANK)
            cout << "Total #msgs=" << global_msg_num << ", Total #vadd=" << global_vadd_num << endl;

        // dump graph
        ResetTimer(WORKER_TIMER);
        dump_partition(params.output_path.c_str());
        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }

    //run the worker
    void run(const WorkerParams& params, int num_phases)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
                exit(-1);
        }
        init_timers();

        //dispatch splits
        ResetTimer(WORKER_TIMER);
        vector<vector<string> >* arrangement;
        if (_my_rank == MASTER_RANK) {
            arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
        }

        //send vertices according to hash_id (reduce)
        sync_graph();
        message_buffer->init(vertexes);
        //barrier for data loading
        worker_barrier(); //@@@@@@@@@@@@@
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        //=========================================================

        init_timers();
        ResetTimer(WORKER_TIMER);

        for (global_phase_num = 1; global_phase_num <= num_phases; global_phase_num++) {
            if (_my_rank == MASTER_RANK)
                cout << "################ Phase " << global_phase_num << " ################" << endl;

            //supersteps
            global_step_num = 0;
            long long step_msg_num;
            long long step_vadd_num;
            long long global_msg_num = 0;
            long long global_vadd_num = 0;

            while (true) {
                global_step_num++;
                ResetTimer(4);
                //===================
                if (step_num() == 1) {
                    get_vnum() = all_sum(vertexes.size());
                    if (phase_num() > 1)
                        active_vnum() = get_vnum();
                    else
                        active_vnum() = all_sum(active_count);
                    //===================
                    AggregatorT* agg = (AggregatorT*)get_aggregator();
                    if (agg != NULL)
                        agg->init();
                    //===================
                    clearBits();
                    if (phase_num() > 1)
                        all_compute();
                    else
                        active_compute();
                    message_buffer->combine();
                    step_msg_num = master_sum_LL(message_buffer->get_total_msg());
                    step_vadd_num = master_sum_LL(message_buffer->get_total_vadd());
                    if (_my_rank == MASTER_RANK) {
                        global_msg_num += step_msg_num;
                        global_vadd_num += step_vadd_num;
                    }
                    vector<VertexT*>& to_add = message_buffer->sync_messages();
                    agg_sync();
                    for (int i = 0; i < to_add.size(); i++)
                        add_vertex(to_add[i]);
                    to_add.clear();
                } else {
                    char bits_bor = all_bor(global_bor_bitmap);
                    if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                        break;
                    get_vnum() = all_sum(vertexes.size());
                    int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
                    if (wakeAll == 0) {
                        active_vnum() = all_sum(active_count);
                        if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                            break; //all_halt AND no_msg
                    } else
                        active_vnum() = get_vnum();
                    //===================
                    AggregatorT* agg = (AggregatorT*)get_aggregator();
                    if (agg != NULL)
                        agg->init();
                    //===================
                    clearBits();
                    if (wakeAll == 1)
                        all_compute();
                    else if (phase_num() > 1 && step_num() == 1)
                        all_compute();
                    else
                        active_compute();
                    message_buffer->combine();
                    step_msg_num = master_sum_LL(message_buffer->get_total_msg());
                    step_vadd_num = master_sum_LL(message_buffer->get_total_vadd());
                    if (_my_rank == MASTER_RANK) {
                        global_msg_num += step_msg_num;
                        global_vadd_num += step_vadd_num;
                    }
                    vector<VertexT*>& to_add = message_buffer->sync_messages();
                    agg_sync();
                    for (int i = 0; i < to_add.size(); i++)
                        add_vertex(to_add[i]);
                    to_add.clear();
                }
                //===================
                worker_barrier();
                StopTimer(4);
                if (_my_rank == MASTER_RANK) {
                    cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                    cout << "#msgs: " << step_msg_num << ", #vadd: " << step_vadd_num << endl;
                }
            }
            if (_my_rank == MASTER_RANK) {
                cout << "************ Phase " << global_phase_num << " done. ************" << endl;
                cout << "Total #msgs=" << global_msg_num << ", Total #vadd=" << global_vadd_num << endl;
            }
        }
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);

        // dump graph
        ResetTimer(WORKER_TIMER);
        dump_partition(params.output_path.c_str());
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }

    // run the worker
    void run(const MultiInputParams& params)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(params.input_paths, params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
                exit(-1);
        }
        init_timers();

        //dispatch splits
        ResetTimer(WORKER_TIMER);
        vector<vector<string> >* arrangement;
        if (_my_rank == MASTER_RANK) {
            arrangement = params.native_dispatcher ? dispatchLocality(params.input_paths) : dispatchRan(params.input_paths);
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
        }

        //send vertices according to hash_id (reduce)
        sync_graph();
        message_buffer->init(vertexes);
        //barrier for data loading
        worker_barrier(); //@@@@@@@@@@@@@
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        //=========================================================

        init_timers();
        ResetTimer(WORKER_TIMER);
        //supersteps
        global_step_num = 0;
        long long step_msg_num;
        long long step_vadd_num;
        long long global_msg_num = 0;
        long long global_vadd_num = 0;
        while (true) {
            global_step_num++;
            ResetTimer(4);
            //===================
            char bits_bor = all_bor(global_bor_bitmap);
            if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                break;
            get_vnum() = all_sum(vertexes.size());
            int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
            if (wakeAll == 0) {
                active_vnum() = all_sum(active_count);
                if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                    break; //all_halt AND no_msg
            } else
                active_vnum() = get_vnum();
            //===================
            AggregatorT* agg = (AggregatorT*)get_aggregator();
            if (agg != NULL)
                agg->init();
            //===================
            clearBits();
            if (wakeAll == 1)
                all_compute();
            else
                active_compute();
            message_buffer->combine();
            step_msg_num = master_sum_LL(message_buffer->get_total_msg());
            step_vadd_num = master_sum_LL(message_buffer->get_total_vadd());
            if (_my_rank == MASTER_RANK) {
                global_msg_num += step_msg_num;
                global_vadd_num += step_vadd_num;
            }
            vector<VertexT*>& to_add = message_buffer->sync_messages();
            agg_sync();
            for (int i = 0; i < to_add.size(); i++)
                add_vertex(to_add[i]);
            to_add.clear();
            //===================
            worker_barrier();
            StopTimer(4);
            if (_my_rank == MASTER_RANK) {
                cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                cout << "#msgs: " << step_msg_num << ", #vadd: " << step_vadd_num << endl;
            }
        }
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        if (_my_rank == MASTER_RANK)
            cout << "Total #msgs=" << global_msg_num << ", Total #vadd=" << global_vadd_num << endl;

        // dump graph
        ResetTimer(WORKER_TIMER);
        dump_partition(params.output_path.c_str());
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);
    }

    //========================== reports machine-level msg# ===============================
    void run_report(const WorkerParams& params, const string reportPath)
    {
        //check path + init
        if (_my_rank == MASTER_RANK) {
            if (dirCheck(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
                exit(-1);
        }
        init_timers();

        //dispatch splits
        ResetTimer(WORKER_TIMER);
        vector<vector<string> >* arrangement;
        if (_my_rank == MASTER_RANK) {
            arrangement = params.native_dispatcher ? dispatchLocality(params.input_path.c_str()) : dispatchRan(params.input_path.c_str());
            //reportAssignment(arrangement);//DEBUG !!!!!!!!!!
            masterScatter(*arrangement);
            vector<string>& assignedSplits = (*arrangement)[0];
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
            delete arrangement;
        } else {
            vector<string> assignedSplits;
            slaveScatter(assignedSplits);
            //reading assigned splits (map)
            for (vector<string>::iterator it = assignedSplits.begin();
                 it != assignedSplits.end(); it++)
                load_graph(it->c_str());
        }

        //send vertices according to hash_id (reduce)
        sync_graph();
        message_buffer->init(vertexes);
        //barrier for data loading
        worker_barrier(); //@@@@@@@@@@@@@
        StopTimer(WORKER_TIMER);
        PrintTimer("Load Time", WORKER_TIMER);

        //=========================================================
        vector<int> msgNumVec; //$$$$$$$$$$$$$$$$$$$$ added for per-worker msg counting

        init_timers();
        ResetTimer(WORKER_TIMER);
        //supersteps
        global_step_num = 0;
        long long step_msg_num;
        long long step_vadd_num;
        long long global_msg_num = 0;
        long long global_vadd_num = 0;
        while (true) {
            global_step_num++;
            ResetTimer(4);
            //===================
            char bits_bor = all_bor(global_bor_bitmap);
            if (getBit(FORCE_TERMINATE_ORBIT, bits_bor) == 1)
                break;
            get_vnum() = all_sum(vertexes.size());
            int wakeAll = getBit(WAKE_ALL_ORBIT, bits_bor);
            if (wakeAll == 0) {
                active_vnum() = all_sum(active_count);
                if (active_vnum() == 0 && getBit(HAS_MSG_ORBIT, bits_bor) == 0)
                    break; //all_halt AND no_msg
            } else
                active_vnum() = get_vnum();
            //===================
            AggregatorT* agg = (AggregatorT*)get_aggregator();
            if (agg != NULL)
                agg->init();
            //===================
            clearBits();
            if (wakeAll == 1)
                all_compute();
            else
                active_compute();
            message_buffer->combine();
            int my_msg_num = message_buffer->get_total_msg(); //$$$$$$$$$$$$$$$$$$$$ added for per-worker msg counting
            msgNumVec.push_back(my_msg_num); //$$$$$$$$$$$$$$$$$$$$ added for per-worker msg counting
            step_msg_num = master_sum_LL(my_msg_num); //$$$$$$$$$$$$$$$$$$$$ added for per-worker msg counting
            step_vadd_num = master_sum_LL(message_buffer->get_total_vadd());
            if (_my_rank == MASTER_RANK) {
                global_msg_num += step_msg_num;
                global_vadd_num += step_vadd_num;
            }
            vector<VertexT*>& to_add = message_buffer->sync_messages();
            agg_sync();
            for (int i = 0; i < to_add.size(); i++)
                add_vertex(to_add[i]);
            to_add.clear();
            //===================
            worker_barrier();
            StopTimer(4);
            if (_my_rank == MASTER_RANK) {
                cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
                cout << "#msgs: " << step_msg_num << ", #vadd: " << step_vadd_num << endl;
            }
        }
        worker_barrier();
        StopTimer(WORKER_TIMER);
        PrintTimer("Communication Time", COMMUNICATION_TIMER);
        PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
        PrintTimer("- Transfer Time", TRANSFER_TIMER);
        PrintTimer("Total Computational Time", WORKER_TIMER);
        if (_my_rank == MASTER_RANK)
            cout << "Total #msgs=" << global_msg_num << ", Total #vadd=" << global_vadd_num << endl;

        // dump graph
        ResetTimer(WORKER_TIMER);
        dump_partition(params.output_path.c_str());

        StopTimer(WORKER_TIMER);
        PrintTimer("Dump Time", WORKER_TIMER);

        //dump report
        if (_my_rank != MASTER_RANK) {
            slaveGather(msgNumVec);
        } else {
            vector<vector<int> > report(_num_workers);
            masterGather(report);
            report[MASTER_RANK].swap(msgNumVec);
            //////
            //per line per worker: #msg for step1, #msg for step2, ...
            hdfsFS fs = getHdfsFS();
            hdfsFile out = getWHandle(reportPath.c_str(), fs);
            char buffer[100];
            for (int i = 0; i < _num_workers; i++) {
                for (int j = 0; j < report[i].size(); j++) {
                    sprintf(buffer, "%d ", report[i][j]);
                    hdfsWrite(fs, out, (void*)buffer, strlen(buffer));
                }
                sprintf(buffer, "\n");
                hdfsWrite(fs, out, (void*)buffer, strlen(buffer));
            }
            if (hdfsFlush(fs, out)) {
                fprintf(stderr, "Failed to 'flush' %s\n", reportPath.c_str());
                exit(-1);
            }
            hdfsCloseFile(fs, out);
            hdfsDisconnect(fs);
        }
    }
    */

private:
    HashT hash;
    VertexContainer vertexes;
    int active_count;

    MessageBuffer<VertexT>* message_buffer;
    Combiner<MessageT>* combiner;
    AggregatorT* aggregator;
};

#endif
