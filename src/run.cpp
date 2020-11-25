#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){

    MatchingCommand command(argc, argv);
    WorkerParams params = WorkerParams(command, true);

	init_workers();
	if (_my_rank == MASTER_RANK)
		params.print();
	pregel_subgraph(params);
	worker_finalize();
	return 0;
}
