// ./run -d /fdu-zyj/labeled -q /fdu-zyj/query -out /fdu-zyj/output -input g-thinker
//./run -d /labelyou -q /gtQuery -out /output -input g-thinker
#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){

    MatchingCommand command(argc, argv);
    WorkerParams params = WorkerParams(command, true);

	init_workers();
	pregel_subgraph(params);
	worker_finalize();
	return 0;
}
