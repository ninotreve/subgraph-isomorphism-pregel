// ./run -d /fdu-zyj/labeled -q /fdu-zyj/query -out /fdu-zyj/output -input g-thinker
//./run -d /labelyou -q /gtQuery -out /output -input g-thinker
#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){

    MatchingCommand command(argc, argv);
    WorkerParams params;
    params.data_path = command.getDataPath();
    params.query_path = command.getQueryPath();
    params.output_path = command.getOutputPath();

    params.partition = command.getPartition();
    params.filter = command.getFilterMethod();
    params.enumerate = command.getEnumerateMethod();
    params.report = command.getReport();
    params.input = command.getInputFormat();

	init_workers();
	pregel_subgraph(params);
	worker_finalize();
	return 0;
}
