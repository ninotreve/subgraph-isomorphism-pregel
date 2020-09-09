#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){

    MatchingCommand command(argc, argv);
    WorkerParams params;
    params.data_path = command.getDataPath();
    params.query_path = command.getQueryPath();
    params.output_path = command.getOutputPath();

    params.enumerate = command.getEnumerateMethod();
    params.report = command.getReport();
    params.input = command.getInputFormat();

	init_workers();
	pregel_subgraph(params);
	worker_finalize();
	return 0;
}
