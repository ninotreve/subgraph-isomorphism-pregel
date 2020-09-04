#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){

    MatchingCommand command(argc, argv);
    string data_path = command.getDataPath();
    string query_path = command.getQueryPath();
    string output_path = command.getOutputPath();

	init_workers();
	pregel_subgraph(data_path, query_path, output_path, true);
	worker_finalize();
	return 0;
}
