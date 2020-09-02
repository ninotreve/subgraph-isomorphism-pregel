#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){
	init_workers();
	if (argc != 4)
	{
		cout << "Must provide three parameters: toy_folder, query_folder and output_folder."
			 << endl;
		return 1;
	}
	pregel_subgraph(argv[1], argv[2], argv[3], true);
	worker_finalize();
	return 0;
}
