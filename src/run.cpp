#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_subgraph("/toyGraph/toy1", "/toyQuery/query1", "/toyOutput", true);
	worker_finalize();
	return 0;
}
