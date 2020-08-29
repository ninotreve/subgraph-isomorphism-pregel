#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_subgraph("/toyGraph", "/toyQuery", "/toyOutput", true);
	worker_finalize();
	return 0;
}
