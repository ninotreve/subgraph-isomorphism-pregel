#include "pregel_app_subgraph.h"

int main(int argc, char* argv[]){
	init_workers();
	pregel_subgraph("/toyFolder", "/toyOutput", false);
	worker_finalize();
	return 0;
}
