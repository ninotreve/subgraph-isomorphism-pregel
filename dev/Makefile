CCOMPILE=mpic++
CPPFLAGS= -I$(HADOOP_HOME)/include -I$(JAVA_HOME)/include -I$(JAVA_HOME)/include/linux -I ../dev
LIB = -L$(HADOOP_HOME)/lib/native
LDFLAGS = -lhdfs
FLAGS = -Wno-deprecated -O3

all: run

run: run.cpp
	$(CCOMPILE) -std=c++17 run.cpp $(CPPFLAGS) $(LIB) $(LDFLAGS) $(FLAGS) -o run

clean:
	-rm run