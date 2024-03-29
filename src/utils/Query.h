#ifndef QUERY_H
#define QUERY_H

#include <stddef.h>

template <class NodeT>
class Query {
public:
    virtual void addNode(char* line) = 0;
    virtual void printOrder() = 0;

    virtual ~Query() {}
};



#endif
