#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include <stddef.h>

#define AGGSWITCH 10485760

template <class PartialT, class FinalT>
class Aggregator {
public:
    typedef PartialT PartialType;
    typedef FinalT FinalType;

    virtual void init() = 0;
    virtual PartialT* finishPartial() = 0;
    virtual void stepFinal(PartialT* part) = 0;
    virtual FinalT* finishFinal() = 0;
};

class DummyAgg : public Aggregator<char, char> {

public:
    virtual void init()
    {
    }
    virtual void stepFinal(char* part)
    {
    }
    virtual char* finishPartial()
    {
        return NULL;
    }
    virtual char* finishFinal()
    {
        return NULL;
    }
};

#endif
