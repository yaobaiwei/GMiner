//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef AGGREGATOR_HPP_
#define AGGREGATOR_HPP_

#include <stddef.h>

const int AGGSWITCH = 10485760;

template <class Context, class PartialT, class FinalT>
class Aggregator
{
public:
	typedef PartialT PartialType;
	typedef FinalT FinalType;

	virtual void init() = 0;
	virtual void step_partial(Context & context) = 0;
	virtual void step_final(PartialT* part) = 0;
	virtual PartialT* finish_partial() = 0;
	virtual FinalT* finish_final() = 0;
};


class DummyAgg : public Aggregator<char, char, char>
{
public:
	virtual void init() {}
	virtual void step_partial(char * v) {}
	virtual void step_final(char* part) {}
	virtual char* finish_partial()
	{
		return NULL;
	}
	virtual char* finish_final()
	{
		return NULL;
	}
};

#endif /* AGGREGATOR_HPP_ */
