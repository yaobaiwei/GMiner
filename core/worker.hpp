//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef WORKER_HPP_
#define WORKER_HPP_

#include <iostream>
#include <stdio.h>
#include <stdlib.h>

#include "util/aggregator.hpp"
#include "util/global.hpp"

#include "core/master.hpp"
#include "core/slave.hpp"

using namespace std;


template <class Master,  class Slave, class AggregatorT = DummyAgg> //user-defined types for vertexID, adjacency-list items
class Worker
{
public:
	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

	typedef Master MasterT;
	typedef Slave SlaveT;

	Worker();

	virtual ~Worker()
	{
		if (get_agg() != NULL)
			delete (FinalT*)global_agg;
	}

	void set_aggregator(AggregatorT* ag, FinalT init_value);

	void run(const WorkerParams& params);
};


#include "worker.tpp"

#endif /* WORKER_HPP_ */
