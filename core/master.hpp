//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef MASTER_HPP_
#define MASTER_HPP_

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <thread>
#include <vector>

#include "util/aggregator.hpp"
#include "util/communication.hpp"
#include "util/global.hpp"
#include "util/hdfs_core.hpp"
#include "util/serialization.hpp"
#include "util/type.hpp"

using namespace std;


struct Progress
{
	unsigned long long memory_task_num;
	unsigned long long disk_task_num;
};

template <class AggregatorT = DummyAgg>
class Master
{
public:
	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

	virtual void print_result() {}

	Master();

	void agg_sync();
	void context_sync();
	void end_sync();

	void start_to_work();
	void terminate();

	void schedule_listen();
	int progress_scheduler(int request_worker_id);

	void task_steal();

	void run(const WorkerParams& params);

private:

	bool is_end_; // agg_sync

	mutex end_lock_;
	condition_variable end_cond_;

	map<int, Progress> progress_map_;

};


#include "master.tpp"

#endif /* MASTER_HPP_ */
