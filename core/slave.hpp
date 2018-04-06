//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef SLAVE_HPP_
#define SLAVE_HPP_

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

#include "util/aggregator.hpp"
#include "util/communication.hpp"
#include "util/global.hpp"
#include "util/hdfs_core.hpp"
#include "util/minhash.hpp"
#include "util/serialization.hpp"
#include "util/slave_commun.hpp"
#include "util/type.hpp"

#include "core/ref_cache.hpp"
#include "core/task.hpp"
#include "core/task_pipeline.hpp"
#include "core/task_sorter.hpp"
#include "core/vertex.hpp"
#include "core/vertex_table.hpp"

using namespace std;


template <class TaskT,  class AggregatorT = DummyAgg> //user-defined types for vertexID, adjacency-list items
class Slave
{
public:
	//PART 0 =======================================================
	//typedef

	typedef typename TaskT::VertexT VertexT;
	typedef typename TaskT::NodeT NodeT;

	typedef typename TaskT::AdjVertex AdjVertex;
	typedef typename TaskT::AdjVtxList AdjVtxList;
	typedef typename TaskT::AdjVtxIter AdjVtxIter;

	typedef typename TaskT::AdjNode AdjNode;
	typedef typename TaskT::AdjNodeList AdjNodeList;
	typedef typename TaskT::AdjNodeIter AdjNodeIter;

	typedef typename TaskT::SubgraphT SubgraphT;
	typedef typename TaskT::ContextType ContextT;

	typedef typename VertexT::KeyT KeyT;
	typedef typename VertexT::AttrT AttrT;

	typedef ktpair<signT, TaskT> KTpair;
	typedef vector<TaskT*> TaskVec;

	typedef typename AggregatorT::PartialType PartialT;
	typedef typename AggregatorT::FinalType FinalT;

	typedef hash_map<KeyT, int> CountMap;
	typedef typename CountMap::iterator CountMapIter;

	typedef hash_set<KeyT> IDSet;
	typedef typename IDSet::iterator IDSetIter;

	//PART 1 =======================================================
	//constructor, destructor, public run

	Slave();

	virtual ~Slave()
	{
		local_table_.clear();
		cache_table_.clear();
		rm_dir(PQUE_DIR);
		rm_dir(MERGE_DIR);
	}

	void run(const WorkerParams& params);

	//PART 2 =======================================================
	//user-defined functions
	virtual VertexT* to_vertex(char* line) = 0;
	virtual TaskT* create_task(VertexT* v) = 0;
	virtual VertexT* respond(const VertexT* v);

private:

	//PART 3 =======================================================
	//communication primitives

	void set_req(ibinstream & m, vector<KeyT>& request);

	void set_end_tag(ibinstream & m); //negative worker ID means that work has finished its computation

	void set_steal(ibinstream & m);

	int set_resp(ibinstream & m, obinstream & um); //return requester machine ID, -1 means end-tag

	//PART 4 =======================================================
	//high level functions for system

	bool wait_to_start();

	void load_graph(const char* inpath);

	void grow_tasks();

	void dump_tasks(TaskVec& tasks);

	TaskT * recursive_compute(TaskT* task);

	void pull_PQ_to_CMQ();

	void pull_CMQ_to_CPQ();

	void pull_CPQ_to_taskbuf();

	void run_to_no_task();

	void report_to_master(unsigned long long mem_tasks, unsigned long long disk_tasks);

	void recv_run();

	void end_recver();

	void report();

	void end_report();

	void agg_sync();

	void context_sync();

	void end_sync();

	void debug();

	//PART 5 =======================================================
	//members

	VertexTable<KeyT, VertexT> local_table_; 	//key-value store of my vertex portion
	RefCache<KeyT, VertexT> cache_table_; 		//cache the remote vertices

	int cache_size_;			//number of vertices allowed in cache
	bool cache_overflow_;  	//record the status of cache

	atomic_int merged_file_num_;

	bool is_end_;
	bool report_end_;

	//---------------------- multiple-threads ----------------------
	int threadpool_size_;	//number of threads in threadpool
	vector<thread> threadpool_;

	TaskPipeline<TaskT> task_pipeline_;
	TaskSorter<TaskT> task_sorter_;

	mutex end_lock_;
	condition_variable end_cond_;

	mutex commun_lock_;
	condition_variable commun_cond_;

	mutex compute_lock_;
	condition_variable compute_cond_;

	mutex agg_lock_;

	static const int VTX_REQ_MAX_GAP=10;
	atomic_int vtx_req_count_, vtx_resp_count_;
	mutex vtx_req_lock_;
	condition_variable vtx_req_cond_;

};


#include "slave.tpp"

#endif /* SLAVE_HPP_ */
