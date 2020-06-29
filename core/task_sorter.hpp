//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef TASK_SORTER_HPP_
#define TASK_SORTER_HPP_

#include "util/global.hpp"
#include "util/minhash.hpp"
#include "util/type.hpp"

#include "core/priority_queue.hpp"

#include <string.h>
#include <vector>

using namespace std;


template <class TaskT>
class TaskSorter
{
public:
	typedef ktpair<signT, TaskT> KTpair;
	typedef PQueue<signT, TaskT> PQue;

	TaskSorter();

	void init(int k, int V);

	// sign tasks
	void sign_and_sort_tasks(vector<TaskT*>& tasks, vector<KTpair>  & signed_tasks);

	// entry interface for merge sort
	void merge_sort_seed_tasks(PQue& pque, int num_files);

private:

	Minhash hash_;
	char fname_[100];
	char num_[10];
	bool hash_inited_;

	struct TaskSID // TaskSID expanded with stream ID, used by merge(.)
	{
		KTpair ktpair;
		int sid;

		inline bool operator<(const TaskSID& rhs) const
		{
			return ktpair.key > rhs.ktpair.key; //reverse to ">" because priority_queue is a max-heap
		}
	};

	// set filename
	void set_fname(string root, int file_bat, int level);

	// void write2file(vector<KTpair> & signed_tasks);

	// sub-function of seedTask_merge_sort
	void merge_seed_tasks(vector<ofbinstream>& out, ifbinstream& in); //open outside, close inside

	void last_merge_seed_tasks(PQue& pque, int fnum, int level);

};


#include "task_sorter.tpp"

#endif /* TASK_SORTER_HPP_ */
