//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef TASK_PIPELINE_HPP_
#define TASK_PIPELINE_HPP_

#include "util/global.hpp"

#include "core/priority_queue.hpp"
#include "core/task_vector.hpp"
#include "core/threadsafe_queue.hpp"


template <class TaskT>
struct TaskPackage
{
	vector<TaskT*> tasks;
	vector<int> dsts;
	TaskPackage();
	TaskPackage(vector<TaskT *>& _tasks, vector<int>& _dsts);
};


template <class TaskT>
class TaskPipeline
{
public:
	typedef PQueue<signT, TaskT> PQ;
	typedef ThreadsafeQueue<TaskPackage<TaskT>> CMQ;
	typedef ThreadsafeQueue<TaskT*> CPQ;
	typedef TaskVector<TaskT*> TaskBuf;

	void init(string PQUE_DIR, int M);

	PQ& get_pq_instance();
	void pq_push_back(vector<typename PQ::KTpair>& tasks);
	bool pq_pop_front(vector<TaskT*>& tasks);
	void pq_pop_front_to_remote(vector<TaskT*>& remote_tasks);

	void cmq_push_back(TaskPackage<TaskT>& pkg);
	bool cmq_pop_front(TaskPackage<TaskT>& pkg);

	void cpq_push_back(vector<TaskT*>& tasks);
	bool cpq_pop_front(TaskT* &task);

	void taskbuf_push_back(TaskT* &task);
	void taskbuf_content(vector<TaskT*> & tasks);

	size_t cmq_size();
	size_t cpq_size();
	size_t taskbuf_size();

	void close();

private:
	PQ priority_queue_;
	CMQ communication_queue_;
	CPQ computation_queue_;
	TaskBuf task_buffer_;
};


#include "task_pipeline.tpp"

#endif /* TASK_PIPELINE_HPP_ */
