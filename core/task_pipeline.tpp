//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class TaskT>
TaskPackage<TaskT>::TaskPackage() {};

template <class TaskT>
TaskPackage<TaskT>::TaskPackage(vector<TaskT*>& _tasks, vector<int>& _dsts)
{
	tasks = _tasks;
	dsts = _dsts;
}


template <class TaskT>
inline void TaskPipeline<TaskT>::init(string PQUE_DIR, int M)
{
	priority_queue_.init(PQUE_DIR, M);
}

template <class TaskT>
inline typename TaskPipeline<TaskT>::PQ& TaskPipeline<TaskT>::get_pq_instance()
{
	return priority_queue_;
}

template <class TaskT>
inline void TaskPipeline<TaskT>::pq_push_back(vector<typename PQ::KTpair>& tasks)
{
	priority_queue_.push_back(tasks);
}

template <class TaskT>
inline bool TaskPipeline<TaskT>::pq_pop_front(vector<TaskT*>& tasks)
{
	return priority_queue_.pop_front(tasks);
}

template <class TaskT>
inline void TaskPipeline<TaskT>::pq_pop_front_to_remote(vector<TaskT*>& remote_tasks)
{
	priority_queue_.pop_front_to_remote(remote_tasks);
}

template <class TaskT>
inline void TaskPipeline<TaskT>::cmq_push_back(TaskPackage<TaskT>& pkg)
{
	communication_queue_.push_back(pkg);
}

template <class TaskT>
inline bool TaskPipeline<TaskT>::cmq_pop_front(TaskPackage<TaskT>& pkg)
{
	return communication_queue_.pop_front(pkg);
}

template <class TaskT>
inline void TaskPipeline<TaskT>::cpq_push_back(vector<TaskT*>& tasks)
{
	computation_queue_.push_back(tasks);
}

template <class TaskT>
inline bool TaskPipeline<TaskT>::cpq_pop_front(TaskT* &task)
{
	return computation_queue_.pop_front(task);
}

template <class TaskT>
inline void TaskPipeline<TaskT>::taskbuf_push_back(TaskT* &task)
{
	task_buffer_.push_back(task);
}

template <class TaskT>
inline void TaskPipeline<TaskT>::taskbuf_content(vector<TaskT*>& tasks)
{
	task_buffer_.content(tasks);
}

template <class TaskT>
inline size_t TaskPipeline<TaskT>::cmq_size()
{
	return communication_queue_.size();
}

template <class TaskT>
inline size_t TaskPipeline<TaskT>::cpq_size()
{
	return computation_queue_.size();
}

template <class TaskT>
inline size_t TaskPipeline<TaskT>::taskbuf_size()
{
	return task_buffer_.size();
}

template <class TaskT>
inline void TaskPipeline<TaskT>::close()
{
	priority_queue_.close();
	communication_queue_.close();
	computation_queue_.close();
}
