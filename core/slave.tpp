//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


//PART 1 =======================================================
//constructor

template <class TaskT,  class AggregatorT>
Slave<TaskT, AggregatorT>::Slave()
{
	cache_size_ = CACHE_SIZE;
	cache_overflow_ = false;
	merged_file_num_ = 0;
	is_end_ = false;
	report_end_ = false;
	threadpool_size_ = NUM_COMP_THREAD;
	vtx_req_count_=vtx_resp_count_=0;
}

//PART 2 =======================================================
//user-defined functions

template <class TaskT,  class AggregatorT>
inline typename Slave<TaskT, AggregatorT>::VertexT* Slave<TaskT, AggregatorT>::respond(const typename Slave<TaskT, AggregatorT>::VertexT * v)
{
	return NULL;
}

//PART 3 =======================================================
//communication primitives

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::set_req(ibinstream & m, vector<KeyT>& request)
{
	m << REQUEST_VERTEX;
	m << _my_rank;
	m << request;
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::set_end_tag(ibinstream & m) //negative worker ID means that work has finished its computation
{
	m << REQUEST_END; //one may get the ID of the worker by doing - x - 1 again (e.g., may be useful for work stealing)
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::set_steal(ibinstream & m)
{
	m << REQUEST_TASK;
	m << _my_rank;
}

template <class TaskT,  class AggregatorT>
int Slave<TaskT, AggregatorT>::set_resp(ibinstream & m, obinstream & um)//return requester machine ID, -1 means end-tag
{
	int tag;
	um >> tag;
	switch(tag)
	{
	case REQUEST_END:
		return -1;
	case REQUEST_VERTEX:
	{
		int tgt;
		vector<KeyT> ids;

		um >> tgt;
		um >> ids;

		int num = ids.size();//number of vertices in response
		m << num;
		for(int i=0; i<num; i++)
		{
			KeyT id = ids[i];
			VertexT * tmp = respond(local_table_.get(id));
			if(tmp != NULL)
			{
				m << tmp;
				delete tmp;
			}
			else
			{
				m << local_table_.get(id);
			}
		}
		return tgt;
	}
	case REQUEST_TASK:
	{
		int tgt;
		um >> tgt;
		TaskVec remote_tasks;
		task_pipeline_.pq_pop_front_to_remote(remote_tasks);
		int num = remote_tasks.size();
		m << num;
		for(int i = 0; i < num; i++)
		{
			m << remote_tasks[i];
			delete remote_tasks[i];
		}
		return tgt;
	}
	}
}

//PART 4 =======================================================
//high level functions for system

template <class TaskT,  class AggregatorT>
bool Slave<TaskT, AggregatorT>::wait_to_start()
{
	MSG cmd = slave_bcastCMD();
	if(cmd == START)
		return true;
	return false;
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::load_graph(const char* inpath)
{
	hdfsFS fs = get_hdfs_fs();
	hdfsFile in = get_r_handle(inpath, fs);
	LineReader reader(fs, in);
	while(true)
	{
		reader.read_line();
		if (!reader.eof())
		{
			VertexT * v = to_vertex(reader.get_line());
			local_table_.set(v->id, v);
		}
		else
			break;
	}
	hdfsCloseFile(fs, in);
	hdfsDisconnect(fs);
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::grow_tasks()
{
	TaskVec tasks;
	VertexT* v = local_table_.next();
	while(v)
	{
		TaskT * t = create_task(v);
		if(t != NULL)
		{
			t->set_to_request();
			if(t->is_request_empty())
			{
				t = recursive_compute(t);
			}
			if(t != NULL)
			{
				tasks.push_back(t);
				if(tasks.size() >=  global_merge_limit)
					dump_tasks(tasks);
			}
		}
		v = local_table_.next();
	}
	if(!tasks.empty())
		dump_tasks(tasks);
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::dump_tasks(TaskVec& tasks)
{
	char num[10], fname[100];
	sprintf(num, "/%d", merged_file_num_++);
	strcpy(fname, MERGE_DIR.c_str());
	strcat(fname, num);
	vector<KTpair> signed_tasks;
	ifbinstream fin;

	task_sorter_.sign_and_sort_tasks(tasks, signed_tasks);

	fin.open(fname);
	for (int i = 0; i < signed_tasks.size(); i++)
	{
		fin << signed_tasks[i];
		delete signed_tasks[i].task;
	}
	fin.close();

	inc_task_num_in_disk(tasks.size());
	tasks.clear();
}

template <class TaskT,  class AggregatorT>
TaskT* Slave<TaskT, AggregatorT>::recursive_compute(TaskT* task)
{
	//set frontier for UDF compute
	vector<VertexT *> frontier;
	for(int i = 0 ; i < task->to_pull.size(); i++)
	{
		AdjVertex& v=task->to_pull[i];
		VertexT *pvtx=NULL;
		v.wid == _my_rank? pvtx=local_table_.get(v.id) : pvtx=cache_table_.get(v.id);
		frontier.push_back(pvtx);
	}
	//clear task.to_pull, reset it in compute
	task->to_pull.clear();

	if(task->compute(task->subG, task->context, frontier))
	{
		task->set_to_request();
		//if to_request is not empty, add the current task into pque
		if(task->to_request.empty())
		{
			//continue to compute until find remote vertexes needed to pull
			return recursive_compute(task);
		}
		return task;
	}
	else
	{
		//The current task is done, delete it after storing the result of it through aggregator
		AggregatorT* agg = (AggregatorT*)get_aggregator();
		if (agg != NULL)
		{
			agg_lock_.lock();
			agg->step_partial(task->context);
			agg_lock_.unlock();
		}
		delete task;
		return NULL;
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::pull_PQ_to_CMQ()
{
	queue<ibinstream> streams;
	queue<MPI_Request> mpi_requests;
	while(1)
	{
		TaskVec tasks;
		CountMap ref_count;
		CountMap worker_map;
		CountMap to_pull;

		bool status = task_pipeline_.pq_pop_front(tasks);
		if(!status) break;

		for(int i = 0; i < tasks.size(); i++)
		{
			TaskT * t = tasks[i];
			vector<int> & reqs = t->to_request;
			AdjVtxList & nbs = t->to_pull;
			for(int j=0; j< reqs.size(); j++)
			{
				AdjVertex & item = nbs[reqs[j]];
				CountMapIter mIter = ref_count.find(item.id);
				if(mIter == ref_count.end())
				{
					ref_count[item.id] = 1;
					worker_map[item.id] = item.wid;
				}
				else
				{
					mIter->second++;
				}
			}
		}
		for(CountMapIter mIter = ref_count.begin(); mIter != ref_count.end(); mIter++)
		{
			if(!cache_table_.try_to_get(mIter->first, mIter->second))
			{
				to_pull[mIter->first] = worker_map[mIter->first];
			}
		}

		if(cache_table_.size() > cache_size_)
		{
			//guarantee the cache is locked when checking
			unique_lock<mutex> lck(commun_lock_);

			//delete the items whose ref are 0, the num is at most to_pull.size()
			int diff = cache_table_.size() - cache_size_;
			cache_table_.batch_clear(diff);

			while(cache_table_.size() > cache_size_)
			{
				if(cache_table_.size() == to_pull.size())
				{
					cache_overflow_ = true;
					cache_table_.resize(to_pull.size());
					break;
				}
				else
				{
					commun_cond_.wait(lck);
					int diff = cache_table_.size() - cache_size_;
					cache_table_.batch_clear(diff);
				}
			}
		}

		vector<vector<KeyT> > requests(_num_workers - 1);
		for(CountMapIter it = to_pull.begin(); it != to_pull.end(); it++)
		{
			requests[it->second].push_back(it->first);
		}

		vector<int> dsts;
		for(int i = 1; i <_num_workers; i++)
		{
			int dst = (_my_rank + i) % _num_workers; //to avoid receiver-side bottleneck
			if(dst != MASTER_RANK && requests[dst].size() > 0) //only send if there are requests //essentially, won't request to itself
			{
				streams.push(ibinstream());
				mpi_requests.push(MPI_Request());
				set_req(streams.back(), requests[dst]);
				send_ibinstream_nonblock(streams.back(), dst, REQUEST_CHANNEL, mpi_requests.back());
				dsts.push_back(dst);
			}
			if(mpi_requests.size() > 0){
				int test_flag;
				MPI_Test(&(mpi_requests.front()), &test_flag, MPI_STATUS_IGNORE);
				if(test_flag){
					mpi_requests.pop();
					streams.pop();
				}
			}
		}
		TaskPackage<TaskT> pkg(tasks, dsts);
		task_pipeline_.cmq_push_back(pkg);
		vtx_req_count_++;

		std::unique_lock<std::mutex> lck(vtx_req_lock_);
		if((vtx_req_count_-vtx_resp_count_)>=VTX_REQ_MAX_GAP)
			vtx_req_cond_.wait(lck);
		
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::pull_CMQ_to_CPQ()
{
	while(1)
	{
		TaskPackage<TaskT> pkg;
		bool status = task_pipeline_.cmq_pop_front(pkg);
		if(!status) break;

		vector<int>& dsts = pkg.dsts;
		for(int i = 0; i < dsts.size(); i++)
		{
			obinstream um = recv_obinstream(dsts[i], RESPOND_CHANNEL);
			int num;
			um >> num;
			for(int i=0; i<num; i++)
			{
				VertexT * v = new VertexT;
				um >> *v;
				cache_table_.set(v->id, v);
			}
		}
		task_pipeline_.cpq_push_back(pkg.tasks);
		vtx_resp_count_++;

		if((vtx_req_count_-vtx_resp_count_)<VTX_REQ_MAX_GAP)
		{
			std::unique_lock<std::mutex> lck(vtx_req_lock_);
			vtx_req_cond_.notify_one();
		}
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::pull_CPQ_to_taskbuf()
{
	while(1)
	{
		TaskT* t;
		bool status = task_pipeline_.cpq_pop_front(t);
		if(!status) break;

		vector<int> & reqs = t->to_request;
		AdjVtxList & nbs = t->to_pull;
		AdjVtxList refs;
		for(int j=0; j< reqs.size(); j++)
		{
			refs.push_back(nbs[reqs[j]]);
		}
		t->to_request.clear();
		t = recursive_compute(t);

		commun_lock_.lock();
		//clear the items in cache with refs
		for(int j = 0 ; j < refs.size(); j++)
		{
			cache_table_.dec_item_ref(refs[j].id);
		}

		if(cache_overflow_)
		{
			cache_overflow_ = false;
			cache_table_.resize(cache_size_);
		}
		commun_lock_.unlock();
		commun_cond_.notify_one();

		compute_lock_.lock();
		if(t != NULL)
		{
			task_pipeline_.taskbuf_push_back(t);
		}
		else
		{
			dec_task_num_in_memory(1);
		}
		compute_lock_.unlock();
		compute_cond_.notify_one();
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::run_to_no_task()
{
	//check whether there is any task in system
	while(get_task_num_in_memory() || get_task_num_in_disk())
	{
		{
			unique_lock<mutex> lck(compute_lock_);
			while(task_pipeline_.taskbuf_size() < global_taskBuf_size && get_task_num_in_disk())
			{
				compute_cond_.wait(lck);
			}
		}
		TaskVec tasks;
		vector<KTpair> signed_tasks;
		task_pipeline_.taskbuf_content(tasks);
		task_sorter_.sign_and_sort_tasks(tasks, signed_tasks);
		task_pipeline_.pq_push_back(signed_tasks);
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::recv_run()
{
	int num_end_tag = 0;
	queue<ibinstream> streams;
	queue<MPI_Request> requests;
	while(num_end_tag < (_num_workers-1))
	{
		obinstream um = recv_obinstream(MPI_ANY_SOURCE, REQUEST_CHANNEL);
		streams.push(ibinstream());
		int dst = set_resp(streams.back(), um);
		if(dst < 0)
			num_end_tag ++;
		else{
			requests.push(MPI_Request());
			send_ibinstream_nonblock(streams.back(), dst, RESPOND_CHANNEL, requests.back());
		}

		if(requests.size() > 0){
			int test_flag;
			MPI_Test(&requests.front(), &test_flag, MPI_STATUS_IGNORE);
			if(test_flag){
				streams.pop();
				requests.pop();
			}
		}
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::end_recver()
{
	ibinstream m;
	set_end_tag(m);
	for(int i = 0; i< _num_workers; i++)
	{
		int dst = (_my_rank + i) % _num_workers; //to avoid receiver-sided bottleneck
		if( dst != MASTER_RANK)
			send_ibinstream(m, dst, REQUEST_CHANNEL);
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::report_to_master(unsigned long long mem_tasks, unsigned long long disk_tasks)
{
	vector<unsigned long long> progress;
	progress.push_back(_my_rank);
	progress.push_back(mem_tasks);
	progress.push_back(disk_tasks);
	send_data(progress, MASTER_RANK, SCHEDULE_HEARTBEAT_CHANNEL);
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::report()
{
	while (!report_end_)
	{
		report_to_master(get_task_num_in_memory(), get_task_num_in_disk());
		sleep(COMMUN_TIME);
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::end_report()
{
	report_to_master(-1,-1);
	report_end_ = true;
	send_data(DONE, MASTER_RANK, MSCOMMUN_CHANNEL);
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::agg_sync()
{
	AggregatorT* agg = (AggregatorT*)get_aggregator();
	if (agg != NULL)
	{
		agg_lock_.lock();
		PartialT* part = agg->finish_partial();
		agg_lock_.unlock();
		//gathering
		slave_gather(part);
		//scattering
		FinalT final;
		slave_bcast(final);
		*((FinalT*)global_agg) = final;
	}
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::context_sync()
{
	if(SLEEP_TIME == 0)
	{
		//do agg_sync when all the tasks have been processed
		unique_lock<mutex> lck(end_lock_);
		while (!is_end_)
		{
			end_cond_.wait(lck);  //block the thread until end_cond is notified.
		}
	}
	else
	{
		while (!all_land(is_end_))  //do agg_sync periodically when at least one worker is still computing
		{
			agg_sync();
			sleep(SLEEP_TIME);
		}
	}
	agg_sync();
}

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::end_sync()
{
	if(SLEEP_TIME == 0)
	{
		end_lock_.lock();
		is_end_ = true;
		end_lock_.unlock();
		end_cond_.notify_all();
	}
	else
	{
		is_end_ = true;
	}
}


template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::debug()
{
	while(!is_end_)
	{
		cout << _my_rank << " => Tasks in MEM = " << get_task_num_in_memory() \
		     << " | Tasks in DISK = " << get_task_num_in_disk() \
		     << " | CommunQ.size = " << task_pipeline_.cmq_size()*PIPE_POP_NUM \
		     << " | ComputeQ.size = " << task_pipeline_.cpq_size() \
		     << " | TaskBuff.size = " << task_pipeline_.taskbuf_size() <<endl;

		sleep(1);
	}
}

//PART 1 =======================================================
//public run

template <class TaskT,  class AggregatorT>
void Slave<TaskT, AggregatorT>::run(const WorkerParams& params)
{
	bool start = wait_to_start();
	if(!start)
	{
		cout << _my_rank << "=> Error: Failed in parse input data." << endl;
		return;
	}

	//================ Load graph from HDFS =================
	init_timers();
	ResetTimer(WORKER_TIMER);

	string rank = to_string(_my_rank);
	string myfile = params.input_path + "/part_" + rank;
	load_graph(myfile.c_str());

	StopTimer(WORKER_TIMER);
	PrintTimer("Load Time", WORKER_TIMER);
	//=======================================================

	//============== Check local disk path ==================
	PQUE_DIR = params.local_root+ "/" + rank + "/queue";
	MERGE_DIR = params.local_root+ "/" + rank + "/merge";

	//check disk folder
	check_dir(PQUE_DIR, params.force_write);
	check_dir(MERGE_DIR, params.force_write);
	//=======================================================

	//======================== init =========================
	AggregatorT* agg = (AggregatorT*)get_aggregator();
	if (agg != NULL)
		agg->init();
	cache_table_.init(cache_size_);
	task_sorter_.init(global_sign_size, slave_all_sum(local_table_.size()));
	task_pipeline_.init(PQUE_DIR, global_file_size);
	//=======================================================

	//========================= RUN =========================
	//start the sync thread for each process
	thread sync(&Slave::context_sync, this);

	//start listening thread for vertex passing
	thread recver(&Slave::recv_run, this);

	//start master-slaves task_schedule report
	thread reporter(&Slave::report, this);

	//for debugging
	thread debugger(&Slave::debug, this);

	//seeding tasks in parallel
	local_table_.load();

	ResetTimer(WORKER_TIMER);
	for( unsigned i = 0; i < threadpool_size_; ++i )
		threadpool_.emplace_back(std::thread(&Slave::grow_tasks, this));
	for( auto &t : threadpool_ ) t.join();
	threadpool_.clear();
	StopTimer(WORKER_TIMER);
	cout << _my_rank << ": Task Seeding is Finished. | Total Time: " << get_timer(WORKER_TIMER) << endl;

	//Merging the tasks in a global sorted order;
	ResetTimer(WORKER_TIMER);
	task_sorter_.merge_sort_seed_tasks(task_pipeline_.get_pq_instance(), merged_file_num_);
	StopTimer(WORKER_TIMER);
	cout << _my_rank << ": Task Merging is Finished. | Total Time: " << get_timer(WORKER_TIMER) << endl;

	//Task_Pipline
	thread reqThread(&Slave::pull_PQ_to_CMQ, this);
	thread respThread(&Slave::pull_CMQ_to_CPQ, this);
	for( unsigned i = 0; i < threadpool_size_; ++i )
		threadpool_.emplace_back(std::thread(&Slave::pull_CPQ_to_taskbuf, this));
	run_to_no_task();
	cout << _my_rank << ": Regular Work Pipeline is Done, Start Task Stealing Stage." << endl;

	//Task Stealing Step
	queue<ibinstream> streams;
	queue<MPI_Request> requests;
	while(1)
	{
		send_data(get_worker_id(), MASTER_RANK, SCHEDULE_REPORT_CHANNEL);
		int tgt_wid = recv_data<int>(MASTER_RANK, SCHEDULE_REPORT_CHANNEL);
		if(tgt_wid == NO_WORKER_BUSY)
		{
			break;
		}
		else
		{
			//request a task from the corresponding worker
			//and
			//receive the task, recompute the remote vertices and do computation
			streams.push(ibinstream());
			requests.push(MPI_Request());
			set_steal(streams.back());
			send_ibinstream_nonblock(streams.back(), tgt_wid, REQUEST_CHANNEL, requests.back());

			TaskVec tasks;
			int num;

			obinstream um = recv_obinstream(tgt_wid, RESPOND_CHANNEL);
			um >> num;
			for(int i=0; i<num; i++)
			{
				TaskT * t = new TaskT;
				um >> *t;
				tasks.push_back(t);
			}
			inc_task_num_in_memory(num);
			vector<KTpair> signed_tasks;
			task_sorter_.sign_and_sort_tasks(tasks, signed_tasks);
			task_pipeline_.pq_push_back(signed_tasks);
			run_to_no_task();

			if(requests.size() > 0){
				int test_flag;
				MPI_Test(&requests.front(), &test_flag, MPI_STATUS_IGNORE);
				if(test_flag){
					streams.pop();
					requests.pop();
				}
			}
		}
	}

	//close the queues in task_pipeline
	task_pipeline_.close();

	//end the pipeline threads
	reqThread.join();
	respThread.join();
	for( auto &t : threadpool_ ) t.join();
	threadpool_.clear();

	//end the computation
	end_recver();
	end_report();
	end_sync();

	//join the left threads
	recver.join();
	reporter.join();
	sync.join();

	//join the debugger threads
	debugger.join();
}
