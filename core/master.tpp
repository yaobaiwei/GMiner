//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class AggregatorT>
Master<AggregatorT>::Master()
{
	is_end_ = false;
}

template <class AggregatorT>
void Master<AggregatorT>::agg_sync()
{
	AggregatorT* agg = (AggregatorT*)get_aggregator();
	if (agg != NULL)
	{
		vector<Master<AggregatorT>::PartialT> parts(get_num_workers());
		master_gather(parts);
		for (int i = 0; i < get_num_workers(); i++)
		{
			if(i != MASTER_RANK)
			{
				Master<AggregatorT>::PartialT& part = parts[i];
				agg->step_final(&part);
			}
		}
		Master<AggregatorT>::FinalT* final = agg->finish_final();
		*((Master<AggregatorT>::FinalT*)global_agg) = *final;
		//Bcasting
		master_bcast(*final);
	}
}

template <class AggregatorT>
void Master<AggregatorT>::context_sync()
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
			sleep(SLEEP_TIME);
			agg_sync();
		}
	}
	agg_sync();
	print_result();
}

template <class AggregatorT>
void Master<AggregatorT>::end_sync()
{
	if(SLEEP_TIME == 0 )
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

template <class AggregatorT>
void Master<AggregatorT>::start_to_work()
{
	master_bcastCMD(START);
}

template <class AggregatorT>
void Master<AggregatorT>::terminate()
{
	master_bcastCMD(TERMINATE);
}

template <class AggregatorT>
void Master<AggregatorT>::schedule_listen()
{
	while(1)
	{
		vector<unsigned long long> prog = recv_data<vector<unsigned long long>>(MPI_ANY_SOURCE, SCHEDULE_HEARTBEAT_CHANNEL);
		int src = prog[0];  //the slave ID
		Progress & p = progress_map_[src];
		if(prog[1] != -1)
		{
			p.memory_task_num = prog[1];
			p.disk_task_num = prog[2];
		}
		else
		{
			progress_map_.erase(src);
		}
		if(progress_map_.size() == 0)
			break;
	}
}

template <class AggregatorT>
int Master<AggregatorT>::progress_scheduler(int request_worker_id)
{
	//the result should not be the same worker
	int max = 0;
	int max_index = -1;
	map<int, Progress>::iterator m_iter;
	for(m_iter = progress_map_.begin(); m_iter != progress_map_.end(); m_iter++)
	{
		if(m_iter->second.disk_task_num > max)
		{
			max = m_iter->second.disk_task_num;
			max_index = m_iter->first;
		}
	}
	if(max_index != -1 && max_index != request_worker_id)
		return max_index;
	return NO_WORKER_BUSY;
}

template <class AggregatorT>
void Master<AggregatorT>::task_steal()
{
	int end_count = 0;
	while(end_count < (_num_workers-1))
	{
		int request_worker_id = recv_data<int>(MPI_ANY_SOURCE, SCHEDULE_REPORT_CHANNEL);
		int target_worker_id = progress_scheduler(request_worker_id);
		send_data(target_worker_id, request_worker_id, SCHEDULE_REPORT_CHANNEL);
		if(target_worker_id == NO_WORKER_BUSY)
			end_count++;
	}
}

template <class AggregatorT>
void Master<AggregatorT>::run(const WorkerParams& params)
{
	if (dir_check(params.input_path.c_str(), params.output_path.c_str(), true, params.force_write) == -1)
	{
		terminate();
		return;
	}
	start_to_work();
	//============================================================

	//===========================init=============================
	init_timers();
	AggregatorT* agg = (AggregatorT*)get_aggregator();
	if (agg != NULL)
		agg->init();

	//============================ RUN ============================
	thread sync(&Master::context_sync, this);
	thread listen(&Master::schedule_listen,this);
	thread steal(&Master::task_steal,this);
	//ResetTimer(MASTER_TIMER);
	//StopTimer(MASTER_TIMER);
	//MasterPrintTimer("XXX Time", MASTER_TIMER);

	int end_tag = 0;
	while(end_tag < (_num_workers-1))
	{
		int tag = recv_data<int>(MPI_ANY_SOURCE, MSCOMMUN_CHANNEL);
		if(tag == DONE)
		{
			end_tag++;
		}
	}
	end_sync();

	//join the left threads
	sync.join();
	listen.join();
	steal.join();
}
