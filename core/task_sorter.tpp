//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class TaskT>
TaskSorter<TaskT>::TaskSorter(): hash_inited_(false) {}

template <class TaskT>
void TaskSorter<TaskT>::init(int k, int V)
{
	hash_.init(k, V);
	hash_inited_=true;
}

template <class TaskT>
void TaskSorter<TaskT>::sign_and_sort_tasks(vector<TaskT*> & tasks, vector<KTpair>  & signed_tasks)
{
	if(!hash_inited_)
	{
		cout<< "[ERROR] using hash (in TaskAuxiliary) without initialization !!!" << endl;
		exit(-1);
	}

	// 1. add signatures to tasks, to create signed_tasks
	for(int i = 0 ; i < tasks.size(); i++)
	{
		TaskT * t = tasks[i];
		vector<int> & indexes = t->to_request;
		vector<VertexID> temp;
		for(int j = 0; j < indexes.size(); j ++)
		{
			temp.push_back(t->to_pull[indexes[j]].id);
		}
		KTpair item(hash_(temp), t);
		signed_tasks.push_back(item);
	}
	// 2. sort signed_tasks to be merged
	sort(signed_tasks.begin(), signed_tasks.end());
}


template <class TaskT>
void TaskSorter<TaskT>::merge_sort_seed_tasks(PQue& pque, int num_files)
{
	int level = 0; //current level
	int fnum = num_files; //number of files in current level
	vector<ofbinstream> out;
	ifbinstream in;
	while(fnum > NUM_WAY_OF_MERGE)
	{
		if(out.size() != NUM_WAY_OF_MERGE)
			out.resize(NUM_WAY_OF_MERGE); //it may be shrinked in previous layer
		//process full batches
		int bnum = fnum / NUM_WAY_OF_MERGE; //number of full batches
		for(int i=0; i<bnum; i++)
		{
			//open out-streams
			for(int j=0; j<NUM_WAY_OF_MERGE; j++)
			{
				set_fname(MERGE_DIR, i * NUM_WAY_OF_MERGE + j, level);
				out[j].open(fname_);
			}
			//open out-stream
			set_fname(MERGE_DIR, i, level + 1);
			in.open(fname_);

			//merge out-streams into in-stream
			merge_seed_tasks(out, in);
			//delete in-streams as they are consumed
			for(int j=0; j<NUM_WAY_OF_MERGE; j++)
			{
				set_fname(MERGE_DIR, i * NUM_WAY_OF_MERGE + j, level);
				remove(fname_);
			}
		}
		//process non-full last batch
		int last_size = fnum % NUM_WAY_OF_MERGE;
		if(last_size == 0)
		{
			//no non-full last batch
			//update iterating variable "fnum"
			fnum = bnum;
		}
		else
		{
			out.resize(last_size);
			//open in-streams
			for(int j=0; j<last_size; j++)
			{
				set_fname(MERGE_DIR, bnum * NUM_WAY_OF_MERGE + j, level);
				out[j].open(fname_);
			}
			//open in-stream
			set_fname(MERGE_DIR, bnum, level + 1);
			in.open(fname_);

			//merge in-streams into out-stream
			merge_seed_tasks(out, in);
			//delete out-streams as they are consumed
			for(int j=0; j<last_size; j++)
			{
				set_fname(MERGE_DIR, bnum * NUM_WAY_OF_MERGE + j, level);
				remove(fname_);
			}
			//update iterating variable "fnum"
			fnum = bnum + 1;
		}
		//update iterating variable "level"
		level++;
	}
	last_merge_seed_tasks(pque, fnum, level);
}

template <class TaskT>
void TaskSorter<TaskT>::set_fname(string root, int file_bat, int level)
{
	//get filename
	strcpy(fname_, root.c_str());
	sprintf(num_, "/%d", file_bat);
	strcat(fname_, num_);
	if(level > 0)
	{
		sprintf(num_, "_%d", level);
		strcat(fname_, num_);
	}
}

template <class TaskT>
void TaskSorter<TaskT>::merge_seed_tasks(vector<ofbinstream>& out, ifbinstream& in) //open outside, close inside
{
	priority_queue<TaskSID> pq;
	for(int i=0; i<out.size(); i++) //init: put first element of each out-stream to heap
	{
		TaskSID tmp;
		out[i] >> tmp.ktpair;
		tmp.sid = i;
		pq.push(tmp);
	}
	while(!pq.empty())
	{
		TaskSID tmp = pq.top(); //pick min
		pq.pop();
		in << tmp.ktpair; //append to in-stream
		delete tmp.ktpair.task;

		ofbinstream& outs = out[tmp.sid];
		if(!outs.eof()) //get one more entry from the digested out-stream
		{
			TaskSID tmp2;
			outs >> tmp2.ktpair;
			tmp2.sid = tmp.sid;
			//tmp.sid remains the same as the popped entry
			pq.push(tmp2);
		}
	}
	//---
	in.close();
	for(int i=0; i<out.size(); i++)
		out[i].close();
}

template <class TaskT>
void TaskSorter<TaskT>::last_merge_seed_tasks(PQue& pque, int fnum, int level)
{
	vector<ofbinstream> out;
	queue<KTpair> buffer;
	int capacity = pque.get_capacity();

	out.resize(fnum);
	//open in-streams
	for(int i = 0 ; i < fnum ; i++)
	{
		set_fname(MERGE_DIR, i, level);
		out[i].open(fname_);
	}

	priority_queue<TaskSID> pq;
	for(int i=0; i<out.size(); i++) //init: put first element of each on-stream to heap
	{
		TaskSID tmp;
		out[i] >> tmp.ktpair;
		tmp.sid = i;
		pq.push(tmp);
	}

	while(!pq.empty())
	{
		TaskSID tmp = pq.top(); //pick min
		pq.pop();

		buffer.push(tmp.ktpair);
		if(buffer.size() >= 2 * capacity)
		{
			vector<KTpair> tasks;
			for(int i = 0 ; i < capacity; i++)
			{
				tasks.push_back(buffer.front());
				buffer.pop();
			}
			pque.add_block(tasks);
		}

		ofbinstream& outs = out[tmp.sid];
		if(!outs.eof()) //get one more entry from the digested out-stream
		{
			TaskSID tmp2;
			outs >> tmp2.ktpair;
			tmp2.sid = tmp.sid;
			//tmp.sid remains the same as the popped entry
			pq.push(tmp2);
		}
	}

	if(!buffer.empty())
	{
		vector<KTpair> tasks;
		int size = buffer.size() / 2;
		for(int i = 0 ; i < size; i ++)
		{
			tasks.push_back(buffer.front());
			buffer.pop();
		}
		pque.add_block(tasks);
		tasks.clear();

		size = buffer.size();
		for(int i = 0 ; i < size; i ++)
		{
			tasks.push_back(buffer.front());
			buffer.pop();
		}
		pque.add_block(tasks);
		tasks.clear();
	}

	for(int i=0; i<out.size(); i++)
	{
		out[i].close();
		set_fname(MERGE_DIR, i,  level);
		remove(fname_);
	}
}
