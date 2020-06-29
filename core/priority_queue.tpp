//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class KeyT, class TaskT>
PQueue<KeyT, TaskT>::PQueue(): stop_(false) {}

//root: the folder to store the files
//M: the max size of tasks each file should store
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::init(string _root, int M)
{
	root_  = _root;
	capacity_ = M;
	next_file_no_ = 0;
	task_num_ = 0;
	fpos_ = fmeta_list_.begin();
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::push_back(vector<PQueue<KeyT, TaskT>::KTpair>& tasks)
{
	m_mutex_.lock();
	int tsize = tasks.size();
	if(tsize == 0)
	{
		m_mutex_.unlock();
		return;
	}
	task_num_ += tsize;
	if(mem_cache_.size() + tasks.size() >= global_merge_limit)
	{
		merge_to(tasks, mem_cache_);
		mem_cache_.clear();
		merge_to_disk(tasks);
	}
	else
	{
		merge_to(mem_cache_, tasks);
	}
	tasks.clear();
	dec_task_num_in_memory(tsize);
	inc_task_num_in_disk(tsize);
	m_mutex_.unlock();
	m_condv_.notify_all();
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::add_block(vector<PQueue<KeyT, TaskT>::KTpair> & tasks)
{
	m_mutex_.lock();
	int tsize = tasks.size();
	task_num_ += tsize;
	write_to_file(tasks);
	add_fmeta(tasks);
	m_mutex_.unlock();
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::add_buffer(vector<PQueue<KeyT, TaskT>::KTpair> & tasks)
{
	m_mutex_.lock();
	int tsize = tasks.size();
	task_num_ += tsize;
	mem_cache_.insert(mem_cache_.end(),tasks.begin(), tasks.end());
	m_mutex_.unlock();
}

template <class KeyT, class TaskT>
bool PQueue<KeyT, TaskT>::pop_front(vector<TaskT *> & tasks)
{
	unique_lock<mutex> lck(m_mutex_);
	while(fmeta_list_.empty() && file_cache_.empty() && mem_cache_.empty() && tmp_buffer_.empty() && !stop_)
	{
		m_condv_.wait(lck);
	}
	if(stop_)
		return false;
	int pop_num;
	if(task_num_ < PIPE_POP_NUM) pop_num = task_num_;
	else pop_num = PIPE_POP_NUM;

	for(int i = 0 ; i < pop_num; i++)
	{
		TaskT * t = pop_front();
		tasks.push_back(t);
	}
	return true;
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::close()
{
	m_mutex_.lock();
	stop_ = true;
	m_mutex_.unlock();
	m_condv_.notify_all();
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::pop_front_to_remote(vector<TaskT *> & remote_tasks)
{
	m_mutex_.lock();
	int count = 0;
	while((remote_tasks.size() < POP_NUM) && (tmp_buffer_.size() <= global_taskBuf_size))
	{
		TaskT * t = front();
		if(t == NULL)
			break;
		if(t->movable())
		{
			count++;
			remote_tasks.push_back(t);
		}
		else
		{
			tmp_buffer_.push_back(t);
		}
		pop();
	}
	task_num_ -= count;
	dec_task_num_in_disk(count);
	m_mutex_.unlock();
}

//private:
template <class KeyT, class TaskT>
TaskT* PQueue<KeyT, TaskT>::pop_front()
{
	TaskT* t;
	if(!tmp_buffer_.empty())
	{
		t = tmp_buffer_.front();
		tmp_buffer_.pop_front();
	}
	else if(!file_cache_.empty())
	{
		KTpair elem = file_cache_.front();
		file_cache_.pop_front();
		t = elem.task;
	}
	else if(fill_file_cache())
	{
		KTpair elem = file_cache_.front();
		file_cache_.pop_front();
		t = elem.task;
	}
	else
	{
		KTpair elem = mem_cache_.front();
		mem_cache_.pop_front();
		t = elem.task;
	}
	task_num_--;
	dec_task_num_in_disk(1);
	inc_task_num_in_memory(1); //TASK_IN_MEMORY_NUM++;
	return t;
}

template <class KeyT, class TaskT>
TaskT* PQueue<KeyT, TaskT>::front()
{
	if(file_cache_.empty())
	{
		if(!fill_file_cache())
		{
			if(mem_cache_.empty())
				return NULL;
			KTpair elem = mem_cache_.front();
			return elem.task;
		}
	}
	KTpair elem = file_cache_.front();
	return elem.task;
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::pop()
{
	if(file_cache_.empty())
	{
		mem_cache_.pop_front();
	}
	else
	{
		file_cache_.pop_front();
	}
}


//sub-functions of forward_key(), return position in tasks
//search from tasks[start_pos] till next tasks[i]>=bound
template <class KeyT, class TaskT>
int PQueue<KeyT, TaskT>::last_lt(vector<PQueue<KeyT, TaskT>::KTpair> & tasks, KeyT bound,  int start_pos)
{
	int i = start_pos;
	while((i < tasks.size()) &&(tasks[i].key <  bound))
	{
		i++;
	}
	return i -1;
}

//sub-functions of forward_key(), return position in tasks
//search from tasks[start_pos] till last tasks[i] <= tgt
template <class KeyT, class TaskT>
int PQueue<KeyT, TaskT>::last_le(vector<PQueue<KeyT, TaskT>::KTpair> & tasks, KeyT bound,  int start_pos)
{
	int i = start_pos;
	while((i < tasks.size()) && (tasks[i].key <= bound))
	{
		i++;
	}
	return i -1;
}

template <class KeyT, class TaskT>
PQ_STAT PQueue<KeyT, TaskT>::forward_fpos(KeyT cur_key)
{
	while((fpos_ != fmeta_list_.end()) && (cur_key > fpos_->right ))
		fpos_++ ;
	if(fpos_ == fmeta_list_.end())
		return PQ_TAIL;
	if(cur_key < fpos_->left)
		return PQ_BEFORE;
	return PQ_CONTAIN;
}

template <class KeyT, class TaskT>
int PQueue<KeyT, TaskT>::forward_key(vector<PQueue<KeyT, TaskT>::KTpair> & tasks, int cur_index, PQ_STAT status)
{
	switch(status)
	{
	case PQ_BEFORE:
		return last_lt(tasks, fpos_->left, cur_index);
	case PQ_CONTAIN:
		return last_le(tasks, fpos_->right, cur_index);
	case PQ_TAIL:
		return tasks.size() - 1;
	default:
		return -1;
	}
}

//clear the tasks in this function, gc the space
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::merge_to_disk(vector<PQueue<KeyT, TaskT>::KTpair> & tasks)
{
	fpos_ = fmeta_list_.begin();
	int cur_task = 0 ;
	int tasksize = tasks.size();
	while( cur_task< tasksize )
	{
		PQ_STAT stat = forward_fpos(tasks[cur_task].key);
		int ub_task = forward_key(tasks, cur_task, stat);
		if(stat == PQ_BEFORE)
		{
			insert_before(tasks, cur_task, ub_task);
		}
		else if(stat == PQ_CONTAIN)
		{
			insert_contain(tasks, cur_task, ub_task);
		}
		else if(stat == PQ_TAIL)
		{
			insert_tail(tasks, cur_task, ub_task);
		}
		cur_task = ub_task + 1;
	}
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::insert_before(vector<PQueue<KeyT, TaskT>::KTpair> & tasks, int start_pos, int end_pos)
{
	int totalsize = end_pos  - start_pos + 1;
	if( totalsize >= capacity_/2 )
	{
		add_files_before(fpos_,  start_pos, end_pos, tasks);
	}
	else
	{
		merge_with_file(fpos_,  start_pos, end_pos, tasks);
	}
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::insert_contain(vector<PQueue<KeyT, TaskT>::KTpair> & tasks, int start_pos, int end_pos)
{
	vector<PQueue<KeyT, TaskT>::KTpair> merge;
	load_file(fpos_, merge);
	remove(fname_);
	merge_sort(tasks, start_pos, end_pos, merge);
	FInfoIterT pre_pos = fpos_;
	fpos_++;
	fmeta_list_.erase(pre_pos);
	add_files_before(fpos_,  0, merge.size()-1, merge);
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::insert_tail(vector<PQueue<KeyT, TaskT>::KTpair> & tasks, int start_pos, int end_pos)
{
	int totalsize = end_pos  - start_pos + 1;
	if( totalsize >= capacity_/2 )
	{
		//merge to the previous file
		add_files_before(fpos_,  start_pos, end_pos, tasks);
	}
	else
	{
		merge_with_file(--fmeta_list_.end(),  start_pos, end_pos, tasks);
	}
}

//merge a[start_pos - end_pos] into b;
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::merge_sort(vector<PQueue<KeyT, TaskT>::KTpair> & a, int start_pos, int end_pos, vector<PQueue<KeyT, TaskT>::KTpair> & b)
{
	vector<PQueue<KeyT, TaskT>::KTpair> buffer;
	int len_b = b.size();
	buffer.resize(end_pos - start_pos + 1 + len_b);
	int i  = start_pos, j = 0, k = 0;
	while(i <= end_pos  && j < len_b)
	{
		if(a[i] <  b[j])
		{
			buffer[k++]  = a[i++];
		}
		else
		{
			buffer[k++] = b[j++];
		}
	}
	while(i <= end_pos)
		buffer[k++] = a[i++];
	while(j < len_b)
		buffer[k++] = b[j++];

	buffer.swap(b);
}

//merge a, b into a;
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::merge_to(deque<PQueue<KeyT, TaskT>::KTpair> & a, vector<PQueue<KeyT, TaskT>::KTpair> & b)
{
	deque<PQueue<KeyT, TaskT>::KTpair> buffer;
	int len_a = a.size();
	int len_b = b.size();
	buffer.resize(len_a + len_b);
	int i  = 0, j = 0, k = 0;
	while(i < len_a  && j < len_b)
	{
		if(a[i] <  b[j])
		{
			buffer[k++]  = a[i++];
		}
		else
		{
			buffer[k++] = b[j++];
		}
	}
	while(i < len_a)
		buffer[k++] = a[i++];
	while(j < len_b)
		buffer[k++] = b[j++];

	buffer.swap(a);
}

//merge a, b into a;
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::merge_to(vector<PQueue<KeyT, TaskT>::KTpair> & a, deque<PQueue<KeyT, TaskT>::KTpair> & b)
{
	vector<PQueue<KeyT, TaskT>::KTpair> buffer;
	int len_a = a.size();
	int len_b = b.size();
	buffer.resize(len_a + len_b);
	int i  = 0, j = 0, k = 0;
	while(i < len_a  && j < len_b)
	{
		if(a[i] <  b[j])
		{
			buffer[k++]  = a[i++];
		}
		else
		{
			buffer[k++] = b[j++];
		}
	}
	while(i < len_a)
		buffer[k++] = a[i++];
	while(j < len_b)
		buffer[k++] = b[j++];

	buffer.swap(a);
}

//this is called when tasks[start_pos- end_pos].count < capacity/2, overwrite fpos file & may insert one more file before fpos
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::merge_with_file(FInfoIterT & fpos, int start_pos, int end_pos, vector<PQueue<KeyT, TaskT>::KTpair> & tasks)
{
	vector<PQueue<KeyT, TaskT>::KTpair> merge;
	load_file(fpos, merge);
	merge_sort(tasks, start_pos, end_pos, merge);

	int totalsize = merge.size();
	int numfile = (totalsize > capacity_) ? 2 : 1;
	int filesize = totalsize / numfile;

	if(numfile == 2)
	{
		write_to_file(next_file_no_, 0, filesize, merge);
		file_meta meta(merge[0].key, merge[filesize -1].key, filesize, next_file_no_++);
		fmeta_list_.insert(fpos, meta);

		int filesize2 = totalsize - filesize;
		write_to_file(fpos->file_no, filesize, filesize2, merge);
		fpos->left = merge[filesize].key;
		fpos->right = merge[totalsize - 1].key;
		fpos->count = filesize2;
		fpos--;
	}
	else    //filesize == 1
	{
		write_to_file(fpos->file_no, 0, filesize, merge);
		fpos->left = merge[0].key;
		fpos->right = merge[filesize - 1].key;
		fpos->count = filesize;
	}

}

//this is called when tasks[start_pos - end_pos].count >= capacity/2, insert multiple files before fpos
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::add_files_before(FInfoIterT fpos, int start_pos, int end_pos, vector<PQueue<KeyT, TaskT>::KTpair> & tasks)
{
	int totalsize = end_pos  - start_pos + 1;
	int numfile = totalsize / capacity_ + ( totalsize % capacity_ ? 1 : 0);

	int filesize = totalsize / numfile;
	int mod = totalsize % numfile; //number of files with one more task
	numfile -= mod; //number of other tasks

	int fsize_plus1 = filesize + 1;
	while(mod--)
	{
		write_to_file(next_file_no_, start_pos,  fsize_plus1, tasks);
		file_meta meta(tasks[start_pos].key, tasks[start_pos + filesize].key, fsize_plus1, next_file_no_++);
		fmeta_list_.insert(fpos, meta);
		start_pos += (fsize_plus1);
	}

	while(numfile--)
	{
		write_to_file(next_file_no_, start_pos, filesize, tasks);
		file_meta meta(tasks[start_pos].key, tasks[start_pos + filesize - 1].key, filesize, next_file_no_++);
		fmeta_list_.insert(fpos, meta);
		start_pos += filesize ;
	}
}

//load the content of file pointed by fpos into fileCache
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::load_file(FInfoIterT fpos, vector<PQueue<KeyT, TaskT>::KTpair> & file_cache)
{
	set_fname(fpos->file_no);
	struct stat statbuf;
	stat(fname_, &statbuf);
	int fsize = statbuf.st_size;
	ifstream in(fname_);
	char * buff  = new char[fsize];
	in.read(buff, fsize);
	in.close();

	obinstream um(buff, fsize);
	while(!um.end())
	{
		PQueue<KeyT, TaskT>::KTpair item;
		um >> item;
		file_cache.push_back(item);
	}
}

//load the content of file pointed by fpos into fileCache
template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::load_file(FInfoIterT fpos, deque<PQueue<KeyT, TaskT>::KTpair> & file_cache)
{
	set_fname(fpos->file_no);
	struct stat statbuf;
	stat(fname_, &statbuf);
	int fsize = statbuf.st_size;
	ifstream in(fname_);
	char * buff  = new char[fsize];
	in.read(buff, fsize);
	in.close();

	obinstream um(buff, fsize);
	while(!um.end())
	{
		PQueue<KeyT, TaskT>::KTpair item;
		um >> item;
		file_cache.push_back(item);
	}
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::write_to_file(int fileNo, int start, int num, vector<PQueue<KeyT, TaskT>::KTpair> & tasks)
{
	ifbinstream fin;
	set_fname(fileNo);
	fin.open(fname_);
	for (int i = start; i < start + num; i++)
	{
		fin << tasks[i];
		delete tasks[i].task;
	}
	fin.close();
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::write_to_file(vector<PQueue<KeyT, TaskT>::KTpair> & tasks)
{
	ifbinstream fin;
	set_fname(next_file_no_);
	fin.open(fname_);
	for (int i = 0; i < tasks.size(); i++)
	{
		fin << tasks[i];
		delete tasks[i].task;
	}
	fin.close();
}

//if there is no more file, return false;
template <class KeyT, class TaskT>
bool PQueue<KeyT, TaskT>::fill_file_cache()
{
	if(fmeta_list_.empty())
		return false;
	FInfoIterT iter = fmeta_list_.begin();
	load_file(iter, file_cache_);
	remove(fname_);
	fmeta_list_.erase(iter);
	return true;
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::set_fname(int file_no)
{
	strcpy(fname_, root_.c_str());
	sprintf(num_, "/%d", file_no);
	strcat(fname_, num_);
}

template <class KeyT, class TaskT>
void PQueue<KeyT, TaskT>::add_fmeta(vector<KTpair> & tasks)
{
	int size = tasks.size();
	file_meta meta(tasks[0].key, tasks[size -1].key, size, next_file_no_++);
	fmeta_list_.insert(fpos_, meta);
}

template <class KeyT, class TaskT>
int PQueue<KeyT, TaskT>::get_capacity()
{
	return capacity_;
}
