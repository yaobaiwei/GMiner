//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef PRIORITYT_QUEUE_HPP_
#define PRIORITYT_QUEUE_HPP_

#include <atomic>
#include <deque>
#include <fstream>
#include <iostream>
#include <list>

#include "util/global.hpp"
#include "util/ioser.hpp"
#include "util/minhash.hpp"
#include "util/type.hpp"

using namespace std;


enum PQ_STAT
{
	PQ_BEFORE,
	PQ_CONTAIN,
	PQ_TAIL
};

template <class KeyT, class TaskT>
class PQueue
{
public:
	typedef ktpair<KeyT, TaskT> KTpair;

	PQueue();

	// root: the folder to store the files
	// M: the max size of tasks each file should store
	void init(string _root, int M);

	void push_back(vector<KTpair>  & tasks);

	void add_block(vector<KTpair> & tasks);

	void add_buffer(vector<KTpair> & tasks);

	bool pop_front(vector<TaskT *> & tasks);

	void close();

	void pop_front_to_remote(vector<TaskT *> & remote_tasks);

	int get_capacity();

private:

	struct file_meta
	{
		KeyT left;
		KeyT right;
		int count;		// the number of tasks in current file
		int file_no;	// the No. of current file

		file_meta(KeyT _left, KeyT _right, int _count, int _file_no)
		{
			left = _left;
			right = _right;
			count = _count;
			file_no  = _file_no;
		}
	};

	typedef typename list<file_meta>::iterator FInfoIterT;

	TaskT* pop_front();

	TaskT* front();

	void pop();

	//sub-functions of forward_key(), return position in tasks
	//search from tasks[start_pos] till next tasks[i]>=bound
	int last_lt(vector<KTpair> & tasks, KeyT bound,  int start_pos);

	//sub-functions of forward_key(), return position in tasks
	//search from tasks[start_pos] till last tasks[i] <= tgt
	int last_le(vector<KTpair> & tasks, KeyT bound,  int start_pos);

	PQ_STAT forward_fpos(KeyT cur_key);

	int forward_key(vector<KTpair> & tasks, int cur_index, PQ_STAT status);

	//clear the tasks in this function, gc the space
	void merge_to_disk(vector<KTpair> & tasks);

	void insert_before(vector<KTpair> & tasks, int start_pos, int end_pos);

	void insert_contain(vector<KTpair> & tasks, int start_pos, int end_pos);

	void insert_tail(vector<KTpair> & tasks, int start_pos, int end_pos);

	//merge a[start_pos - end_pos] into b;
	void merge_sort(vector<KTpair> & a, int start_pos, int end_pos, vector<KTpair> & b);

	//merge a, b into a;
	void merge_to(deque<KTpair> & a, vector<KTpair> & b);

	//merge a, b into a;
	void merge_to(vector<KTpair> & a, deque<KTpair> & b);

	//this is called when tasks[start_pos- end_pos].count < capacity/2, overwrite fpos file & may insert one more file before fpos
	void merge_with_file(FInfoIterT & fpos, int start_pos, int end_pos, vector<KTpair> & tasks);

	//this is called when tasks[start_pos - end_pos].count >= capacity/2, insert multiple files before fpos
	void add_files_before(FInfoIterT fpos, int start_pos, int end_pos, vector<KTpair> & tasks);

	//load the content of file pointed by fpos into fileCache
	void load_file(FInfoIterT fpos, vector<KTpair> & file_cache);

	//load the content of file pointed by fpos into fileCache
	void load_file(FInfoIterT fpos, deque<KTpair> & file_cache);

	void write_to_file(int fileNo, int start, int num, vector<KTpair> & tasks);

	void write_to_file(vector<KTpair> & tasks);

	//if there is no more file, return false;
	bool fill_file_cache();

	void set_fname(int file_no);

	void add_fmeta(vector<KTpair> & tasks);

	atomic_bool stop_;
	int capacity_; //max # of tasks allowed in a file (min allowed is capacity/2)
	string root_;
	list<file_meta> fmeta_list_;
	deque<KTpair> mem_cache_; //buffering incoming tasks for batch merging
	deque<KTpair> file_cache_; //buffering tasks in a file
	deque<TaskT *> tmp_buffer_; //a tmp buffer to store the tasks popped out from queue for remote transfer but is non-movable

	//multi-threads support
	mutex m_mutex_;
	condition_variable m_condv_;

	FInfoIterT fpos_; //position in fmetalist
	int next_file_no_; //the next fileNo for new file
	char fname_[100];
	char num_[10];
	unsigned long long task_num_;
};


#include "priority_queue.tpp"

#endif /* PRIORITYT_QUEUE_HPP_ */
