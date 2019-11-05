//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef GLOBAL_HPP_
#define GLOBAL_HPP_

#include <mpi.h>

#include <dirent.h>
#include <unistd.h>
#include <ext/hash_set>
#include <ext/hash_map>
#include <sys/stat.h>
#include <sys/stat.h>

#include <assert.h>
#include <math.h>
#include <limits.h>
#include <stddef.h>
#include <string.h>
#include <fstream>
#include <iostream>
#include <mutex>

#include "util/hdfs_core.hpp"

extern "C" {
#include "util/iniparser/iniparser.h"
}

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;
using namespace std;

//============================
// worker info

// For ZMQ usage
struct WorkerInfo
{
	string hostname;
	int port;
};

inline static string GetTcpAddress(string hostname, int port)
{
	return "tcp://" + hostname + ":" + to_string(port);
}

#ifdef PARTITION
#define MASTER_RANK 0
#else
#define MASTER_RANK (_num_workers - 1)
#endif
static int SLAVE_LEADER = 0;

//============================

extern int _my_rank;
extern int _num_workers;
extern vector<WorkerInfo> _workers_info;  // For ZMQ usage

inline int get_worker_id()
{
	return _my_rank;
}

inline int get_num_workers()
{
	return _num_workers;
}

void init_worker(int* argc, char*** argv);
void worker_finalize();
void worker_barrier();

//------------------------
// worker parameters
struct WorkerParams
{
	string local_root;
	string input_path;
	string output_path;
	bool force_write;
	bool native_dispatcher;

	WorkerParams();
};

//============================

extern int global_step_num;
inline int step_num()
{
	return global_step_num;
}

extern int global_phase_num;
inline int phase_num()
{
	return global_phase_num;
}

extern void* global_message_buffer;
inline void set_message_buffer(void* mb)
{
	global_message_buffer = mb;
}
inline void* get_message_buffer()
{
	return global_message_buffer;
}

extern void* global_combiner;
inline void set_combiner(void* cb)
{
	global_combiner = cb;
}
inline void* get_combiner()
{
	return global_combiner;
}

extern void* global_aggregator;
inline void set_aggregator(void* ag)
{
	global_aggregator = ag;
}
inline void* get_aggregator()
{
	return global_aggregator;
}

extern void* global_agg; // for aggregator, FinalT of last round
inline void* get_agg()
{
	return global_agg;
}

extern int global_vnum;
inline int& get_vnum()
{
	return global_vnum;
}

extern int global_active_vnum;
inline int& active_vnum()
{
	return global_active_vnum;
}

enum BITS
{
	HAS_MSG_ORBIT = 0,
	FORCE_TERMINATE_ORBIT = 1,
	WAKE_ALL_ORBIT = 2
};
// currently, only 3 bits are used, others can be defined by users
extern char global_bor_bitmap;

void clear_bits();
void set_bit(int bit);
int get_bit(int bit, char bitmap);
void has_msg();
void wake_all();
void force_terminate();

//================================DISK IO (TASKQUEUE)=====================================
extern string PQUE_DIR;
extern string MERGE_DIR;

void mk_dir(const char *dir);
void rm_dir(string path);
void check_dir(string path, bool force_write);
//=========================================================

//=================multi-thread============================
extern unsigned long long TASK_IN_MEMORY_NUM;
extern unsigned long long TASK_IN_DISK_NUM;

extern mutex global_lock;

void inc_task_num_in_disk(int num);
void dec_task_num_in_disk(int num);
unsigned long long get_task_num_in_disk();
void inc_task_num_in_memory(int num);
void dec_task_num_in_memory(int num);
unsigned long long get_task_num_in_memory();

//=========================TYPES FOR SYSTEM============================
typedef int VertexID;
typedef vector<VertexID> signT;

//=========================Master-Slave Model==========================
enum MSG
{
	START = 0,
	TERMINATE = 1,
	REPORT = 2,
	DONE = 3,
	NO_WORKER_BUSY = -1,
	REQUEST_VERTEX = -2,
	REQUEST_END = -3,
	REQUEST_TASK = -4
};

// offsets of ports
const int SCHEDULE_REPORT_PORT = 0;
const int SCHEDULE_HEARTBEAT_PORT = 1;
const int MSCOMMUN_PORT = 2;
const int REQUEST_PORT = 3;
const int RESPOND_PORT = 4;

const int COMM_CHANNEL_200=200;
const int MSCOMMUN_CHANNEL=205;

const int COMMUN_TIME=1;

//============================HDFS Parameters==========================
extern string HDFS_HOST_ADDRESS;
extern int HDFS_PORT;

//==========================System Parameters==========================
// max number of tasks buffered in memory before merging in priority queue
extern int global_merge_limit;
// max number of tasks buffered in memory in pipeline
extern int global_taskBuf_size;
// number of dimensions for signature
extern int global_sign_size;
// max number of tasks in data file of priority queue
extern int global_file_size;
// setting for merge_sort in task_seeding stage
extern int NUM_WAY_OF_MERGE;

// num of threads in threadpool for computation
extern int NUM_COMP_THREAD;
// the size of cache in each worker
extern int CACHE_SIZE;
// # of tasks popped out each batch in the pipeline
extern int PIPE_POP_NUM;

// # of tasks popped out for each remote remove
extern int POP_NUM;
// thresholds of tasks to measure whether the task can be moved to other workers
extern int SUBG_SIZE_T;
extern double LOCAL_RATE;

// #of seconds for sleep in thread context_sync
extern int SLEEP_TIME;


void load_hdfs_config();
void load_system_parameters(WorkerParams& param);

//=====================================================================


#endif /* GLOBAL_HPP_ */
