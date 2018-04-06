//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#include "global.hpp"

int _my_rank;
int _num_workers;

void init_worker(int * argc, char*** argv)
{
	int provided;
	MPI_Init_thread(argc, argv, MPI_THREAD_MULTIPLE, &provided);
	if(provided != MPI_THREAD_MULTIPLE)
	{
		printf("MPI do not Support Multiple thread\n");
		exit(0);
	}
	MPI_Comm_size(MPI_COMM_WORLD, &_num_workers);
	MPI_Comm_rank(MPI_COMM_WORLD, &_my_rank);
}

void worker_finalize()
{
	MPI_Finalize();
}

void worker_barrier()
{
	MPI_Barrier(MPI_COMM_WORLD);
}

//------------------------
// worker parameters
WorkerParams::WorkerParams()
{
	local_root = "/tmp/gminer";
	force_write = true;
	native_dispatcher = false;
}

//============================

int global_step_num;
int global_phase_num;

void* global_message_buffer = NULL;
void* global_combiner = NULL;
void* global_aggregator = NULL;
void* global_agg = NULL; //for aggregator, FinalT of last round

int global_vnum = 0;
int global_active_vnum = 0;

//currently, only 3 bits are used, others can be defined by users
char global_bor_bitmap;

void clear_bits()
{
	global_bor_bitmap = 0;
}

void set_bit(int bit)
{
	global_bor_bitmap |= (2 << bit);
}

int get_bit(int bit, char bitmap)
{
	return ((bitmap & (2 << bit)) == 0) ? 0 : 1;
}

void has_msg()
{
	set_bit(HAS_MSG_ORBIT);
}

void wake_all()
{
	set_bit(WAKE_ALL_ORBIT);
}

void force_terminate()
{
	set_bit(FORCE_TERMINATE_ORBIT);
}

//================================DISK IO (TASKQUEUE)=====================================
string PQUE_DIR;
string MERGE_DIR;

void mk_dir(const char *dir)
{
	char tmp[256];
	char *p = NULL;
	size_t len;

	snprintf(tmp, sizeof(tmp), "%s", dir);
	len = strlen(tmp);
	if(tmp[len - 1] == '/') tmp[len - 1] = '\0';
	for (p = tmp + 1; *p; p++)
	{
		if (*p == '/')
		{
			*p = 0;
			mkdir(tmp, S_IRWXU);
			*p = '/';
		}
	}
	mkdir(tmp, S_IRWXU);
}

void rm_dir(string path)
{
	DIR* dir = opendir(path.c_str());
	struct dirent * file;
	while ((file = readdir(dir)) != NULL)
	{
		if(strcmp(file->d_name, ".") == 0 || strcmp(file->d_name, "..") == 0)
			continue;
		string filename = path + "/" + file->d_name;
		remove(filename.c_str());
	}
	if (rmdir(path.c_str()) == -1)
	{
		perror ("The following error occurred");
		exit(-1);
	}
}

void check_dir(string path, bool force_write)
{
	if(access(path.c_str(), F_OK) == 0 )
	{
		if (force_write)
		{
			rm_dir(path);
			mk_dir(path.c_str());
		}
		else
		{
			cout << path <<  " already exists!" << endl;
			exit(-1);
		}
	}
	else
	{
		mk_dir(path.c_str());
	}
}

//=========================================================

//=================multi-thread============================
unsigned long long TASK_IN_MEMORY_NUM=0;
unsigned long long TASK_IN_DISK_NUM=0;

mutex global_lock;

void inc_task_num_in_disk(int num)
{
	lock_guard<mutex> lck(global_lock);
	TASK_IN_DISK_NUM += num;
}

void dec_task_num_in_disk(int num)
{
	lock_guard<mutex> lck(global_lock);
	TASK_IN_DISK_NUM -= num;
}

unsigned long long get_task_num_in_disk()
{
	lock_guard<mutex> lck(global_lock);
	return TASK_IN_DISK_NUM;
}

void inc_task_num_in_memory(int num)
{
	lock_guard<mutex> lck(global_lock);
	TASK_IN_MEMORY_NUM += num;
}

void dec_task_num_in_memory(int num)
{
	lock_guard<mutex> lck(global_lock);
	TASK_IN_MEMORY_NUM -= num;
}

unsigned long long get_task_num_in_memory()
{
	lock_guard<mutex> lck(global_lock);
	return TASK_IN_MEMORY_NUM;
}
//============================HDFS Parameters==========================
string HDFS_HOST_ADDRESS;
int HDFS_PORT;

//==========================System Parameters==========================
// max number of tasks buffered in memory before merging in priority queue
int global_merge_limit=1000;
// max number of tasks buffered in memory in pipeline
int global_taskBuf_size=1000;
// number of dimensions for signature
int global_sign_size=4;
// max number of tasks in data file of priority queue
int global_file_size=100;
// setting for merge_sort in task_seeding stage
int NUM_WAY_OF_MERGE=1000;

// num of threads in threadpool for computation
int NUM_COMP_THREAD=2;
// the size of cache in each worker
int CACHE_SIZE=1000000;
// # of tasks popped out each batch in the pipeline
int PIPE_POP_NUM=100;

// # of tasks popped out for each remote remove
int POP_NUM=100;
// thresholds of tasks to measure whether the task can be moved to other workers
int SUBG_SIZE_T=30;
double LOCAL_RATE=0.5;

// #of seconds for sleep in thread context_sync
int SLEEP_TIME=0;


void load_hdfs_config()
{
	dictionary *ini;
	double val, val_not_found = -1;
	char *str, *str_not_found = "null";

	const char* GMINER_HOME = getenv("GMINER_HOME");
	if(GMINER_HOME == NULL)
	{
		fprintf(stderr, "must conf the ENV: GMINER_HOME. exits.\n");
		exit(-1);
	}
	string conf_path(GMINER_HOME);
	conf_path.append("/gminer-conf.ini");
	ini = iniparser_load(conf_path.c_str());
	if(ini == NULL)
	{
		fprintf(stderr, "can not open %s. exits.\n", "gminer-conf.ini");
		exit(-1);
	}

	// [PATH]
	str = iniparser_getstring(ini,"PATH:HDFS_HOST_ADDRESS", str_not_found);
	if(strcmp(str, str_not_found)!=0) HDFS_HOST_ADDRESS = str;
	else
	{
		fprintf(stderr, "must enter the HDFS_HOST_ADDRESS. exits.\n");
		exit(-1);
	}

	val = iniparser_getint(ini, "PATH:HDFS_PORT", val_not_found);
	if(val!=val_not_found) HDFS_PORT=val;
	else
	{
		fprintf(stderr, "must enter the HDFS_PORT. exits.\n");
		exit(-1);
	}

	iniparser_freedict(ini);
}

void load_system_parameters(WorkerParams& param)
{
	dictionary *ini;
	double val, val_not_found = -1;
	char *str, *str_not_found = "null";

	const char* GMINER_HOME = getenv("GMINER_HOME");
	if(GMINER_HOME == NULL)
	{
		fprintf(stderr, "must conf the ENV: GMINER_HOME. exits.\n");
		exit(-1);
	}
	string conf_path(GMINER_HOME);
	conf_path.append("/gminer-conf.ini");
	ini = iniparser_load(conf_path.c_str());
	if(ini == NULL)
	{
		fprintf(stderr, "can not open %s. exits.\n", "gminer-conf.ini");
		exit(-1);
	}

	// [PATH]

	str = iniparser_getstring(ini,"PATH:HDFS_INPUT_PATH", str_not_found);
	if(strcmp(str, str_not_found)!=0) param.input_path = str;
	else
	{
		fprintf(stderr, "must enter the HDFS_INPUT_PATH. exits.\n");
		exit(-1);
	}

	str = iniparser_getstring(ini,"PATH:HDFS_OUTPUT_PATH", str_not_found);
	if(strcmp(str, str_not_found)!=0) param.output_path = str;
	else
	{
		fprintf(stderr, "must enter the HDFS_OUTPUT_PATH. exits.\n");
		exit(-1);
	}

	str = iniparser_getstring(ini,"PATH:LOCAL_TEMP_PATH", str_not_found);
	if(strcmp(str, str_not_found)!=0) param.local_root = str;
	else
	{
		fprintf(stderr, "must enter the LOCAL_TEMP_PATH. exits.\n");
		exit(-1);
	}

	val = iniparser_getboolean(ini, "", val_not_found);
	if(val!=val_not_found) param.force_write = bool(val);

	param.native_dispatcher = false;

	// [COMPUTING]
	val = iniparser_getint(ini, "COMPUTING:CACHE_SIZE", val_not_found);
	if(val!=val_not_found) CACHE_SIZE=val;

	val = iniparser_getint(ini, "COMPUTING:NUM_COMP_THREAD", val_not_found);
	if(val!=val_not_found) NUM_COMP_THREAD=val;

	val = iniparser_getint(ini, "COMPUTING:PIPE_POP_NUM", val_not_found);
	if(val!=val_not_found) PIPE_POP_NUM=val;

	// [STEALING]
	val = iniparser_getint(ini, "STEALING:POP_NUM", val_not_found);
	if(val!=val_not_found) POP_NUM=val;

	val = iniparser_getint(ini, "STEALING:SUBG_SIZE_T", val_not_found);
	if(val!=val_not_found) SUBG_SIZE_T=val;

	val = iniparser_getdouble(ini, "STEALING:LOCAL_RATE", val_not_found);
	if(val!=val_not_found) LOCAL_RATE=val;

	// [CONTEXT SYNC]
	val = iniparser_getint(ini, "SYNC:SLEEP_TIME", val_not_found);
	if(val!=val_not_found) SLEEP_TIME=val;


	iniparser_freedict(ini);
}

//=====================================================================
