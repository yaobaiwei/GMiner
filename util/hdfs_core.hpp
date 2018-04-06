//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef HDFS_CORE_HPP_
#define HDFS_CORE_HPP_

#include <hdfs.h>
#include <mpi.h>

#include <algorithm>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <string>
#include <vector>

#include "util/global.hpp"

using namespace std;


static const int HDFS_BUF_SIZE = 65536;
static const int LINE_DEFAULT_SIZE = 4096;
static const int HDFS_BLOCK_SIZE = 8388608; // 8M

//====== get File System ======

hdfsFS get_hdfs_fs();

hdfsFS get_local_fs();

//======== HDFS Delete ========

int hdfs_delete(hdfsFS& fs, const char* outdir, int flag = 1);

//====== get File Handle ======

hdfsFile get_r_handle(const char* path, hdfsFS fs);

hdfsFile get_w_handle(const char* path, hdfsFS fs);

hdfsFile get_rw_handle(const char* path, hdfsFS fs);

//====== Read line ======

//logic:
//buf[] is for batch reading from HDFS file
//line[] is a line buffer, the string length is "length", the buffer size is "size"
//after each readLine(), need to check eof(), if it's true, no line is read due to EOF
class LineReader
{
public:
	//dynamic fields
	char* line;
	int length;
	int size;

	LineReader(hdfsFS& fs, hdfsFile& handle);

	~LineReader();

	//internal use only!
	void double_linebuf();

	//internal use only!
	void line_append(const char* first, int num);

	//internal use only!
	void fill();

	bool eof();

	//user interface
	//the line starts at "line", with "length" chars
	void append_line();

	void read_line();

	char* get_line();

private:
	//static fields
	char buf_[HDFS_BUF_SIZE];
	tSize buf_pos_;
	tSize buf_size_;
	hdfsFS fs_;
	hdfsFile handle_;
	bool file_end_;
};

//====== Dir Creation ======
void dir_create(const char* outdir);

//====== Dir Check ======
int out_dir_check(const char* outdir, bool print, bool force); //returns -1 if fail, 0 if succeed

int dir_check(const char* indir, const char* outdir, bool print, bool force); //returns -1 if fail, 0 if succeed

int dir_check(vector<string>& indirs, const char* outdir, bool print, bool force); //returns -1 if fail, 0 if succeed

int dir_check(const char* indir, vector<string>& outdirs, bool print, bool force); //returns -1 if fail, 0 if succeed

int dir_check(const char* outdir, bool force); //returns -1 if fail, 0 if succeed

int dir_check(const char* indir); //returns -1 if fail, 0 if succeed

//====== Write line ======

class LineWriter
{
public:

	LineWriter(const char* path, hdfsFS fs, int me);

	~LineWriter();

	//internal use only!
	void next_hdl();

	void write_line(char* line, int num);

private:
	hdfsFS fs_;
	hdfsFile cur_hdl_;

	const char* path_;
	int me_; //-1 if there's no concept of machines (like: hadoop fs -put)
	int nxt_part_;
	int cur_size_;
};

//====== Put: local->HDFS ======

void put(const char* localpath, const char* hdfspath);

void putf(const char* localpath, const char* hdfspath); //force put, overwrites target

//====== Put: all local files under dir -> HDFS ======

void put_dir(const char* localpath, const char* hdfspath);

//====== BufferedWriter ======
struct BufferedWriter
{
public:

	BufferedWriter(const char* path, hdfsFS fs);
	BufferedWriter(const char* path, hdfsFS fs, int me);

	~BufferedWriter();

	//internal use only!
	void next_hdl();

	void check();

	void write(const char* content);

private:
	hdfsFS fs_;
	const char* path_;
	int me_; //-1 if there's no concept of machines (like: hadoop fs -put)
	int next_part_;
	vector<char> buf_;
	hdfsFile cur_hdl_;
};

//====== Dispatcher ======

struct sizedFName
{
	char* fname;
	tOffset size;

	bool operator<(const sizedFName& o) const;
};

struct sizedFString
{
	string fname;
	tOffset size;

	bool operator<(const sizedFString& o) const;
};

const char* rfind(const char* str, char delim);

vector<string>* dispatch_run(const char* in_dir, int num_slaves); //remember to "delete[] assignment" after used

//considers locality
//1. compute avg size, define it as quota
//2. sort files by size
//3. for each file, if its slave has quota, assign it to the slave
//4. for the rest, run the greedy assignment
//(libhdfs do not have location info, but we can check slaveID from fileName)
//*** NOTE: NOT SUITABLE FOR DATA "PUT" TO HDFS, ONLY FOR DATA PROCESSED BY AT LEAST ONE JOB
vector<string>* dispatch_locality(const char* in_dir, int num_slaves); //remember to "delete[] assignment" after used

vector<vector<string> >* dispatch_run(const char* in_dir); //remember to delete assignment after used

vector<vector<string> >* dispatch_run(vector<string>& in_dirs); //remember to delete assignment after used

//considers locality
//1. compute avg size, define it as quota
//2. sort files by size
//3. for each file, if its slave has quota, assign it to the slave
//4. for the rest, run the greedy assignment
//(libhdfs do not have location info, but we can check slaveID from fileName)
//*** NOTE: NOT SUITABLE FOR DATA "PUT" TO HDFS, ONLY FOR DATA PROCESSED BY AT LEAST ONE JOB
vector<vector<string> >* dispatch_locality(const char* in_dir); //remember to delete assignment after used

vector<vector<string> >* dispatch_locality(vector<string>& in_dirs); //remember to delete assignment after used

void report_assignment(vector<string>* assignment, int num_slaves);

void report_assignment(vector<vector<string> >* assignment);


#endif /* HDFS_CORE_HPP_ */
