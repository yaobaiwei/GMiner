//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu

#include "hdfs_core.hpp"

const char* newLine = "\n";

//====== get File System ======

hdfsFS get_hdfs_fs()
{
#ifdef USE_HDFS2_
	hdfsBuilder * bld = hdfsNewBuilder();
	hdfsBuilderSetNameNode(bld, HDFS_HOST_ADDRESS.c_str());
	hdfsBuilderSetNameNodePort(bld, HDFS_PORT);
	hdfsFS fs = hdfsBuilderConnect(bld);
	if (!fs)
	{
		fprintf(stderr, "Failed to connect to HDFS!\n");
		MPI_Abort(MPI_COMM_WORLD, 1);
	}
#else
	hdfsFS fs = hdfsConnect(HDFS_HOST_ADDRESS.c_str(), HDFS_PORT);
	if (!fs)
	{
		fprintf(stderr, "Failed to connect to HDFS!\n");
		exit(-1);
	}
#endif /* USE_HDFS2_ */
	return fs;
}

hdfsFS get_local_fs()
{
#ifdef USE_HDFS2_
	hdfsBuilder * bld = hdfsNewBuilder();
	hdfsBuilderSetNameNode(bld, NULL);
	hdfsFS lfs = hdfsBuilderConnect(bld);
	if (!lfs)
	{
		fprintf(stderr, "Failed to connect to 'local' FS!\n");
		MPI_Abort(MPI_COMM_WORLD, 1);
	}
#else
	hdfsFS lfs = hdfsConnect(NULL, 0);
	if (!lfs)
	{
		fprintf(stderr, "Failed to connect to 'local' FS!\n");
		exit(-1);
	}
#endif /* USE_HDFS2_ */
	return lfs;
}

//======== HDFS Delete ========

int hdfs_delete(hdfsFS& fs, const char* outdir, int flag)
{
#ifdef USE_HDFS2_
	return hdfsDelete(fs, outdir, flag);
#else
	return hdfsDelete(fs, outdir);
#endif /* USE_HDFS2_ */
}

//====== get File Handle ======

hdfsFile get_r_handle(const char* path, hdfsFS fs)
{
	hdfsFile hdl = hdfsOpenFile(fs, path, O_RDONLY | O_CREAT, 0, 0, 0);
	if (!hdl)
	{
		fprintf(stderr, "Failed to open %s for reading!\n", path);
		exit(-1);
	}
	return hdl;
}

hdfsFile get_w_handle(const char* path, hdfsFS fs)
{
	hdfsFile hdl = hdfsOpenFile(fs, path, O_WRONLY | O_CREAT, 0, 0, 0);
	if (!hdl)
	{
		fprintf(stderr, "Failed to open %s for writing!\n", path);
		exit(-1);
	}
	return hdl;
}

hdfsFile get_rw_handle(const char* path, hdfsFS fs)
{
	hdfsFile hdl = hdfsOpenFile(fs, path, O_RDWR | O_CREAT, 0, 0, 0);
	if (!hdl)
	{
		fprintf(stderr, "Failed to open %s!\n", path);
		exit(-1);
	}
	return hdl;
}

//====== Read line ======

//logic:
//buf[] is for batch reading from HDFS file
//line[] is a line buffer, the string length is "length", the buffer size is "size"
//after each readLine(), need to check eof(), if it's true, no line is read due to EOF
//struct LineReader

LineReader::LineReader(hdfsFS& fs, hdfsFile& handle)
	: buf_pos_(0)
	, length(0)
	, size(LINE_DEFAULT_SIZE)
{
	fs_ = fs;
	handle_ = handle;
	file_end_ = false;
	fill();
	line = (char*)malloc(LINE_DEFAULT_SIZE * sizeof(char));
}

LineReader::~LineReader()
{
	free(line);
}

//internal use only!
void LineReader::double_linebuf()
{
	size *= 2;
	line = (char*)realloc(line, size * sizeof(char));
}

//internal use only!
void LineReader::line_append(const char* first, int num)
{
	while (length + num + 1 > size)
		double_linebuf();
	memcpy(line + length, first, num);
	length += num;
}

//internal use only!
void LineReader::fill()
{
	buf_size_ = hdfsRead(fs_, handle_, buf_, HDFS_BUF_SIZE);
	if (buf_size_ == -1)
	{
		fprintf(stderr, "Read Failure!\n");
		exit(-1);
	}
	buf_pos_ = 0;
	if (buf_size_ < HDFS_BUF_SIZE)
		file_end_ = true;
}

bool LineReader::eof()
{
	return length == 0 && file_end_;
}

//user interface
//the line starts at "line", with "length" chars
void LineReader::append_line()
{
	if (buf_pos_ == buf_size_)
		return;
	char* pch = (char*)memchr(buf_ + buf_pos_, '\n', buf_size_ - buf_pos_);
	if (pch == NULL)
	{
		line_append(buf_ + buf_pos_, buf_size_ - buf_pos_);
		buf_pos_ = buf_size_;
		if (!file_end_)
			fill();
		else
			return;
		pch = (char*)memchr(buf_, '\n', buf_size_);
		while (pch == NULL)
		{
			line_append(buf_, buf_size_);
			if (!file_end_)
				fill();
			else
				return;
			pch = (char*)memchr(buf_, '\n', buf_size_);
		}
	}
	int validLen = pch - buf_ - buf_pos_;
	line_append(buf_ + buf_pos_, validLen);
	buf_pos_ += validLen + 1; //+1 to skip '\n'
	if (buf_pos_ == buf_size_)
	{
		if (!file_end_)
			fill();
		else
			return;
	}
}

void LineReader::read_line()
{
	length = 0;
	append_line();
}

char* LineReader::get_line()
{
	line[length] = '\0';
	return line;
}

//====== Dir Creation ======
void dir_create(const char* outdir)
{
	hdfsFS fs = get_hdfs_fs();
	int created = hdfsCreateDirectory(fs, outdir);
	if (created == -1)
	{
		fprintf(stderr, "Failed to create folder %s!\n", outdir);
		exit(-1);
	}
	hdfsDisconnect(fs);
}

//====== Dir Check ======
int out_dir_check(const char* outdir, bool print, bool force) //returns -1 if fail, 0 if succeed
{
	hdfsFS fs = get_hdfs_fs();
	if (hdfsExists(fs, outdir) == 0)
	{
		if (force)
		{
			if (hdfs_delete(fs, outdir) == -1)
			{
				if (print)
					fprintf(stderr, "Error deleting %s!\n", outdir);
				exit(-1);
			}
			int created = hdfsCreateDirectory(fs, outdir);
			if (created == -1)
			{
				if (print)
					fprintf(stderr, "Failed to create folder %s!\n", outdir);
				exit(-1);
			}
		}
		else
		{
			if (print)
				fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
			hdfsDisconnect(fs);
			return -1;
		}
	}
	else
	{
		int created = hdfsCreateDirectory(fs, outdir);
		if (created == -1)
		{
			if (print)
				fprintf(stderr, "Failed to create folder %s!\n", outdir);
			exit(-1);
		}
	}
	hdfsDisconnect(fs);
	return 0;
}

//====== Dir Check ======
int dir_check(const char* indir, const char* outdir, bool print, bool force) //returns -1 if fail, 0 if succeed
{
	hdfsFS fs = get_hdfs_fs();
	if (hdfsExists(fs, indir) != 0)
	{
		if (print)
			fprintf(stderr, "Input path \"%s\" does not exist!\n", indir);
		hdfsDisconnect(fs);
		return -1;
	}
	if (hdfsExists(fs, outdir) == 0)
	{
		if (force)
		{
			if (hdfs_delete(fs, outdir) == -1)
			{
				if (print)
					fprintf(stderr, "Error deleting %s!\n", outdir);
				exit(-1);
			}
			int created = hdfsCreateDirectory(fs, outdir);
			if (created == -1)
			{
				if (print)
					fprintf(stderr, "Failed to create folder %s!\n", outdir);
				exit(-1);
			}
		}
		else
		{
			if (print)
				fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
			hdfsDisconnect(fs);
			return -1;
		}
	}
	else
	{
		int created = hdfsCreateDirectory(fs, outdir);
		if (created == -1)
		{
			if (print)
				fprintf(stderr, "Failed to create folder %s!\n", outdir);
			exit(-1);
		}
	}
	hdfsDisconnect(fs);
	return 0;
}

int dir_check(vector<string>& indirs, const char* outdir, bool print, bool force) //returns -1 if fail, 0 if succeed
{
	hdfsFS fs = get_hdfs_fs();
	for (int i = 0; i < indirs.size(); i++)
	{
		const char* indir = indirs[i].c_str();
		if (hdfsExists(fs, indir) != 0)
		{
			if (print)
				fprintf(stderr, "Input path \"%s\" does not exist!\n", indir);
			hdfsDisconnect(fs);
			return -1;
		}
	}
	if (hdfsExists(fs, outdir) == 0)
	{
		if (force)
		{
			if (hdfs_delete(fs, outdir) == -1)
			{
				if (print)
					fprintf(stderr, "Error deleting %s!\n", outdir);
				exit(-1);
			}
			int created = hdfsCreateDirectory(fs, outdir);
			if (created == -1)
			{
				if (print)
					fprintf(stderr, "Failed to create folder %s!\n", outdir);
				exit(-1);
			}
		}
		else
		{
			if (print)
				fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
			hdfsDisconnect(fs);
			return -1;
		}
	}
	else
	{
		int created = hdfsCreateDirectory(fs, outdir);
		if (created == -1)
		{
			if (print)
				fprintf(stderr, "Failed to create folder %s!\n", outdir);
			exit(-1);
		}
	}
	hdfsDisconnect(fs);
	return 0;
}

int dir_check(const char* indir, vector<string>& outdirs, bool print, bool force) //returns -1 if fail, 0 if succeed
{
	hdfsFS fs = get_hdfs_fs();
	if (hdfsExists(fs, indir) != 0)
	{
		if (print)
			fprintf(stderr, "Input path \"%s\" does not exist!\n", indir);
		hdfsDisconnect(fs);
		return -1;
	}
	for (int i = 0; i < outdirs.size(); i++)
	{
		const char * outdir = outdirs[i].c_str();
		if (hdfsExists(fs, outdir) == 0)
		{
			if (force)
			{
				if (hdfs_delete(fs, outdir) == -1)
				{
					if (print)
						fprintf(stderr, "Error deleting %s!\n", outdir);
					exit(-1);
				}
				int created = hdfsCreateDirectory(fs, outdir);
				if (created == -1)
				{
					if (print)
						fprintf(stderr, "Failed to create folder %s!\n", outdir);
					exit(-1);
				}
			}
			else
			{
				if (print)
					fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
				hdfsDisconnect(fs);
				return -1;
			}
		}
		else
		{
			int created = hdfsCreateDirectory(fs, outdir);
			if (created == -1)
			{
				if (print)
					fprintf(stderr, "Failed to create folder %s!\n", outdir);
				exit(-1);
			}
		}
	}

	hdfsDisconnect(fs);
	return 0;
}

int dir_check(const char* outdir, bool force) //returns -1 if fail, 0 if succeed
{
	hdfsFS fs = get_hdfs_fs();
	if (hdfsExists(fs, outdir) == 0)
	{
		if (force)
		{
			if (hdfs_delete(fs, outdir) == -1)
			{
				fprintf(stderr, "Error deleting %s!\n", outdir);
				exit(-1);
			}
			int created = hdfsCreateDirectory(fs, outdir);
			if (created == -1)
			{
				fprintf(stderr, "Failed to create folder %s!\n", outdir);
				exit(-1);
			}
		}
		else
		{
			fprintf(stderr, "Output path \"%s\" already exists!\n", outdir);
			hdfsDisconnect(fs);
			return -1;
		}
	}
	else
	{
		int created = hdfsCreateDirectory(fs, outdir);
		if (created == -1)
		{
			fprintf(stderr, "Failed to create folder %s!\n", outdir);
			exit(-1);
		}
	}
	hdfsDisconnect(fs);
	return 0;
}

int dir_check(const char* indir) //returns -1 if fail, 0 if succeed
{
	hdfsFS fs = get_hdfs_fs();
	if (hdfsExists(fs, indir) != 0)
	{
		fprintf(stderr, "Input path \"%s\" does not exist!\n", indir);
		hdfsDisconnect(fs);
		return -1;
	}
	else
	{
		hdfsDisconnect(fs);
		return 0;
	}
}

//====== Write line ======
//struct LineWriter

LineWriter::LineWriter(const char* path, hdfsFS fs, int me)
	: nxt_part_(0)
	, cur_size_(0)
{
	path_ = path;
	fs_ = fs;
	me_ = me;
	cur_hdl_ = NULL;
	//===============================
	//if(overwrite==true) readDirForce();
	//else readDirCheck();
	//===============================
	//1. cannot use above, otherwise multiple dir check/delete will be done during parallel writing
	//2. before calling the constructor, make sure "path" does not exist
	next_hdl();
}

LineWriter::~LineWriter()
{
	if (hdfsFlush(fs_, cur_hdl_))
	{
		fprintf(stderr, "Failed to 'flush' %s\n", path_);
		exit(-1);
	}
	hdfsCloseFile(fs_, cur_hdl_);
}

//internal use only!
void LineWriter::next_hdl()
{
	//set fileName
	char fname[20];
	strcpy(fname, "part_");
	char buffer[10];
	if (me_ >= 0)
	{
		sprintf(buffer, "%d", me_);
		strcat(fname, buffer);
		strcat(fname, "_");
	}
	sprintf(buffer, "%d", nxt_part_);
	strcat(fname, buffer);
	//flush old file
	if (nxt_part_ > 0)
	{
		if (hdfsFlush(fs_, cur_hdl_))
		{
			fprintf(stderr, "Failed to 'flush' %s\n", path_);
			exit(-1);
		}
		hdfsCloseFile(fs_, cur_hdl_);
	}
	//open new file
	nxt_part_++;
	cur_size_ = 0;
	char* file_path = new char[strlen(path_) + strlen(fname) + 2];
	strcpy(file_path, path_);
	strcat(file_path, "/");
	strcat(file_path, fname);
	cur_hdl_ = get_w_handle(file_path, fs_);
	delete[] file_path;
}

void LineWriter::write_line(char* line, int num)
{
	if (cur_size_ + num + 1 > HDFS_BLOCK_SIZE) //+1 because of '\n'
	{
		next_hdl();
	}
	tSize numWritten = hdfsWrite(fs_, cur_hdl_, line, num);
	if (numWritten == -1)
	{
		fprintf(stderr, "Failed to write file!\n");
		exit(-1);
	}
	cur_size_ += numWritten;
	numWritten = hdfsWrite(fs_, cur_hdl_, newLine, 1);
	if (numWritten == -1)
	{
		fprintf(stderr, "Failed to create a new line!\n");
		exit(-1);
	}
	cur_size_ += 1;
}

//====== Put: local->HDFS ======

void put(const char* localpath, const char* hdfspath)
{
	if (dir_check(hdfspath, false) == -1)
		return;
	hdfsFS fs = get_hdfs_fs();
	hdfsFS lfs = get_local_fs();

	hdfsFile in = get_r_handle(localpath, lfs);
	LineReader* reader = new LineReader(lfs, in);
	LineWriter* writer = new LineWriter(hdfspath, fs, -1);
	while (true)
	{
		reader->read_line();
		if (!reader->eof())
		{
			writer->write_line(reader->line, reader->length);
		}
		else
			break;
	}
	hdfsCloseFile(lfs, in);
	delete reader;
	delete writer;

	hdfsDisconnect(lfs);
	hdfsDisconnect(fs);
}

void putf(const char* localpath, const char* hdfspath) //force put, overwrites target
{
	dir_check(hdfspath, true);
	hdfsFS fs = get_hdfs_fs();
	hdfsFS lfs = get_local_fs();

	hdfsFile in = get_r_handle(localpath, lfs);
	LineReader* reader = new LineReader(lfs, in);
	LineWriter* writer = new LineWriter(hdfspath, fs, -1);
	while (true)
	{
		reader->read_line();
		if (!reader->eof())
		{
			writer->write_line(reader->line, reader->length);
		}
		else
			break;
	}
	hdfsCloseFile(lfs, in);
	delete reader;
	delete writer;

	hdfsDisconnect(lfs);
	hdfsDisconnect(fs);
}

//====== Put: all local files under dir -> HDFS ======

void put_dir(const char* localpath, const char* hdfspath)
{
	if (dir_check(hdfspath, false) == -1)
		return;
	hdfsFS fs = get_hdfs_fs();
	hdfsFS lfs = get_local_fs();
	int num_files;
	hdfsFileInfo* fileinfo = hdfsListDirectory(lfs, localpath, &num_files);
	if (fileinfo == NULL)
	{
		fprintf(stderr, "Failed to list directory %s!\n", localpath);
		exit(-1);
	}
	//------
	LineWriter* writer = new LineWriter(hdfspath, fs, -1);
	for (int i = 0; i < num_files; i++)
	{
		if (fileinfo[i].mKind == kObjectKindFile)
		{
			cout << "Putting file: " << fileinfo[i].mName << endl;
			hdfsFile in = get_r_handle(fileinfo[i].mName, lfs);
			LineReader* reader = new LineReader(lfs, in);
			while (true)
			{
				reader->read_line();
				if (!reader->eof())
				{
					writer->write_line(reader->line, reader->length);
				}
				else
					break;
			}
			hdfsCloseFile(lfs, in);
			delete reader;
		}
	}
	//------
	hdfsFreeFileInfo(fileinfo, num_files);
	delete writer;
	hdfsDisconnect(lfs);
	hdfsDisconnect(fs);
}

//====== BufferedWriter ======
//struct BufferedWriter

BufferedWriter::BufferedWriter(const char* path, hdfsFS fs)
{
	next_part_ = 0;
	path_ = path;
	fs_ = fs;
	me_ = -1;
	cur_hdl_ = get_w_handle(path_, fs_);
}

BufferedWriter::BufferedWriter(const char* path, hdfsFS fs, int me)
{
	next_part_ = 0;
	path_ = path;
	fs_ = fs;
	me_ = me;
	cur_hdl_ = NULL;
	next_hdl();
}

BufferedWriter::~BufferedWriter()
{
	tSize numWritten = hdfsWrite(fs_, cur_hdl_, &buf_[0], buf_.size());
	if (numWritten == -1)
	{
		fprintf(stderr, "Failed to write file!\n");
		exit(-1);
	}
	buf_.clear();

	if (hdfsFlush(fs_, cur_hdl_))
	{
		fprintf(stderr, "Failed to 'flush' %s\n", path_);
		exit(-1);
	}
	hdfsCloseFile(fs_, cur_hdl_);
}

//internal use only!
void BufferedWriter::next_hdl()
{
	//set fileName
	char fname[20];

	if (me_ >= 0)
	{
		sprintf(fname, "part_%d_%d", me_, next_part_);
	}
	else
	{
		sprintf(fname, "part_%d", next_part_);
	}

	//flush old file
	if (next_part_ > 0)
	{
		if (hdfsFlush(fs_, cur_hdl_))
		{
			fprintf(stderr, "Failed to 'flush' %s\n", path_);
			exit(-1);
		}
		hdfsCloseFile(fs_, cur_hdl_);
	}
	//open new file
	next_part_++;
	char* file_path = new char[strlen(path_) + strlen(fname) + 2];
	sprintf(file_path, "%s/%s", path_, fname);
	cur_hdl_ = get_w_handle(file_path, fs_);
	delete[] file_path;
}

void BufferedWriter::check()
{
	if (buf_.size() >= HDFS_BLOCK_SIZE)
	{
		tSize numWritten = hdfsWrite(fs_, cur_hdl_, &buf_[0], buf_.size());
		if (numWritten == -1)
		{
			fprintf(stderr, "Failed to write file!\n");
			exit(-1);
		}
		buf_.clear();
		if (me_ != -1) // -1 means "output in the specified file only"
		{
			next_hdl();
		}
	}
}

void BufferedWriter::write(const char* content)
{
	int len = strlen(content);
	buf_.insert(buf_.end(), content, content + len);
}

//====== Dispatcher ======

//struct sizedFName
bool sizedFName::operator<(const sizedFName& o) const
{
	return size > o.size; //large file goes first
}

//struct sizedFString
bool sizedFString::operator<(const sizedFString& o) const
{
	return size > o.size; //large file goes first
}

const char* rfind(const char* str, char delim)
{
	int len = strlen(str);
	int pos = 0;
	for (int i = len - 1; i >= 0; i--)
	{
		if (str[i] == delim)
		{
			pos = i;
			break;
		}
	}
	return str + pos;
}

vector<string>* dis_patch_run(const char* in_dir, int num_slaves) //remember to "delete[] assignment" after used
{
	//locality is not considered for simplicity
	vector<string>* assignment = new vector<string>[num_slaves];
	hdfsFS fs = get_hdfs_fs();
	int num_files;
	hdfsFileInfo* fileinfo = hdfsListDirectory(fs, in_dir, &num_files);
	if (fileinfo == NULL)
	{
		fprintf(stderr, "Failed to list directory %s!\n", in_dir);
		exit(-1);
	}
	tOffset* assigned = new tOffset[num_slaves];
	for (int i = 0; i < num_slaves; i++)
		assigned[i] = 0;
	//sort files by size
	vector<sizedFName> sizedfile;
	for (int i = 0; i < num_files; i++)
	{
		if (fileinfo[i].mKind == kObjectKindFile)
		{
			sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
			sizedfile.push_back(cur);
		}
	}
	sort(sizedfile.begin(), sizedfile.end());
	//allocate files to slaves
	vector<sizedFName>::iterator it;
	for (it = sizedfile.begin(); it != sizedfile.end(); ++it)
	{
		int min = 0;
		tOffset minSize = assigned[0];
		for (int j = 1; j < num_slaves; j++)
		{
			if (minSize > assigned[j])
			{
				min = j;
				minSize = assigned[j];
			}
		}
		assignment[min].push_back(it->fname);
		assigned[min] += it->size;
	}
	delete[] assigned;
	hdfsFreeFileInfo(fileinfo, num_files);
	hdfsDisconnect(fs);
	return assignment;
}

//considers locality
//1. compute avg size, define it as quota
//2. sort files by size
//3. for each file, if its slave has quota, assign it to the slave
//4. for the rest, run the greedy assignment
//(libhdfs do not have location info, but we can check slaveID from fileName)
//*** NOTE: NOT SUITABLE FOR DATA "PUT" TO HDFS, ONLY FOR DATA PROCESSED BY AT LEAST ONE JOB
vector<string>* dispatch_locality(const char* in_dir, int num_slaves) //remember to "delete[] assignment" after used
{
	//considers locality
	vector<string>* assignment = new vector<string>[num_slaves];
	hdfsFS fs = get_hdfs_fs();
	int num_files;
	hdfsFileInfo* fileinfo = hdfsListDirectory(fs, in_dir, &num_files);
	if (fileinfo == NULL)
	{
		fprintf(stderr, "Failed to list directory %s!\n", in_dir);
		exit(-1);
	}
	tOffset* assigned = new tOffset[num_slaves];
	for (int i = 0; i < num_slaves; i++)
		assigned[i] = 0;
	//sort files by size
	vector<sizedFName> sizedfile;
	int avg = 0;
	for (int i = 0; i < num_files; i++)
	{
		if (fileinfo[i].mKind == kObjectKindFile)
		{
			sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
			sizedfile.push_back(cur);
			avg += fileinfo[i].mSize;
		}
	}
	avg /= num_slaves;
	sort(sizedfile.begin(), sizedfile.end());
	//allocate files to slaves
	vector<sizedFName>::iterator it;
	vector<sizedFName> recycler;
	for (it = sizedfile.begin(); it != sizedfile.end(); ++it)
	{
		istringstream ss(rfind(it->fname, '/'));
		string cur;
		getline(ss, cur, '_');
		getline(ss, cur, '_');
		int slave_of_file = atoi(cur.c_str());
		if (assigned[slave_of_file] + it->size <= avg)
		{
			assignment[slave_of_file].push_back(it->fname);
			assigned[slave_of_file] += it->size;
		}
		else
			recycler.push_back(*it);
	}
	for (it = recycler.begin(); it != recycler.end(); ++it)
	{
		int min = 0;
		tOffset minSize = assigned[0];
		for (int j = 1; j < num_slaves; j++)
		{
			if (minSize > assigned[j])
			{
				min = j;
				minSize = assigned[j];
			}
		}
		assignment[min].push_back(it->fname);
		assigned[min] += it->size;
	}
	delete[] assigned;
	hdfsFreeFileInfo(fileinfo, num_files);
	hdfsDisconnect(fs);
	return assignment;
}

vector<vector<string> >* dispatch_run(const char* in_dir) //remember to delete assignment after used
{
	//locality is not considered for simplicity
	vector<vector<string> >* assignment_pointer = new vector<vector<string> >(_num_workers);
	vector<vector<string> >& assignment = *assignment_pointer;
	hdfsFS fs = get_hdfs_fs();
	int num_files;
	hdfsFileInfo* fileinfo = hdfsListDirectory(fs, in_dir, &num_files);
	if (fileinfo == NULL)
	{
		fprintf(stderr, "Failed to list directory %s!\n", in_dir);
		exit(-1);
	}
	tOffset* assigned = new tOffset[_num_workers];
	for (int i = 0; i < _num_workers; i++)
		assigned[i] = 0;
	//sort files by size
	vector<sizedFName> sizedfile;
	for (int i = 0; i < num_files; i++)
	{
		if (fileinfo[i].mKind == kObjectKindFile)
		{
			sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
			sizedfile.push_back(cur);
		}
	}
	sort(sizedfile.begin(), sizedfile.end());
	//allocate files to slaves
	vector<sizedFName>::iterator it;
	for (it = sizedfile.begin(); it != sizedfile.end(); ++it)
	{
		int min = 0;
		tOffset minSize = assigned[0];
		for (int j = 1; j < _num_workers; j++)
		{
			if (minSize > assigned[j])
			{
				min = j;
				minSize = assigned[j];
			}
		}
		assignment[min].push_back(it->fname);
		assigned[min] += it->size;
	}
	delete[] assigned;
	hdfsFreeFileInfo(fileinfo, num_files);
	hdfsDisconnect(fs);
	return assignment_pointer;
}

vector<vector<string> >* dispatch_run(vector<string>& in_dirs) //remember to delete assignment after used
{
	//locality is not considered for simplicity
	vector<vector<string> >* assignment_pointer = new vector<vector<string> >(_num_workers);
	vector<vector<string> >& assignment = *assignment_pointer;
	hdfsFS fs = get_hdfs_fs();
	vector<sizedFString> sizedfile;
	for (int pos = 0; pos < in_dirs.size(); pos++)
	{
		const char* in_dir = in_dirs[pos].c_str();
		int num_files;
		hdfsFileInfo* fileinfo = hdfsListDirectory(fs, in_dir, &num_files);
		if (fileinfo == NULL)
		{
			fprintf(stderr, "Failed to list directory %s!\n", in_dir);
			exit(-1);
		}
		for (int i = 0; i < num_files; i++)
		{
			if (fileinfo[i].mKind == kObjectKindFile)
			{
				sizedFString cur = { fileinfo[i].mName, fileinfo[i].mSize };
				sizedfile.push_back(cur);
			}
		}
		hdfsFreeFileInfo(fileinfo, num_files);
	}
	hdfsDisconnect(fs);
	//sort files by size
	sort(sizedfile.begin(), sizedfile.end());
	tOffset* assigned = new tOffset[_num_workers];
	for (int i = 0; i < _num_workers; i++)
		assigned[i] = 0;
	//allocate files to slaves
	vector<sizedFString>::iterator it;
	for (it = sizedfile.begin(); it != sizedfile.end(); ++it)
	{
		int min = 0;
		tOffset minSize = assigned[0];
		for (int j = 1; j < _num_workers; j++)
		{
			if (minSize > assigned[j])
			{
				min = j;
				minSize = assigned[j];
			}
		}
		assignment[min].push_back(it->fname);
		assigned[min] += it->size;
	}
	delete[] assigned;
	return assignment_pointer;
}

//considers locality
//1. compute avg size, define it as quota
//2. sort files by size
//3. for each file, if its slave has quota, assign it to the slave
//4. for the rest, run the greedy assignment
//(libhdfs do not have location info, but we can check slaveID from fileName)
//*** NOTE: NOT SUITABLE FOR DATA "PUT" TO HDFS, ONLY FOR DATA PROCESSED BY AT LEAST ONE JOB
vector<vector<string> >* dispatch_locality(const char* in_dir) //remember to delete assignment after used
{
	//considers locality
	vector<vector<string> >* assignment_pointer = new vector<vector<string> >(_num_workers);
	vector<vector<string> >& assignment = *assignment_pointer;
	hdfsFS fs = get_hdfs_fs();
	int num_files;
	hdfsFileInfo* fileinfo = hdfsListDirectory(fs, in_dir, &num_files);
	if (fileinfo == NULL)
	{
		fprintf(stderr, "Failed to list directory %s!\n", in_dir);
		exit(-1);
	}
	tOffset* assigned = new tOffset[_num_workers];
	for (int i = 0; i < _num_workers; i++)
		assigned[i] = 0;
	//sort files by size
	vector<sizedFName> sizedfile;
	int avg = 0;
	for (int i = 0; i < num_files; i++)
	{
		if (fileinfo[i].mKind == kObjectKindFile)
		{
			sizedFName cur = { fileinfo[i].mName, fileinfo[i].mSize };
			sizedfile.push_back(cur);
			avg += fileinfo[i].mSize;
		}
	}
	avg /= _num_workers;
	sort(sizedfile.begin(), sizedfile.end());
	//allocate files to slaves
	vector<sizedFName>::iterator it;
	vector<sizedFName> recycler;
	for (it = sizedfile.begin(); it != sizedfile.end(); ++it)
	{
		istringstream ss(rfind(it->fname, '/'));
		string cur;
		getline(ss, cur, '_');
		getline(ss, cur, '_');
		int slave_of_file = atoi(cur.c_str());
		if (assigned[slave_of_file] + it->size <= avg)
		{
			assignment[slave_of_file].push_back(it->fname);
			assigned[slave_of_file] += it->size;
		}
		else
			recycler.push_back(*it);
	}
	for (it = recycler.begin(); it != recycler.end(); ++it)
	{
		int min = 0;
		tOffset minSize = assigned[0];
		for (int j = 1; j < _num_workers; j++)
		{
			if (minSize > assigned[j])
			{
				min = j;
				minSize = assigned[j];
			}
		}
		assignment[min].push_back(it->fname);
		assigned[min] += it->size;
	}
	delete[] assigned;
	hdfsFreeFileInfo(fileinfo, num_files);
	hdfsDisconnect(fs);
	return assignment_pointer;
}

vector<vector<string> >* dispatch_locality(vector<string>& in_dirs) //remember to delete assignment after used
{
	//considers locality
	vector<vector<string> >* assignment_pointer = new vector<vector<string> >(_num_workers);
	vector<vector<string> >& assignment = *assignment_pointer;
	hdfsFS fs = get_hdfs_fs();
	vector<sizedFString> sizedfile;
	int avg = 0;
	for (int pos = 0; pos < in_dirs.size(); pos++)
	{
		const char* in_dir = in_dirs[pos].c_str();
		int num_files;
		hdfsFileInfo* fileinfo = hdfsListDirectory(fs, in_dir, &num_files);
		if (fileinfo == NULL)
		{
			fprintf(stderr, "Failed to list directory %s!\n", in_dir);
			exit(-1);
		}
		for (int i = 0; i < num_files; i++)
		{
			if (fileinfo[i].mKind == kObjectKindFile)
			{
				sizedFString cur = { fileinfo[i].mName, fileinfo[i].mSize };
				sizedfile.push_back(cur);
				avg += fileinfo[i].mSize;
			}
		}
		hdfsFreeFileInfo(fileinfo, num_files);
	}
	hdfsDisconnect(fs);
	tOffset* assigned = new tOffset[_num_workers];
	for (int i = 0; i < _num_workers; i++)
		assigned[i] = 0;
	//sort files by size
	avg /= _num_workers;
	sort(sizedfile.begin(), sizedfile.end());
	//allocate files to slaves
	vector<sizedFString>::iterator it;
	vector<sizedFString> recycler;
	for (it = sizedfile.begin(); it != sizedfile.end(); ++it)
	{
		istringstream ss(rfind(it->fname.c_str(), '/'));
		string cur;
		getline(ss, cur, '_');
		getline(ss, cur, '_');
		int slave_of_file = atoi(cur.c_str());
		if (assigned[slave_of_file] + it->size <= avg)
		{
			assignment[slave_of_file].push_back(it->fname);
			assigned[slave_of_file] += it->size;
		}
		else
			recycler.push_back(*it);
	}
	for (it = recycler.begin(); it != recycler.end(); ++it)
	{
		int min = 0;
		tOffset minSize = assigned[0];
		for (int j = 1; j < _num_workers; j++)
		{
			if (minSize > assigned[j])
			{
				min = j;
				minSize = assigned[j];
			}
		}
		assignment[min].push_back(it->fname);
		assigned[min] += it->size;
	}
	delete[] assigned;
	return assignment_pointer;
}

void report_assignment(vector<string>* assignment, int num_slaves)
{
	for (int i = 0; i < num_slaves; i++)
	{
		cout << "====== Rank " << i << " ======" << endl;
		vector<string>::iterator it;
		for (it = assignment[i].begin(); it != assignment[i].end(); ++it)
		{
			cout << *it << endl;
		}
	}
}

void report_assignment(vector<vector<string> >* assignment)
{
	for (int i = 0; i < _num_workers; i++)
	{
		cout << "====== Rank " << i << " ======" << endl;
		vector<string>::iterator it;
		for (it = (*assignment)[i].begin(); it != (*assignment)[i].end(); ++it)
		{
			cout << *it << endl;
		}
	}
}
