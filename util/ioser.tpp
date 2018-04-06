//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


ifbinstream::ifbinstream()//empty
{
	file_ = NULL;
	buf_pos_ = 0;
	tot_pos_ = 0;
	mem_buf_ = new char[STREAM_MEMBUF_SIZE];
}

ifbinstream::ifbinstream(const char* path)
{
	file_ = fopen(path, "wb");
	buf_pos_ = 0;
	tot_pos_ = 0;
	mem_buf_ = new char[STREAM_MEMBUF_SIZE];
}

inline void ifbinstream::bufflush()
{
	fwrite(mem_buf_, 1, buf_pos_, file_);
}

ifbinstream::~ifbinstream()
{
	delete mem_buf_;
	if(file_ == NULL) return; //already closed
	if(buf_pos_ > 0) bufflush();
	fclose(file_);
}

inline size_t ifbinstream::size()
{
	return tot_pos_;
}

void ifbinstream::raw_byte(char c)
{
	if(buf_pos_ == STREAM_MEMBUF_SIZE)
	{
		bufflush();
		buf_pos_ = 0;
	}
	mem_buf_[buf_pos_] = c;
	buf_pos_++;
	tot_pos_++;
}

void ifbinstream::raw_bytes(const void* ptr, int size)
{
	tot_pos_ += size;
	int gap = STREAM_MEMBUF_SIZE - buf_pos_;
	char * cptr = (char *)ptr;
	if(gap < size)
	{
		memcpy(mem_buf_ + buf_pos_, cptr, gap);
		buf_pos_ = STREAM_MEMBUF_SIZE; //useful for correct exec of bufflush()
		bufflush();
		size -= gap;
		cptr += gap;
		while(size > STREAM_MEMBUF_SIZE)
		{
			memcpy(mem_buf_, cptr, STREAM_MEMBUF_SIZE);
			bufflush();
			size -= STREAM_MEMBUF_SIZE;
			cptr += STREAM_MEMBUF_SIZE;
		}
		memcpy(mem_buf_, cptr, size);
		buf_pos_ = size;
	}
	else
	{
		memcpy(mem_buf_ + buf_pos_, ptr, size);
		buf_pos_ += size;
	}
}

//below is for object reuse

void ifbinstream::close() //also for flushing
{
	if(file_ == NULL) return; //already closed
	if(buf_pos_ > 0) bufflush();
	fclose(file_);
	file_ = NULL; //set status to closed
}

void ifbinstream::open(const char* path) //it does not check whether you closed previous file
{
	file_ = fopen(path,"wb");
	buf_pos_ = 0;
	tot_pos_ = 0;
}

bool ifbinstream::is_open()
{
	return file_ != NULL;
}

//make sure mm only contains one object (mm should be cleared before serializing an object)
ifbinstream & operator<<(ifbinstream & m, ibinstream mm)
{
	m.raw_bytes(mm.get_buf(), mm.size());
	return m;
}

ifbinstream & operator<<(ifbinstream & m, size_t i)
{
	m.raw_bytes(&i, sizeof(size_t));
	return m;
}

ifbinstream & operator<<(ifbinstream & m, bool i)
{
	m.raw_bytes(&i, sizeof(bool));
	return m;
}

ifbinstream & operator<<(ifbinstream & m, int i)
{
	m.raw_bytes(&i, sizeof(int));
	return m;
}

ifbinstream & operator<<(ifbinstream & m, double i)
{
	m.raw_bytes(&i, sizeof(double));
	return m;
}

ifbinstream & operator<<(ifbinstream & m, unsigned long long  i)
{
	m.raw_bytes(&i, sizeof(unsigned long long));
	return m;
}

ifbinstream & operator<<(ifbinstream & m, char c)
{
	m.raw_byte(c);
	return m;
}

template <class T>
ifbinstream & operator<<(ifbinstream & m, const T* p)
{
	return m << *p;
}

template <class T>
ifbinstream & operator<<(ifbinstream & m, const vector<T>& v)
{
	m << v.size();
	for (typename vector<T>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}

template <>
ifbinstream & operator<<(ifbinstream & m, const vector<int> & v)
{
	m << v.size();
	m.raw_bytes(&v[0], v.size() * sizeof(int));
	return m;
}

template <>
ifbinstream & operator<<(ifbinstream & m, const vector<double> & v)
{
	m << v.size();
	m.raw_bytes(&v[0], v.size() * sizeof(double));
	return m;
}

//miao update begin
template <class T>
ifbinstream & operator<<(ifbinstream & m, const list<T> & v)
{
	m << v.size();
	for(typename list<T>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}
//miao update end

template <class T>
ifbinstream & operator<<(ifbinstream & m, const set<T> & v)
{
	m << v.size();
	for(typename set<T>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}

ifbinstream & operator<<(ifbinstream & m, const string & str)
{
	m << str.length();
	m.raw_bytes(str.c_str(), str.length());
	return m;
}

template <class KeyT, class ValT>
ifbinstream & operator<<(ifbinstream & m, const map<KeyT, ValT> & v)
{
	m << v.size();
	for (typename map<KeyT, ValT>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << it->first;
		m << it->second;
	}
	return m;
}

template <class KeyT, class ValT>
ifbinstream & operator<<(ifbinstream & m, const hash_map<KeyT, ValT> & v)
{
	m << v.size();
	for (typename hash_map<KeyT, ValT>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << it->first;
		m << it->second;
	}
	return m;
}

template <class T>
ifbinstream & operator<<(ifbinstream & m, const hash_set<T> & v)
{
	m << v.size();
	for (typename hash_set<T>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}

//-------------------------------------

ofbinstream::ofbinstream()
{
	mem_buf_ = new char[STREAM_MEMBUF_SIZE];
	file_ = NULL; //set status to closed
}

ofbinstream::ofbinstream(const char* path)
{
	mem_buf_ = new char[STREAM_MEMBUF_SIZE];
	file_ = fopen(path, "rb");
	//get file size
	file_size_ = -1;
	struct stat statbuff;
	if(stat(path, &statbuff) == 0) file_size_ = statbuff.st_size;
	//get first batch
	fill();
	tot_pos_ = 0;
}

ofbinstream::~ofbinstream()
{
	delete mem_buf_;
	if(file_ == NULL) return; //already closed
	fclose(file_);
}

bool ofbinstream::open(const char* path) //return whether the file exists
{
	file_ = fopen(path, "rb");
	if(file_ == NULL) return false;
	//get file size
	file_size_ = -1;
	struct stat statbuff;
	if(stat(path, &statbuff) == 0) file_size_ = statbuff.st_size;
	//get first batch
	fill();
	tot_pos_ = 0;
	return true;
}

void ofbinstream::close()
{
	if(file_ == NULL) return; //already closed
	fclose(file_);
	file_ = NULL; //set status to closed
}

inline size_t ofbinstream::size()
{
	return file_size_;
}

inline bool ofbinstream::eof()
{
	return tot_pos_ >= file_size_;
}

inline void ofbinstream::fill()
{
	buf_size_ = fread(mem_buf_, 1, STREAM_MEMBUF_SIZE, file_);
	buf_pos_ = 0;
}

char ofbinstream::raw_byte()
{
	tot_pos_++;
	if(buf_pos_ == buf_size_) fill();
	return mem_buf_[buf_pos_++];
}

void* ofbinstream::raw_bytes(unsigned int n_bytes)
{
	tot_pos_ += n_bytes;
	int gap = buf_size_ - buf_pos_;
	if(gap >= n_bytes)
	{
		char* ret = mem_buf_ + buf_pos_;
		buf_pos_ += n_bytes;
		return ret;
	}
	else
	{
		//copy the last gap-batch to head of membuf
		//!!! require that STREAM_MEMBUF_SIZE >= n_bytes !!!
		memcpy(mem_buf_, mem_buf_ + buf_pos_, gap);
		//gap-shifted refill
		buf_size_ = gap + fread(mem_buf_ + gap, 1, STREAM_MEMBUF_SIZE - gap, file_);
		buf_pos_ = n_bytes;
		return mem_buf_;
	}
}

//=============== add skip function ===============
void ofbinstream::skip(int num_bytes)
{
	tot_pos_ += num_bytes;
	if(tot_pos_ >= file_size_) return; //eof
	buf_pos_ += num_bytes; //done if bufpos < bufsize
	if(buf_pos_ >= buf_size_)
	{
		fseek(file_, buf_pos_ - buf_size_, SEEK_CUR);
		fill();
	}
}


ofbinstream & operator>>(ofbinstream & m, size_t & i)
{
	i = *(size_t*)m.raw_bytes(sizeof(size_t));
	return m;
}

ofbinstream & operator>>(ofbinstream & m, bool & i)
{
	i = *(bool*)m.raw_bytes(sizeof(bool));
	return m;
}

ofbinstream & operator>>(ofbinstream & m, int & i)
{
	i = *(int*)m.raw_bytes(sizeof(int));
	return m;
}

ofbinstream & operator>>(ofbinstream & m, double & i)
{
	i = *(double*)m.raw_bytes(sizeof(double));
	return m;
}

ofbinstream & operator>>(ofbinstream & m, unsigned long long & i)
{
	i = *(unsigned long long*)m.raw_bytes(sizeof(unsigned long long));
	return m;
}

ofbinstream & operator>>(ofbinstream & m, char & c)
{
	c = m.raw_byte();
	return m;
}

template <class T>
ofbinstream & operator>>(ofbinstream & m, T* & p)
{
	p = new T;
	return m >> (*p);
}

template <class T>
ofbinstream & operator>>(ofbinstream & m, vector<T> & v)
{
	size_t size;
	m >> size;
	v.resize(size);
	for (typename vector<T>::iterator it = v.begin(); it != v.end(); ++it)
	{
		m >> *it;
	}
	return m;
}

template <>
ofbinstream & operator>>(ofbinstream & m, vector<int> & v)
{
	size_t size;
	m >> size;
	vector<int>::iterator it = v.begin();
	int len = STREAM_MEMBUF_SIZE / sizeof(int);
	int bytes = len * sizeof(int);
	while(size > len)
	{
		int* data = (int*)m.raw_bytes(bytes);
		v.insert(it, data, data + len);
		it = v.end();
		size -= len;
	}
	int* data = (int*)m.raw_bytes(sizeof(int) * size);
	v.insert(it, data, data + size);
	return m;
}

template <>
ofbinstream & operator>>(ofbinstream & m, vector<double> & v)
{
	size_t size;
	m >> size;
	vector<double>::iterator it = v.begin();
	int len = STREAM_MEMBUF_SIZE / sizeof(double);
	int bytes = len * sizeof(double);
	while(size > len)
	{
		double* data = (double*)m.raw_bytes(bytes);
		v.insert(it, data, data + len);
		it = v.end();
		size -= len;
	}
	double* data = (double*)m.raw_bytes(sizeof(double) * size);
	v.insert(it, data, data + size);
	return m;
}

//miao update begin
template <class T>
ofbinstream & operator>>(ofbinstream & m, list<T> & v)
{
	size_t size;
	m >> size;
	for (size_t i = 0; i < size; i++)
	{
		T tmp;
		m >> tmp;
		v.push_back(tmp);
	}
	return m;
}
//miao update end

template <class T>
ofbinstream & operator>>(ofbinstream & m, set<T> & v)
{
	size_t size;
	m >> size;
	for (size_t i = 0; i < size; i++)
	{
		T tmp;
		m >> tmp;
		v.insert(v.end(), tmp);
	}
	return m;
}

ofbinstream & operator>>(ofbinstream & m, string & str)
{
	size_t length;
	m >> length;
	str.clear();

	while(length > STREAM_MEMBUF_SIZE)
	{
		char* data = (char*)m.raw_bytes(STREAM_MEMBUF_SIZE); //raw_bytes cannot accept input > STREAM_MEMBUF_SIZE
		str.append(data, STREAM_MEMBUF_SIZE);
		length -= STREAM_MEMBUF_SIZE;
	}
	char* data = (char*)m.raw_bytes(length);
	str.append(data, length);

	return m;
}

template <class KeyT, class ValT>
ofbinstream & operator>>(ofbinstream & m, map<KeyT, ValT> & v)
{
	size_t size;
	m >> size;
	for (int i = 0; i < size; i++)
	{
		KeyT key;
		m >> key;
		m >> v[key];
	}
	return m;
}

template <class KeyT, class ValT>
ofbinstream & operator>>(ofbinstream & m, hash_map<KeyT, ValT> & v)
{
	size_t size;
	m >> size;
	for (int i = 0; i < size; i++)
	{
		KeyT key;
		m >> key;
		m >> v[key];
	}
	return m;
}

template <class T>
ofbinstream & operator>>(ofbinstream & m, hash_set<T> & v)
{
	size_t size;
	m >> size;
	for (int i = 0; i < size; i++)
	{
		T key;
		m >> key;
		v.insert(key);
	}
	return m;
}
