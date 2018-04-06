//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


char* ibinstream::get_buf()
{
	return &buf_[0];
}

void ibinstream::raw_byte(char c)
{
	buf_.push_back(c);
}

void ibinstream::raw_bytes(const void* ptr, int size)
{
	buf_.insert(buf_.end(), (const char*)ptr, (const char*)ptr + size);
}

size_t ibinstream::size()
{
	return buf_.size();
}

void ibinstream::clear()
{
	buf_.clear();
}


ibinstream& operator<<(ibinstream& m, size_t i)
{
	m.raw_bytes(&i, sizeof(size_t));
	return m;
}

ibinstream& operator<<(ibinstream& m, bool i)
{
	m.raw_bytes(&i, sizeof(bool));
	return m;
}

ibinstream& operator<<(ibinstream& m, int i)
{
	m.raw_bytes(&i, sizeof(int));
	return m;
}

ibinstream& operator<<(ibinstream& m, double i)
{
	m.raw_bytes(&i, sizeof(double));
	return m;
}

ibinstream& operator<<(ibinstream& m, unsigned long long i)
{
	m.raw_bytes(&i, sizeof(unsigned long long));
	return m;
}

ibinstream& operator<<(ibinstream& m, char c)
{
	m.raw_byte(c);
	return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const T* p)
{
	return m << *p;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const vector<T>& v)
{
	m << v.size();
	for (typename vector<T>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}

template <>
ibinstream& operator<<(ibinstream& m, const vector<int>& v)
{
	m << v.size();
	m.raw_bytes(&v[0], v.size() * sizeof(int));
	return m;
}

template <>
ibinstream& operator<<(ibinstream& m, const vector<double>& v)
{
	m << v.size();
	m.raw_bytes(&v[0], v.size() * sizeof(double));
	return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const list<T>& v)
{
	m << v.size();
	for (typename list<T>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}

template <class T>
ibinstream& operator<<(ibinstream& m, const set<T>& v)
{
	m << v.size();
	for (typename set<T>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}

ibinstream& operator<<(ibinstream& m, const string& str)
{
	m << str.length();
	m.raw_bytes(str.c_str(), str.length());
	return m;
}

template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const map<KeyT, ValT>& v)
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
ibinstream& operator<<(ibinstream& m, const hash_map<KeyT, ValT>& v)
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
ibinstream& operator<<(ibinstream& m, const hash_set<T>& v)
{
	m << v.size();
	for (typename hash_set<T>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}

template <class T, class _HashFcn,  class _EqualKey >
ibinstream& operator<<(ibinstream& m, const hash_set<T, _HashFcn, _EqualKey>& v)
{
	m << v.size();
	for (typename hash_set<T, _HashFcn, _EqualKey>::const_iterator it = v.begin(); it != v.end(); ++it)
	{
		m << *it;
	}
	return m;
}


obinstream::obinstream() : buf_(NULL), size_(0), index_(0) {};
obinstream::obinstream(char* b, size_t s) : buf_(b), size_(s), index_(0) {};
obinstream::obinstream(char* b, size_t s, size_t idx) : buf_(b), size_(s), index_(idx) {};
obinstream::~obinstream()
{
	delete[] buf_;
}

char obinstream::raw_byte()
{
	return buf_[index_++];
}

void* obinstream::raw_bytes(unsigned int n_bytes)
{
	char* ret = buf_ + index_;
	index_ += n_bytes;
	return ret;
}

void obinstream::assign(char* b, size_t s, size_t idx)
{
	buf_=b;
	size_=s;
	index_=idx;
}

void obinstream::clear()
{
	delete[] buf_;
	buf_=NULL;
	size_=index_=0;
}

bool obinstream::end()
{
	return index_ >= size_;
}

obinstream& operator>>(obinstream& m, size_t& i)
{
	i = *(size_t*)m.raw_bytes(sizeof(size_t));
	return m;
}

obinstream& operator>>(obinstream& m, bool& i)
{
	i = *(bool*)m.raw_bytes(sizeof(bool));
	return m;
}

obinstream& operator>>(obinstream& m, int& i)
{
	i = *(int*)m.raw_bytes(sizeof(int));
	return m;
}

obinstream& operator>>(obinstream& m, double& i)
{
	i = *(double*)m.raw_bytes(sizeof(double));
	return m;
}

obinstream& operator>>(obinstream& m, unsigned long long& i)
{
	i = *(unsigned long long*)m.raw_bytes(sizeof(unsigned long long));
	return m;
}

obinstream& operator>>(obinstream& m, char& c)
{
	c = m.raw_byte();
	return m;
}

template <class T>
obinstream& operator>>(obinstream& m, T*& p)
{
	p = new T;
	return m >> (*p);
}

template <class T>
obinstream& operator>>(obinstream& m, vector<T>& v)
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
obinstream& operator>>(obinstream& m, vector<int>& v)
{
	size_t size;
	m >> size;
	v.resize(size);
	int* data = (int*)m.raw_bytes(sizeof(int) * size);
	v.assign(data, data + size);
	return m;
}

template <>
obinstream& operator>>(obinstream& m, vector<double>& v)
{
	size_t size;
	m >> size;
	v.resize(size);
	double* data = (double*)m.raw_bytes(sizeof(double) * size);
	v.assign(data, data + size);
	return m;
}

template <class T>
obinstream& operator>>(obinstream& m, list<T>& v)
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

template <class T>
obinstream& operator>>(obinstream& m, set<T>& v)
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

obinstream& operator>>(obinstream& m, string& str)
{
	size_t length;
	m >> length;
	str.clear();
	char* data = (char*)m.raw_bytes(length);
	str.append(data, length);
	return m;
}

template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, map<KeyT, ValT>& v)
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
obinstream& operator>>(obinstream& m, hash_map<KeyT, ValT>& v)
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
obinstream& operator>>(obinstream& m, hash_set<T>& v)
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

template <class T, class _HashFcn,  class _EqualKey >
obinstream& operator>>(obinstream& m, hash_set<T, _HashFcn, _EqualKey>& v)
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
