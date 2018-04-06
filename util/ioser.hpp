//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef IOSER_HPP_
#define IOSER_HPP_

#include <sys/stat.h>
#include <list>
#include <map>
#include <set>
#include <stdio.h>
#include <string.h>
#include <string>
#include <vector>

#include "util/global.hpp"
#include "util/serialization.hpp"

using namespace std;

const int STREAM_MEMBUF_SIZE = 65536; // 64k

//-------------------------------------

class ifbinstream
{
public:
	ifbinstream();
	ifbinstream(const char* path);

	~ifbinstream();

	size_t size();

	void bufflush();
	void raw_byte(char c);
	void raw_bytes(const void* ptr, int size);

	//below is for object reuse
	void close(); //also for flushing
	void open(const char* path); //it does not check whether you closed previous file
	bool is_open();

private:
	char* mem_buf_;
	int buf_pos_;
	size_t tot_pos_;
	FILE* file_;
};

// make sure mm only contains one object (mm should be cleared before serializing an object)
ifbinstream & operator<<(ifbinstream & m, ibinstream mm);
ifbinstream & operator<<(ifbinstream & m, size_t i);
ifbinstream & operator<<(ifbinstream & m, bool i);
ifbinstream & operator<<(ifbinstream & m, int i);
ifbinstream & operator<<(ifbinstream & m, double i);
ifbinstream & operator<<(ifbinstream & m, unsigned long long  i);
ifbinstream & operator<<(ifbinstream & m, char c);

template <class T>
ifbinstream & operator<<(ifbinstream & m, const T* p);

template <class T>
ifbinstream & operator<<(ifbinstream & m, const vector<T>& v);

template <>
ifbinstream & operator<<(ifbinstream & m, const vector<int> & v);

template <>
ifbinstream & operator<<(ifbinstream & m, const vector<double> & v);

template <class T>
ifbinstream & operator<<(ifbinstream & m, const list<T> & v);

template <class T>
ifbinstream & operator<<(ifbinstream & m, const set<T> & v);

ifbinstream & operator<<(ifbinstream & m, const string & str);

template <class KeyT, class ValT>
ifbinstream & operator<<(ifbinstream & m, const map<KeyT, ValT> & v);

template <class KeyT, class ValT>
ifbinstream & operator<<(ifbinstream & m, const hash_map<KeyT, ValT> & v);

template <class T>
ifbinstream & operator<<(ifbinstream & m, const hash_set<T> & v);

//-------------------------------------

class ofbinstream
{
public:
	ofbinstream();
	ofbinstream(const char* path);

	~ofbinstream();

	bool open(const char* path); //return whether the file exists
	void close();

	size_t size();

	bool eof();
	void fill();

	char raw_byte();
	void* raw_bytes(unsigned int n_bytes);

	// add skip function
	void skip(int num_bytes);

private:
	char* mem_buf_;
	int buf_pos_;
	int buf_size_; // membuf may not be full (e.g. last batch)
	size_t tot_pos_;
	size_t file_size_;
	FILE* file_;
};

ofbinstream & operator>>(ofbinstream & m, size_t & i);
ofbinstream & operator>>(ofbinstream & m, bool & i);
ofbinstream & operator>>(ofbinstream & m, int & i);
ofbinstream & operator>>(ofbinstream & m, double & i);
ofbinstream & operator>>(ofbinstream & m, unsigned long long & i);
ofbinstream & operator>>(ofbinstream & m, char & c);

template <class T>
ofbinstream & operator>>(ofbinstream & m, T* & p);

template <class T>
ofbinstream & operator>>(ofbinstream & m, vector<T> & v);

template <>
ofbinstream & operator>>(ofbinstream & m, vector<int> & v);

template <>
ofbinstream & operator>>(ofbinstream & m, vector<double> & v);

template <class T>
ofbinstream & operator>>(ofbinstream & m, list<T> & v);

template <class T>
ofbinstream & operator>>(ofbinstream & m, set<T> & v);

ofbinstream & operator>>(ofbinstream & m, string & str);

template <class KeyT, class ValT>
ofbinstream & operator>>(ofbinstream & m, map<KeyT, ValT> & v);

template <class KeyT, class ValT>
ofbinstream & operator>>(ofbinstream & m, hash_map<KeyT, ValT> & v);

template <class T>
ofbinstream & operator>>(ofbinstream & m, hash_set<T> & v);

#include "ioser.tpp"

#endif /* IOSER_HPP_ */
