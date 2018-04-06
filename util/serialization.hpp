//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef SERIALIZATION_HPP_
#define SERIALIZATION_HPP_

#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>

#include "util/global.hpp"

using namespace std;


class ibinstream
{
public:
	char* get_buf();

	void raw_byte(char c);

	void raw_bytes(const void* ptr, int size);

	size_t size();

	void clear();

private:
	vector<char> buf_;
};

ibinstream& operator<<(ibinstream& m, size_t i);

ibinstream& operator<<(ibinstream& m, bool i);

ibinstream& operator<<(ibinstream& m, int i);

ibinstream& operator<<(ibinstream& m, double i);

ibinstream& operator<<(ibinstream& m, unsigned long long i);

ibinstream& operator<<(ibinstream& m, char c);

template <class T>
ibinstream& operator<<(ibinstream& m, const T* p);

template <class T>
ibinstream& operator<<(ibinstream& m, const vector<T>& v);

template <>
ibinstream& operator<<(ibinstream& m, const vector<int>& v);

template <>
ibinstream& operator<<(ibinstream& m, const vector<double>& v);

template <class T>
ibinstream& operator<<(ibinstream& m, const list<T>& v);

template <class T>
ibinstream& operator<<(ibinstream& m, const set<T>& v);

ibinstream& operator<<(ibinstream& m, const string& str);

template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const map<KeyT, ValT>& v);

template <class KeyT, class ValT>
ibinstream& operator<<(ibinstream& m, const hash_map<KeyT, ValT>& v);

template <class T>
ibinstream& operator<<(ibinstream& m, const hash_set<T>& v);

template <class T, class _HashFcn,  class _EqualKey >
ibinstream& operator<<(ibinstream& m, const hash_set<T, _HashFcn, _EqualKey>& v);


class obinstream
{
public:
	obinstream();
	obinstream(char* b, size_t s);
	obinstream(char* b, size_t s, size_t idx);
	~obinstream();

	char raw_byte();
	void* raw_bytes(unsigned int n_bytes);

	void assign(char* b, size_t s, size_t idx=0);

	void clear();

	bool end();

private:
	char* buf_; // responsible for deleting the buffer, do not delete outside
	size_t size_;
	size_t index_;
};

obinstream& operator>>(obinstream& m, size_t& i);

obinstream& operator>>(obinstream& m, bool& i);

obinstream& operator>>(obinstream& m, int& i);

obinstream& operator>>(obinstream& m, double& i);

obinstream& operator>>(obinstream& m, unsigned long long& i);

obinstream& operator>>(obinstream& m, char& c);

template <class T>
obinstream& operator>>(obinstream& m, T*& p);

template <class T>
obinstream& operator>>(obinstream& m, vector<T>& v);

template <>
obinstream& operator>>(obinstream& m, vector<int>& v);

template <>
obinstream& operator>>(obinstream& m, vector<double>& v);

template <class T>
obinstream& operator>>(obinstream& m, list<T>& v);

template <class T>
obinstream& operator>>(obinstream& m, set<T>& v);

obinstream& operator>>(obinstream& m, string& str);

template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, map<KeyT, ValT>& v);

template <class KeyT, class ValT>
obinstream& operator>>(obinstream& m, hash_map<KeyT, ValT>& v);

template <class T>
obinstream& operator>>(obinstream& m, hash_set<T>& v);

template <class T, class _HashFcn,  class _EqualKey >
obinstream& operator>>(obinstream& m, hash_set<T, _HashFcn, _EqualKey>& v);


#include "serialization.tpp"

#endif /* SERIALIZATION_HPP_ */
