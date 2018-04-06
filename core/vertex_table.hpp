//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef VERTEX_TABLE_HPP_
#define VERTEX_TABLE_HPP_

#include <ext/hash_map>
#include <ext/hash_set>
#include <iostream>
#include <mutex>

using __gnu_cxx::hash_map;
using __gnu_cxx::hash_set;
using namespace std;


template <class KeyT, class ValueT> class VertexTable
{
public:
	void load();

	int size();

	ValueT* get(KeyT key);

	void set(KeyT key, ValueT* value);

	void clear();

	ValueT* next();

private:
	typedef typename hash_map<KeyT, ValueT*>::iterator VTIter;

	hash_map<KeyT, ValueT*> vtx_table_;
	VTIter vt_iter_;
	mutex t_mutex_;
};


#include "vertex_table.tpp"

#endif /* VERTEX_TABLE_HPP_ */
