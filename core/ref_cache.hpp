//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef REFCACHE_HPP_
#define REFCACHE_HPP_

#include <ext/hash_map>
#include <algorithm>
#include <iostream>
#include <list>

using __gnu_cxx::hash_map;
using namespace std;


template <class KeyT, class VertexT>
struct Item
{
	KeyT key;
	VertexT* value;
	int ref;
};

template <class KeyT, class VertexT>
class RefCache
{
public:

	RefCache();

	void init(int maxsize);

	void resize(int newsize);

	void clear();

	int size();

	VertexT * get(KeyT key);

	bool try_to_get(KeyT key, int count);

	void dec_item_ref(KeyT key);

	void batch_clear(int num);

	void set(KeyT key, VertexT * value);

private:
	typedef Item<KeyT, VertexT> ItemT;
	typedef typename list<ItemT>::iterator ItemIterT;
	typedef hash_map<KeyT, ItemIterT> VTable;  // vertex table
	typedef typename VTable::iterator VTableIter;

	mutex m_mutex_;
	int capacity_;
	list<ItemT> cache_list_;
	VTable mp_;
};


#include "ref_cache.tpp"

#endif /* REFCACHE_HPP_ */
