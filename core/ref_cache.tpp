//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class KeyT, class VertexT>
RefCache<KeyT, VertexT>::RefCache() {}

template <class KeyT, class VertexT>
void RefCache<KeyT, VertexT>::init(int maxsize)
{
	capacity_ = maxsize;
}

template <class KeyT, class VertexT>
void RefCache<KeyT, VertexT>::resize(int newsize)
{
	m_mutex_.lock();
	if(newsize <= capacity_)
	{
		int overSize = capacity_ - newsize;
		int diff = (overSize > cache_list_.size()) ? cache_list_.size() : overSize;
		for (int i = 0; i < diff; i++)
		{
			if (cache_list_.back().ref != 0)
				break;
			delete cache_list_.back().value; // delete the last one
			mp_.erase(cache_list_.back().key);
			cache_list_.pop_back();
		}
	}
	capacity_ = newsize;
	m_mutex_.unlock();
}

template <class KeyT, class VertexT>
void RefCache<KeyT, VertexT>::clear()
{
	m_mutex_.lock();
	for(RefCache<KeyT, VertexT>::ItemIterT iter =  cache_list_.begin(); iter != cache_list_.end(); iter++)
	{
		delete iter->value;
	}
	cache_list_.clear();
	mp_.clear();
	m_mutex_.unlock();
}

template <class KeyT, class VertexT>
int RefCache<KeyT, VertexT>::size()
{
	lock_guard<mutex> lck(m_mutex_);
	return cache_list_.size();
}

template <class KeyT, class VertexT>
VertexT* RefCache<KeyT, VertexT>::get(KeyT key)
{
	lock_guard<mutex> lck(m_mutex_);
	RefCache<KeyT, VertexT>::VTableIter it = mp_.find(key);
	RefCache<KeyT, VertexT>::ItemIterT listIt = it->second;
	return listIt->value;
}

template <class KeyT, class VertexT>
bool RefCache<KeyT, VertexT>::try_to_get(KeyT key, int count)
{
	lock_guard<mutex> lck(m_mutex_);
	RefCache<KeyT, VertexT>::VTableIter it = mp_.find(key);
	if(it==mp_.end())
	{
		RefCache<KeyT, VertexT>::ItemT new_node;
		new_node.key = key;
		new_node.value = NULL;
		new_node.ref = count;
		cache_list_.push_front(new_node);  //add the new one to the head
		mp_[key] = cache_list_.begin();
		return false;
	}
	else
	{
		RefCache<KeyT, VertexT>::ItemIterT listIt = it->second;
		listIt->ref += count;
		if(listIt->ref == count)
		{
			cache_list_.push_front(*listIt);
			cache_list_.erase(listIt);
			it->second = cache_list_.begin();
		}
		return true;
	}
}

template <class KeyT, class VertexT>
void RefCache<KeyT, VertexT>::dec_item_ref(KeyT key)
{
	lock_guard<mutex> lck(m_mutex_);
	RefCache<KeyT, VertexT>::VTableIter it = mp_.find(key);
	RefCache<KeyT, VertexT>::ItemIterT listIt = it->second;
	listIt->ref -= 1;
	if(listIt->ref == 0)
	{
		cache_list_.push_back(*listIt);
		cache_list_.erase(listIt);
		it->second = --cache_list_.end();
	}
}

template <class KeyT, class VertexT>
void RefCache<KeyT, VertexT>::batch_clear(int num)
{
	lock_guard<mutex> lck(m_mutex_);
	int diff = (num > cache_list_.size()) ?  cache_list_.size() : num;
	for(int i = 0 ; i < diff; i++)
	{
		if(cache_list_.back().ref != 0)
			return;
		delete cache_list_.back().value; // delete the last one
		mp_.erase(cache_list_.back().key);
		cache_list_.pop_back();
	}
}

template <class KeyT, class VertexT>
void RefCache<KeyT, VertexT>::set(KeyT key, VertexT * value)
{
	lock_guard<mutex> lck(m_mutex_);
	RefCache<KeyT, VertexT>::VTableIter it = mp_.find(key);
	if(it==mp_.end())   //the element doesn't exist in current cache
	{
		cout<< "[ERROR] Impossible branch in Ref-Cache !!!" << endl;
		exit(-1);
	}
	RefCache<KeyT, VertexT>::ItemIterT listIt = it->second;
	if(listIt->value != NULL)
	{
		cout<< "[ERROR] Impossible branch in Ref-Cache !!!" << endl;
		exit(-1);
	}
	listIt->value = value;
}
