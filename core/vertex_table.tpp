//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class KeyT, class ValueT>
void VertexTable<KeyT, ValueT>::load()
{
	vt_iter_ = vtx_table_.begin();
}

template <class KeyT, class ValueT>
int VertexTable<KeyT, ValueT>::size()
{
	return vtx_table_.size();
}

template <class KeyT, class ValueT>
ValueT* VertexTable<KeyT, ValueT>::get(KeyT key)
{
	return vtx_table_[key];
}

template <class KeyT, class ValueT>
void VertexTable<KeyT, ValueT>::set(KeyT key, ValueT* value)
{
	vtx_table_[key] = value;
}

template <class KeyT, class ValueT>
void VertexTable<KeyT, ValueT>::clear()
{
	for(VTIter p = vtx_table_.begin(); p != vtx_table_.end(); p++)
		delete p->second;
	vtx_table_.clear();
}

template <class KeyT, class ValueT>
ValueT* VertexTable<KeyT, ValueT>::next()
{
	lock_guard<mutex> lck(t_mutex_);
	if(vt_iter_ == vtx_table_.end())
		return NULL;
	ValueT* p = vt_iter_->second;
	vt_iter_++;
	return p;
}
