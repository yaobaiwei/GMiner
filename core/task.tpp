//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class KeyT, class ContextT, class AttrT>
Task<KeyT, ContextT, AttrT>::Task() {}

// to be used by users in UDF compute(.)
template <class KeyT, class ContextT, class AttrT>
void Task<KeyT, ContextT, AttrT>::pull(const AdjVertex & v)
{
	to_pull.push_back(v);
}

template <class KeyT, class ContextT, class AttrT>
void Task<KeyT, ContextT, AttrT>::pull(AdjVtxList & adjlist)
{
	to_pull.insert(to_pull.end(), adjlist.begin(), adjlist.end());
}

// user can implement their self-defined cost model strategy to judge the movability
template <class KeyT, class ContextT, class AttrT>
bool Task<KeyT, ContextT, AttrT>::movable()
{
	// cost model
	int subG_size = subG.get_nodes().size();
	int frontier_size = to_pull.size();
	int remote_vtxs = to_request.size();
	double local_rate = (frontier_size - remote_vtxs ) *1.0 / frontier_size;
	// judge whether this task can be movable, which should satisfy the following conditions
	if(subG_size <= SUBG_SIZE_T && local_rate < LOCAL_RATE)
		return true;
	return false;
}


// put remote-items in to_pull into to_request
template <class KeyT, class ContextT, class AttrT>
void Task<KeyT, ContextT, AttrT>::set_to_request()
{
	for(int i = 0; i < to_pull.size(); i++)
	{
		if(to_pull[i].wid != _my_rank)
			to_request.push_back(i);
	}
}

template <class KeyT, class ContextT, class AttrT>
bool Task<KeyT, ContextT, AttrT>::is_request_empty()
{
	return to_request.empty();
}


template <class KeyT, class ContextT = char, class AttrT=char>
ibinstream& operator<<(ibinstream& m, const Task<KeyT, ContextT, AttrT>& v)
{
	m << v.subG;
	m << v.to_pull;
	m << v.context;
	return m;
}

template <class KeyT, class ContextT = char, class AttrT=char>
obinstream& operator>>(obinstream& m, Task<KeyT, ContextT, AttrT>& v)
{
	m >> v.subG;
	m >> v.to_pull;
	m >> v.context;
	v.set_to_request();
	return m;
}

template <class KeyT, class ContextT = char, class AttrT=char>
ifbinstream& operator<<(ifbinstream& m, const Task<KeyT, ContextT, AttrT>& v)
{
	m << v.subG;
	m << v.to_pull;
	m << v.context;
	return m;
}

template <class KeyT, class ContextT = char, class AttrT=char>
ofbinstream& operator>>(ofbinstream& m, Task<KeyT, ContextT, AttrT>& v)
{
	m >> v.subG;
	m >> v.to_pull;
	m >> v.context;
	v.set_to_request();
	return m;
}
