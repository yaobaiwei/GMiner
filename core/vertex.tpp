//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class TaskT>
AdjVertex<TaskT>::AdjVertex() {}

template <class TaskT>
AdjVertex<TaskT>::AdjVertex(KeyT _id, int _wid)
{
	id = _id;
	wid = _wid;
}

#ifdef USE_ATTRIBUTE
template <class TaskT>
AdjVertex<TaskT>::AdjVertex(KeyT _id, int _wid, AttrT& _attr)
{
	id = _id;
	wid = _wid;
	attr = _attr;
}
#endif

template <class TaskT>
bool operator==(const AdjVertex<TaskT>& a, const AdjVertex<TaskT>& b)
{
	return a.id == b.id;
}

template <class TaskT>
bool operator<(const AdjVertex<TaskT>& a, const AdjVertex<TaskT>& b)
{
	return a.id < b.id;
}

template <class TaskT>
ibinstream& operator<<(ibinstream& m, const AdjVertex<TaskT>& v)
{
	m << v.id;
	m << v.wid;
#ifdef USE_ATTRIBUTE
	m << v.attr;
#endif
	return m;
}

template <class TaskT>
obinstream& operator>>(obinstream& m, AdjVertex<TaskT>& v)
{
	m >> v.id;
	m >> v.wid;
#ifdef USE_ATTRIBUTE
	m >> v.attr;
#endif
	return m;
}

template <class TaskT>
ifbinstream& operator<<(ifbinstream& m,  const AdjVertex<TaskT>& v)
{
	m << v.id;
	m << v.wid;
#ifdef USE_ATTRIBUTE
	m << v.attr;
#endif
	return m;
}

template <class TaskT>
ofbinstream& operator>>(ofbinstream& m,  AdjVertex<TaskT>& v)
{
	m >> v.id;
	m >> v.wid;
#ifdef USE_ATTRIBUTE
	m >> v.attr;
#endif
	return m;
}


template <class TaskT>
void Vertex<TaskT>::set_node(NodeT& node)
{
	node.id = id;
#ifdef USE_ATTRIBUTE
	node.attr = attr;
#endif
}

template <class TaskT>
typename Vertex<TaskT>::AdjList& Vertex<TaskT>::get_adjlist()
{
	return adjlist;
}

template <class TaskT>
bool operator==(const Vertex<TaskT>& a, const Vertex<TaskT>& b)
{
	return a.id == b.id;
}

template <class TaskT>
bool operator<(const Vertex<TaskT>& a, const Vertex<TaskT>& b)
{
	return a.id < b.id;
}

template <class TaskT>
ibinstream& operator<<(ibinstream& m, const Vertex<TaskT>& v)
{
	m << v.id;
	m << v.adjlist;
#ifdef USE_ATTRIBUTE
	m << v.attr;
#endif
	return m;
}

template <class TaskT>
obinstream& operator>>(obinstream& m, Vertex<TaskT>& v)
{
	m >> v.id;
	m >> v.adjlist;
#ifdef USE_ATTRIBUTE
	m >> v.attr;
#endif
	return m;
}

template <class TaskT>
ifbinstream& operator<<(ifbinstream& m,  const Vertex<TaskT>& v)
{
	m << v.id;
	m << v.adjlist;
#ifdef USE_ATTRIBUTE
	m <<  v.attr;
#endif
	return m;
}

template <class TaskT>
ofbinstream& operator>>(ofbinstream& m,  Vertex<TaskT>& v)
{
	m >> v.id;
	m >> v.adjlist;
#ifdef USE_ATTRIBUTE
	m >>  v.attr;
#endif
	return m;
}
