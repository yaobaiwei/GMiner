//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class TaskT>
AdjNode<TaskT>::AdjNode() {}

template <class TaskT>
AdjNode<TaskT>::AdjNode(const typename AdjNode<TaskT>::KeyT& _id)
{
	id = _id;
}

#ifdef USE_ATTRIBUTE
template <class TaskT>
AdjNode<TaskT>::AdjNode(const typename AdjNode<TaskT>::KeyT& _id, const typename AdjNode<TaskT>::AttrT& _attr)
{
	id = _id;
	attr = _attr;
}
#endif

template <class TaskT>
bool operator==(const AdjNode<TaskT>& a, const AdjNode<TaskT>& b)
{
	return a.id == b.id;
}

template <class TaskT>
bool operator<(const AdjNode<TaskT>& a, const AdjNode<TaskT>& b)
{
	return a.id < b.id;
}

template <class TaskT>
ibinstream& operator<<(ibinstream& m, const AdjNode<TaskT>& v)
{
	m << v.id;
#ifdef USE_ATTRIBUTE
	m << v.attr;
#endif
	return m;
}

template <class TaskT>
obinstream& operator>>(obinstream& m, AdjNode<TaskT>& v)
{
	m >> v.id;
#ifdef USE_ATTRIBUTE
	m >> v.attr;
#endif
	return m;
}

template <class TaskT>
ifbinstream& operator<<(ifbinstream& m,  const AdjNode<TaskT>& v)
{
	m << v.id;
#ifdef USE_ATTRIBUTE
	m << v.attr;
#endif
	return m;
}

template <class TaskT>
ofbinstream& operator>>(ofbinstream& m,  AdjNode<TaskT>& v)
{
	m >> v.id;
#ifdef USE_ATTRIBUTE
	m >> v.attr;
#endif
	return m;
}


template <class TaskT>
Node<TaskT>::Node() {}

template <class TaskT>
Node<TaskT>::Node(const typename Node<TaskT>::KeyT& _id)
{
	id = _id;
}

#ifdef USE_ATTRIBUTE
template <class TaskT>
Node<TaskT>::Node(const typename Node<TaskT>::KeyT& _id, const typename Node<TaskT>::AttrT& _attr)
{
	id = _id;
	attr = _attr;
}
#endif

template <class TaskT>
void Node<TaskT>::add_neighbor(typename Node<TaskT>::AdjNodeT& node)
{
	adjlist.push_back(node);
}

template <class TaskT>
void Node<TaskT>::add_neighbor(Node<TaskT>& node)
{
	AdjNodeT item;
	item.id = node.id;
#ifdef USE_ATTRIBUTE
	item.attr = node.attr;
#endif
	adjlist.push_back(item);
}

template <class TaskT>
typename Node<TaskT>::AdjList& Node<TaskT>::get_adjlist()
{
	return adjlist;
}

template <class TaskT>
bool operator==(const Node<TaskT>& a, const Node<TaskT>& b)
{
	return a.id == b.id;
}

template <class TaskT>
bool operator<(const Node<TaskT>& a, const Node<TaskT>& b)
{
	return a.id < b.id;
}

template <class TaskT>
ibinstream& operator<<(ibinstream& m, const Node<TaskT>& v)
{
	m << v.id;
	m << v.adjlist;
#ifdef USE_ATTRIBUTE
	m << v.attr;
#endif
	return m;
}

template <class TaskT>
obinstream& operator>>(obinstream& m, Node<TaskT>& v)
{
	m >> v.id;
	m >> v.adjlist;
#ifdef USE_ATTRIBUTE
	m >> v.attr;
#endif
	return m;
}

template <class TaskT>
ifbinstream& operator<<(ifbinstream& m,  const Node<TaskT>& v)
{
	m << v.id;
	m << v.adjlist;
#ifdef USE_ATTRIBUTE
	m <<  v.attr;
#endif
	return m;
}

template <class TaskT>
ofbinstream& operator>>(ofbinstream& m,  Node<TaskT>& v)
{
	m >> v.id;
	m >> v.adjlist;
#ifdef USE_ATTRIBUTE
	m >>  v.attr;
#endif
	return m;
}
