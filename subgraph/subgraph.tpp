//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class NodeT>
void Subgraph<NodeT>::add_node(NodeT& node)
{
	ItemIterT it=nodes_.insert(nodes_.end(), node);
	node_map_[node.id] = it;
}

template <class NodeT>
void Subgraph<NodeT>::add_edge(NodeT & a, NodeT & b)
{
	a.add_neighbor(b);
	b.add_neighbor(a);
}

template <class NodeT>
void Subgraph<NodeT>::del_node(VertexID vid)
{
	typename NodeMap::iterator nodeIter = node_map_.find(vid);
	if(nodeIter != node_map_.end())
	{
		nodes_.erase(nodeIter->second);
		node_map_.erase(nodeIter);
	}
}

template <class NodeT>
bool Subgraph<NodeT>::has_node(VertexID vid)
{
	return node_map_.find(vid) != node_map_.end();
}

template <class NodeT>
NodeT* Subgraph<NodeT>::get_node(VertexID id)
{
	typename NodeMap::iterator nodeIter = node_map_.find(id);
	if(nodeIter != node_map_.end())
		return &(*(nodeIter->second));
	return NULL;
}

template <class NodeT>
list<NodeT>& Subgraph<NodeT>::get_nodes()
{
	return nodes_;
}
