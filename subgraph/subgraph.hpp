//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef SUBGRAPH_HPP_
#define SUBGRAPH_HPP_

#include <vector>

#include "util/global.hpp"
#include "util/ioser.hpp"


template <class NodeT>
class Subgraph
{
public:
	typedef typename NodeT::KeyT KeyT;
	typedef typename NodeT::AttrT AttrT;
	typedef typename NodeT::AdjNodeT AdjNodeT;
	typedef typename NodeT::AdjList AdjList;

	typedef typename list<NodeT>::iterator ItemIterT;
	typedef hash_map<VertexID, ItemIterT> NodeMap;

	void add_node(NodeT & node);
	void add_edge(NodeT & a, NodeT & b);

	void del_node(VertexID vid);
	void del_edge(NodeT & a, NodeT & b);

	bool has_node(VertexID vid);
	NodeT * get_node(VertexID id);
	list<NodeT>& get_nodes();

	friend ibinstream& operator<<(ibinstream& m, const Subgraph& v)
	{
		m << v.nodes_;
		return m;
	}

	friend obinstream& operator>>(obinstream& m, Subgraph& v)
	{
		list<NodeT> & nodes = v.nodes_;
		NodeMap & nodemap = v.node_map_;
		m >> nodes;
		for(ItemIterT it = nodes.begin(); it != nodes.end(); ++it)
			nodemap[it->id] = it;
		return m;
	}

	// only need to serialize those remote vertexes, local vertexes can get from local_table after '>>'  operation
	friend ifbinstream& operator<<(ifbinstream& m, const Subgraph& v)
	{
		m << v.nodes_;
		return m;
	}

	friend ofbinstream& operator>>(ofbinstream& m, Subgraph& v)
	{
		list<NodeT> & nodes = v.nodes_;
		NodeMap & nodemap = v.node_map_;
		m >> nodes;
		for(ItemIterT it = nodes.begin(); it != nodes.end(); ++it)
			nodemap[it->id] = it;
		return m;
	}

private:
	NodeMap node_map_; // key => vid ; value =>the index of node in vector<NodeT>
	list<NodeT> nodes_;  // store the nodes of this subgraph  &&  miao update
};


#include "subgraph.tpp"

#endif /* SUBGRAPH_HPP_ */
