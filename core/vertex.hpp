//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef VERTEX_HPP_
#define VERTEX_HPP_

#include <vector>

#include "util/serialization.hpp"
#include "util/ioser.hpp"

#include "subgraph/node.hpp"

using namespace std;


// user-defined types for vertexID, adjacency-list items
template <class TaskT>
struct AdjVertex
{
	typedef typename TaskT::KeyType KeyT;
	typedef typename TaskT::AttrType AttrT;

	KeyT id;
	int wid;

	AdjVertex();
	AdjVertex(KeyT _id, int _wid);

#ifdef USE_ATTRIBUTE
	AttrT attr;

	AdjVertex(KeyT _id, int _wid, AttrT& _attr);
#endif

};

template <class TaskT>
bool operator==(const AdjVertex<TaskT>& a, const AdjVertex<TaskT>& b);

template <class TaskT>
bool operator<(const AdjVertex<TaskT>& a, const AdjVertex<TaskT>& b);

template <class TaskT>
ibinstream& operator<<(ibinstream& m, const AdjVertex<TaskT>& v);

template <class TaskT>
obinstream& operator>>(obinstream& m, AdjVertex<TaskT>& v);

template <class TaskT>
ifbinstream& operator<<(ifbinstream& m,  const AdjVertex<TaskT>& v);

template <class TaskT>
ofbinstream& operator>>(ofbinstream& m,  AdjVertex<TaskT>& v);


// user-defined types for vertexID & adjacency-list items
template <class TaskT>
struct Vertex
{
	typedef typename TaskT::KeyType KeyT;
	typedef typename TaskT::AttrType AttrT;

	typedef Node<TaskT> NodeT;

	typedef AdjVertex<TaskT> AdjVtxT;
	typedef vector<AdjVtxT> AdjList;
	typedef typename vector<AdjVtxT>::iterator AdjIter;

	KeyT id;
	AdjList adjlist;
#ifdef USE_ATTRIBUTE
	AttrT attr;
#endif

	void set_node(NodeT & node);

	AdjList &  get_adjlist();
};

template <class TaskT>
bool operator==(const Vertex<TaskT>& a, const Vertex<TaskT>& b);

template <class TaskT>
bool operator<(const Vertex<TaskT>& a, const Vertex<TaskT>& b);

template <class TaskT>
ibinstream& operator<<(ibinstream& m, const Vertex<TaskT>& v);

template <class TaskT>
obinstream& operator>>(obinstream& m, Vertex<TaskT>& v);

template <class TaskT>
ifbinstream& operator<<(ifbinstream& m,  const Vertex<TaskT>& v);

template <class TaskT>
ofbinstream& operator>>(ofbinstream& m,  Vertex<TaskT>& v);


#include "vertex.tpp"

#endif /* VERTEX_HPP_ */
