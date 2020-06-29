//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef NODE_HPP_
#define NODE_HPP_

#include <vector>

#include "util/ioser.hpp"
#include "util/serialization.hpp"

using namespace std;


template <class TaskT> //user-defined types for vertexID, adjacency-list items
struct AdjNode
{
	typedef typename TaskT::KeyType KeyT;
	KeyT id;

	AdjNode();
	AdjNode(const KeyT& _id);

#ifdef USE_ATTRIBUTE
	typedef typename TaskT::AttrType AttrT;
	AttrT attr;

	AdjNode(const KeyT& _id, const AttrT& _attr);
#endif

};

template <class TaskT>
bool operator==(const AdjNode<TaskT>& a, const AdjNode<TaskT>& b);

template <class TaskT>
bool operator<(const AdjNode<TaskT>& a, const AdjNode<TaskT>& b);

template <class TaskT>
ibinstream& operator<<(ibinstream& m, const AdjNode<TaskT>& v);

template <class TaskT>
obinstream& operator>>(obinstream& m, AdjNode<TaskT>& v);

template <class TaskT>
ifbinstream& operator<<(ifbinstream& m,  const AdjNode<TaskT>& v);

template <class TaskT>
ofbinstream& operator>>(ofbinstream& m,  AdjNode<TaskT>& v);


template <class TaskT> //user-defined types for vertexID & adjacency-list items
struct Node
{
	typedef typename TaskT::KeyType KeyT;
	typedef typename TaskT::AttrType AttrT;

	typedef AdjNode<TaskT> AdjNodeT;
	typedef vector<AdjNodeT> AdjList;
	typedef typename vector<AdjNodeT>::iterator AdjIter;

	KeyT id;
	AdjList adjlist;

	Node();
	Node(const KeyT& _id);

#ifdef USE_ATTRIBUTE
	AttrT attr;

	Node(const KeyT& _id, const AttrT& _attr);
#endif

	void add_neighbor(AdjNodeT& node);
	void add_neighbor(Node& node);
	AdjList&  get_adjlist();

};

template <class TaskT>
bool operator==(const Node<TaskT>& a, const Node<TaskT>& b);

template <class TaskT>
bool operator<(const Node<TaskT>& a, const Node<TaskT>& b);

template <class TaskT>
ibinstream& operator<<(ibinstream& m, const Node<TaskT>& v);

template <class TaskT>
obinstream& operator>>(obinstream& m, Node<TaskT>& v);

template <class TaskT>
ifbinstream& operator<<(ifbinstream& m,  const Node<TaskT>& v);

template <class TaskT>
ofbinstream& operator>>(ofbinstream& m,  Node<TaskT>& v);


#include "node.tpp"

#endif /* NODE_HPP_ */
