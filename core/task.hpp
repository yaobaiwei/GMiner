//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef TASK_HPP_
#define TASK_HPP_

#include <vector>

#include "core/vertex.hpp"

#include "subgraph/node.hpp"
#include "subgraph/subgraph.hpp"

using namespace std;


template <class KeyT, class ContextT = char, class AttrT=char>
class Task
{
public:

	typedef KeyT KeyType;
	typedef ContextT ContextType;
	typedef AttrT AttrType;

	typedef Task<KeyT, ContextT, AttrT> TaskT;
	typedef Vertex<TaskT> VertexT;
	typedef Node<TaskT> NodeT;
	typedef Subgraph<NodeT> SubgraphT;

	typedef typename VertexT::AdjVtxT AdjVertex;
	typedef typename VertexT::AdjList AdjVtxList;
	typedef typename VertexT::AdjIter AdjVtxIter;

	typedef typename NodeT::AdjNodeT AdjNode;
	typedef typename NodeT::AdjList AdjNodeList;
	typedef typename NodeT::AdjIter AdjNodeIter;

	SubgraphT subG;
	vector<int> to_request;  // vertex to pull from remote workers
	AdjVtxList to_pull;  // vertices to be pulled for using in the next round
	ContextType context;

	Task();

	virtual ~Task() {}

	virtual bool compute(SubgraphT & g, ContextType & context, vector<VertexT *> & frontier) = 0;

	// to be used by users in UDF compute(.)
	void pull(const AdjVertex & v);
	void pull(AdjVtxList & adjlist);

	// user can implement their self-defined cost model strategy to judge the movability
	bool movable();

	// put remote-items in to_pull into to_request
	void set_to_request();

	bool is_request_empty();
};

template <class KeyT, class ContextT = char, class AttrT=char>
ibinstream& operator<<(ibinstream& m, const Task<KeyT, ContextT, AttrT>& v);

template <class KeyT, class ContextT = char, class AttrT=char>
obinstream& operator>>(obinstream& m, Task<KeyT, ContextT, AttrT>& v);

template <class KeyT, class ContextT = char, class AttrT=char>
ifbinstream& operator<<(ifbinstream& m, const Task<KeyT, ContextT, AttrT>& v);

template <class KeyT, class ContextT = char, class AttrT=char>
ofbinstream& operator>>(ofbinstream& m, Task<KeyT, ContextT, AttrT>& v);


#include "task.tpp"

#endif /* TASK_HPP_ */
