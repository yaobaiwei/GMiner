//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef BPARTITIONER_HPP_
#define BPARTITIONER_HPP_

#include <iostream>
#include <vector>

#include "subgraph/attribute.hpp"

#include "partition/BVertex.hpp"
#include "partition/Driver.hpp"

using namespace std;


template<class BPartVertexT>
class BPartitioner : public Driver<BPartVertexT>
{
protected:

	using typename Driver<BPartVertexT>::VertexContainer;
	using typename Driver<BPartVertexT>::VertexIter;
	using Driver<BPartVertexT>::vertexes_;
	using Driver<BPartVertexT>::hash_;

	typedef hash_map<VertexID, VertexID> HashMap;
	typedef typename HashMap::iterator MapIter;
	typedef MessageBuffer<BPartVertexT> MessageBufT;
	typedef typename MessageBufT::MessageContainerT MessageContainerT;

	typedef hash_map<VertexID, blockInfo> BInfoMap;
	typedef BInfoMap::iterator BIter;

	int active_count_;
	HashMap blk_to_slv_;
	MessageBufT* message_buffer_;
	Combiner<VertexID>* combiner_;

	void unset_combiner();
	void voronoi_compute();
	void subG_hashmin();
	int slave_of(BPartVertexT* v);
	int num_digits(int number);
	void block_sync();
	void active_compute();

	using Driver<BPartVertexT>::load_graph;
	using Driver<BPartVertexT>::dump_partition;

	virtual void load_vertex(BPartVertexT* v);
	virtual void sync_graph();

	virtual void block_assign() = 0;

	virtual void set_voronoi_combiner() = 0; //only for the part of logic about Voronoi sampling
	virtual void set_hashmin_combiner() = 0; //only for the part of logic about Voronoi sampling
	virtual BPartVertexT* to_vertex(char* line) = 0;  //this is what user specifies!!!!!!
	virtual void to_line(BPartVertexT* v, BufferedWriter& writer) = 0;  //this is what user specifies!!!!!!
	virtual void nb_info_exchange() = 0;  //this is what user specifies!!!!!!

public:
	BPartitioner();

	virtual ~BPartitioner();

	// run the worker
	virtual void run(const WorkerParams& params);
};


#include "BPartitioner.tpp"

#endif /* BPARTITIONER_HPP_ */
