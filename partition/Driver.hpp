//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef PARTITION_DRIVER_HPP_
#define PARTITION_DRIVER_HPP_

#include <vector>

#include "util/hdfs_core.hpp"

using namespace std;


template <class BVertexT>
class Driver
{
protected:

	typedef vector<hash_map<VertexID, kvpair> > MapVec;
	typedef vector<vector<IDTrip>> ExchangeT;
	typedef typename BVertexT::HashType HashT;
	char buf_[1024];

	typedef vector<BVertexT*> VertexContainer;
	typedef typename VertexContainer::iterator VertexIter;
	VertexContainer vertexes_;
	HashT hash_;
	void load_graph(const char* inpath);
	void dump_partition(const char* outpath);

	//===============================================

	virtual void load_vertex(BVertexT* v) = 0;

	virtual void sync_graph() = 0;

	virtual BVertexT* to_vertex(char* line) = 0;

	virtual void to_line(BVertexT* v, BufferedWriter& writer) = 0;

	virtual void nb_info_exchange() = 0;

public:

	Driver();

	virtual ~Driver();

	// run the worker
	virtual void run(const WorkerParams& params) = 0;
};


#include "Driver.tpp"

#endif /* PARTITION_DRIVER_HPP_ */
