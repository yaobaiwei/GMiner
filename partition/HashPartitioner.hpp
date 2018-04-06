//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef HASHPARTITIONER_HPP_
#define HASHPARTITIONER_HPP_

#include <iostream>
#include <vector>

#include "subgraph/attribute.hpp"

#include "partition/BVertex.hpp"
#include "partition/Driver.hpp"

using namespace std;


template<class BHashVertexT>
class HashPartitioner : public Driver<BHashVertexT>
{
protected:

	using typename Driver<BHashVertexT>::VertexContainer;
	using typename Driver<BHashVertexT>::VertexIter;
	using Driver<BHashVertexT>::vertexes_;
	using Driver<BHashVertexT>::hash_;

	using Driver<BHashVertexT>::load_graph;
	using Driver<BHashVertexT>::dump_partition;

	virtual void load_vertex(BHashVertexT* v);
	virtual void sync_graph();

	virtual BHashVertexT* to_vertex(char* line) = 0;
	virtual void to_line(BHashVertexT* v, BufferedWriter& writer) = 0;
	virtual void nb_info_exchange() = 0;

public:
	HashPartitioner();

	virtual ~HashPartitioner();

	// run the worker
	virtual void run(const WorkerParams& params);
};


//=====================================normal_HashPartitioner=====================================
struct normal_BHashValue
{
	int block;
	vector<VertexID> neighbors;
	vector<kvpair> nbs_info;
	//kvpair:
	//before master-sync: value = block.id (color)
	//after master-sync: value = wid

	normal_BHashValue();
};

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const normal_BHashValue& v);
obinstream& operator >> (obinstream& m, normal_BHashValue& v);

class normal_BHashVertex : public BVertex<VertexID, normal_BHashValue, VertexID>
{
public:
	virtual void compute(MessageContainer& messages); //not used
};

class normal_HashPartitioner : public HashPartitioner<normal_BHashVertex>
{
public:
	virtual normal_BHashVertex* to_vertex(char* line);
	virtual void to_line(normal_BHashVertex* v, BufferedWriter& writer); //key: "vertexID blockID slaveID"
	virtual void nb_info_exchange();
};
//=====================================normal_HashPartitioner=====================================


//=====================================label_HashPartitioner======================================
struct label_BHashValue
{
	int block;
	char label;
	vector<nbInfo> neighbors;
	vector<kvpair> nbs_info;
	//kvpair:
	//before master-sync: value = block.id (color)
	//after master-sync: value = wid

	label_BHashValue();
};

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const label_BHashValue& v);
obinstream& operator >> (obinstream& m, label_BHashValue& v);

class label_BHashVertex : public BVertex<VertexID, label_BHashValue, VertexID>
{
public:
	virtual void compute(MessageContainer& messages); //not used
};

class label_HashPartitioner : public HashPartitioner<label_BHashVertex>
{
public:
	virtual label_BHashVertex* to_vertex(char* line);
	virtual void to_line(label_BHashVertex* v, BufferedWriter& writer); //key: "vertexID blockID slaveID"
	virtual void nb_info_exchange();
};

//=====================================label_HashPartitioner======================================


//======================================attr_HashPartitioner======================================
struct attr_BHashValue
{
	int block;
	Attribute<AttrValueT> attr;
	vector<nbAttrInfo<AttrValueT> > neighbors;
	vector<kvpair> nbs_info;
	//kvpair:
	//before master-sync: value = block.id (color)
	//after master-sync: value = wid

	attr_BHashValue();
};

//no vertex-add, will not be called
ibinstream& operator<<(ibinstream& m, const attr_BHashValue& v);
obinstream& operator >> (obinstream& m, attr_BHashValue& v);

class attr_BHashVertex : public BVertex<VertexID, attr_BHashValue, VertexID>
{
public:
	virtual void compute(MessageContainer& messages); //not used
};

class attr_HashPartitioner : public HashPartitioner<attr_BHashVertex>
{
public:
	virtual attr_BHashVertex* to_vertex(char* line);
	virtual void to_line(attr_BHashVertex* v, BufferedWriter& writer); //key: "vertexID blockID slaveID"
	virtual void nb_info_exchange();
};

//======================================attr_HashPartitioner======================================

#include "HashPartitioner.tpp"

#endif /* HASHPARTITIONER_HPP_ */
