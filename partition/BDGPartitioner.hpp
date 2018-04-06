//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef BDGPARTITIONER_HPP_
#define BDGPARTITIONER_HPP_

#include "partition/BPartitioner.hpp"


template<class BDGPartVertexT>
class BDGPartitioner : public BPartitioner<BDGPartVertexT>
{
protected:

	using typename BPartitioner<BDGPartVertexT>::VertexContainer;
	using typename BPartitioner<BDGPartVertexT>::VertexIter;
	using BPartitioner<BDGPartVertexT>::vertexes_;
	using BPartitioner<BDGPartVertexT>::hash_;

	using typename BPartitioner<BDGPartVertexT>::HashMap;
	using typename BPartitioner<BDGPartVertexT>::MapIter;
	using typename BPartitioner<BDGPartVertexT>::MessageBufT;
	using typename BPartitioner<BDGPartVertexT>::MessageContainerT;

	using typename BPartitioner<BDGPartVertexT>::BInfoMap;
	using typename BPartitioner<BDGPartVertexT>::BIter;

	using BPartitioner<BDGPartVertexT>::active_count_;
	using BPartitioner<BDGPartVertexT>::blk_to_slv_;
	using BPartitioner<BDGPartVertexT>::message_buffer_;
	using BPartitioner<BDGPartVertexT>::combiner_;

	using BPartitioner<BDGPartVertexT>::unset_combiner;
	using BPartitioner<BDGPartVertexT>::voronoi_compute;
	using BPartitioner<BDGPartVertexT>::subG_hashmin;
	using BPartitioner<BDGPartVertexT>::slave_of;
	using BPartitioner<BDGPartVertexT>::num_digits;
	using BPartitioner<BDGPartVertexT>::block_sync;
	using BPartitioner<BDGPartVertexT>::active_compute;

	virtual void block_assign();
	void partition(vector<blockInfo> & blocksInfo, hash_map<VertexID, int> & blk2slv);  //will implement the partition strategy base on blockInfo

	virtual void set_voronoi_combiner() = 0; //only for the part of logic about Voronoi sampling
	virtual void set_hashmin_combiner() = 0; //only for the part of logic about Voronoi sampling
	virtual BDGPartVertexT* to_vertex(char* line) = 0;  //this is what user specifies!!!!!!
	virtual void to_line(BDGPartVertexT* v, BufferedWriter& writer) = 0;  //this is what user specifies!!!!!!
	virtual void nb_info_exchange() = 0;  //this is what user specifies!!!!!!

};


//=====================================normal_BDGPartitioner=====================================
struct normal_BDGPartValue
{
	VertexID color; //block ID
	vector<VertexID> neighbors;
	vector<kvpair> nbs_info;
	//kvpair:
	//before master-sync: value = block.id (color)
	//after master-sync: value = wid

	normal_BDGPartValue();
};

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const normal_BDGPartValue& v);
obinstream& operator >> (obinstream& m, normal_BDGPartValue& v);

class normal_BDGPartVorCombiner : public Combiner<VertexID>
{
public:
	virtual void combine(VertexID& old, const VertexID& new_msg); //ignore new_msg
};

class normal_BDGPartHashMinCombiner : public Combiner<VertexID>
{
public:
	virtual void combine(VertexID& old, const VertexID& new_msg);
};

class normal_BDGPartVertex : public BVertex<VertexID, normal_BDGPartValue, VertexID>
{
public:
	normal_BDGPartVertex();
	void broadcast(VertexID msg);
	void compute(MessageContainer& messages); //Voronoi Diagram partitioning algorithm
};

class normal_BDGPartitioner : public BDGPartitioner<normal_BDGPartVertex>
{
public:
	virtual void set_voronoi_combiner();
	virtual void set_hashmin_combiner();

	virtual normal_BDGPartVertex* to_vertex(char* line);
	virtual void to_line(normal_BDGPartVertex* v, BufferedWriter& writer); //key: "vertexID blockID slaveID"
	virtual void nb_info_exchange();
};

//=====================================normal_BDGPartitioner=====================================


//=====================================label_BDGPartitioner======================================
struct label_BDGPartValue
{
	VertexID color; //block ID
	char label;
	vector<nbInfo> neighbors;
	vector<kvpair> nbs_info;
	//kvpair:
	//before master-sync: value = block.id (color)
	//after master-sync: value = wid

	label_BDGPartValue();
};

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const label_BDGPartValue& v);
obinstream& operator >> (obinstream& m, label_BDGPartValue& v);

class label_BDGPartVorCombiner : public Combiner<VertexID>
{
public:
	virtual void combine(VertexID& old, const VertexID& new_msg); //ignore new_msg
};

class label_BDGPartHashMinCombiner : public Combiner<VertexID>
{
public:
	virtual void combine(VertexID& old, const VertexID& new_msg);
};

class label_BDGPartVertex : public BVertex<VertexID, label_BDGPartValue, VertexID>
{
public:
	label_BDGPartVertex();
	void broadcast(VertexID msg);
	void compute(MessageContainer& messages); //Voronoi Diagram partitioning algorithm
};

class label_BDGPartitioner : public BDGPartitioner<label_BDGPartVertex>
{
public:
	virtual void set_voronoi_combiner();
	virtual void set_hashmin_combiner();

	virtual label_BDGPartVertex* to_vertex(char* line);
	virtual void to_line(label_BDGPartVertex* v, BufferedWriter& writer); //key: "vertexID blockID slaveID"
	virtual void nb_info_exchange();
};

//=====================================label_BDGPartitioner======================================


//======================================attr_BDGPartitioner======================================
struct attr_BDGPartValue
{
	VertexID color; //block ID
	Attribute<AttrValueT> attr;
	vector<nbAttrInfo<AttrValueT> > neighbors;
	vector<kvpair> nbs_info;
	//kvpair:
	//before master-sync: value = block.id (color)
	//after master-sync: value = wid

	attr_BDGPartValue();
};

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const attr_BDGPartValue& v);
obinstream& operator >> (obinstream& m, attr_BDGPartValue& v);

class attr_BDGPartVorCombiner : public Combiner<VertexID>
{
public:
	virtual void combine(VertexID& old, const VertexID& new_msg); //ignore new_msg
};

class attr_BDGPartHashMinCombiner : public Combiner<VertexID>
{
public:
	virtual void combine(VertexID& old, const VertexID& new_msg);
};

class attr_BDGPartVertex : public BVertex<VertexID, attr_BDGPartValue, VertexID>
{
public:
	attr_BDGPartVertex();
	void broadcast(VertexID msg);
	void compute(MessageContainer& messages); //Voronoi Diagram partitioning algorithm
};

class attr_BDGPartitioner : public BDGPartitioner<attr_BDGPartVertex>
{
public:
	virtual void set_voronoi_combiner();
	virtual void set_hashmin_combiner();

	virtual attr_BDGPartVertex* to_vertex(char* line);
	virtual void to_line(attr_BDGPartVertex* v, BufferedWriter& writer); //key: "vertexID blockID slaveID"
	virtual void nb_info_exchange();
};

//======================================attr_BDGPartitioner======================================

#include "BDGPartitioner.tpp"

#endif /* BDGPARTITIONER_HPP_ */
