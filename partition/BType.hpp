//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef BTYPE_HPP_
#define BTYPE_HPP_

#include "util/type.hpp"


struct kvpair
{
	VertexID vid;
	int value;

	bool operator==(const kvpair& o) const;
};
ibinstream& operator << (ibinstream& m, const kvpair& idm);
obinstream& operator >> (obinstream& m, kvpair& idm);


struct IDTrip   //remember to free "messages" outside
{
	VertexID id;
	kvpair trip;
};
ibinstream& operator << (ibinstream& m, const IDTrip& idm);
obinstream& operator >> (obinstream& m, IDTrip& idm);


struct triplet
{
	VertexID vid;
	int bid;
	int wid;

	bool operator==(const triplet& o) const;
};
ibinstream& operator << (ibinstream& m, const triplet& idm);
obinstream& operator >> (obinstream& m, triplet& idm);


struct blockInfo
{
	VertexID color;
	int size;
	set<VertexID> nb_blocks; //the adjacent blocks

	blockInfo();
	blockInfo(VertexID _color, int _size);

	bool operator<(const blockInfo& o) const;
};
ibinstream& operator << (ibinstream& m, const blockInfo& idm);
obinstream& operator >> (obinstream& m, blockInfo& idm);


struct nbInfo
{
	VertexID vid;
	char label;
};
ibinstream& operator << (ibinstream& m, const nbInfo& v);
obinstream& operator >> (obinstream& m, nbInfo& v);


#include "BType.tpp"

#endif /* BTYPE_HPP_ */
