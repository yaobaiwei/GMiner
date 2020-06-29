//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


//struct kvpair;
bool kvpair::operator==(const kvpair& o) const
{
	return vid == o.vid;
}
ibinstream& operator << (ibinstream& m, const kvpair& idm)
{
	m << idm.vid;
	m << idm.value;
	return m;
}
obinstream& operator >> (obinstream& m, kvpair& idm)
{
	m >> idm.vid;
	m >> idm.value;
	return m;
}


//struct IDTrip;
ibinstream& operator << (ibinstream& m, const IDTrip& idm)
{
	m << idm.id;
	m << idm.trip;
	return m;
}
obinstream& operator >> (obinstream& m, IDTrip& idm)
{
	m >> idm.id;
	m >> idm.trip;
	return m;
}


//struct triplet;
bool triplet::operator==(const triplet& o) const
{
	return vid == o.vid;
}
ibinstream& operator << (ibinstream& m, const triplet& idm)
{
	m << idm.vid;
	m << idm.bid;
	m << idm.wid;
	return m;
}
obinstream& operator >> (obinstream& m, triplet& idm)
{
	m >> idm.vid;
	m >> idm.bid;
	m >> idm.wid;
	return m;
}


//struct blockInfo;
blockInfo::blockInfo() { }
blockInfo::blockInfo(VertexID _color, int _size) : color(_color), size(_size) { }
bool blockInfo::operator<(const blockInfo& o) const
{
	return size > o.size; //large file goes first
}
ibinstream& operator << (ibinstream& m, const blockInfo& idm)
{
	m << idm.color;
	m << idm.size;
	m << idm.nb_blocks;
	return m;
}
obinstream& operator >> (obinstream& m, blockInfo& idm)
{
	m >> idm.color;
	m >> idm.size;
	m >> idm.nb_blocks;
	return m;
}


//struct nbInfo;
ibinstream& operator << (ibinstream& m, const nbInfo& v)
{
	m << v.vid;
	m << v.label;
	return m;
}
obinstream& operator >> (obinstream& m, nbInfo& v)
{
	m >> v.vid;
	m >> v.label;
	return m;
}
