//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef ATTRIBUTE_HPP_
#define ATTRIBUTE_HPP_

#include <string>

#include "util/ioser.hpp"
#include "util/serialization.hpp"


template<typename AttrValueT>
class Attribute
{
public:

	void init_attr_vec(const vector<AttrValueT>& attr_vec)
	{
		copy(attr_vec.begin(), attr_vec.end(), inserter(attr_vec_, attr_vec_.begin()));
	}

	vector<AttrValueT>& get_attr_vec()
	{
		return attr_vec_;
	}

	friend ibinstream& operator<<(ibinstream& m, const Attribute& v)
	{
		return m << v.attr_vec_;
	}

	friend obinstream& operator>>(obinstream& m, Attribute& v)
	{
		return m >> v.attr_vec_;
	}

	friend ifbinstream& operator<<(ifbinstream& m, const Attribute& v)
	{
		return m << v.attr_vec_;
	}

	friend ofbinstream& operator>>(ofbinstream& m, Attribute& v)
	{
		return m >> v.attr_vec_;
	}

private:
	vector<AttrValueT> attr_vec_;
};

template<typename AttrValueT>
struct nbAttrInfo
{
	VertexID vid;
	Attribute<AttrValueT> attr;
};


template<typename AttrValueT>
ibinstream& operator<<(ibinstream& m, const nbAttrInfo<AttrValueT>& v)
{
	m << v.vid;
	m << v.attr;
	return m;
}

template<typename AttrValueT>
obinstream& operator>>(obinstream& m, nbAttrInfo<AttrValueT>& v)
{
	m >> v.vid;
	m >> v.attr;
	return m;
}


#endif /* ATTRIBUTE_HPP_ */
