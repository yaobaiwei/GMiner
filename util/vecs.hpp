//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef VECS_HPP_
#define VECS_HPP_

#include <vector>

#include "util/combiner.hpp"
#include "util/global.hpp"
#include "util/serialization.hpp"


template <class KeyT, class MessageT>
struct MsgPair
{
	KeyT key;
	MessageT msg;

	MsgPair();
	MsgPair(KeyT v1, MessageT v2);

	bool operator<(const MsgPair& rhs) const;
};

template <class KeyT, class MessageT>
ibinstream& operator<<(ibinstream& m, const MsgPair<KeyT, MessageT>& v);

template <class KeyT, class MessageT>
obinstream& operator>>(obinstream& m, MsgPair<KeyT, MessageT>& v);

//===============================================

template <class KeyT, class MessageT, class HashT>
class Vecs
{
public:
	typedef vector<MsgPair<KeyT, MessageT> > Vec;
	typedef vector<Vec> VecGroup;

	Vecs();

	void append(const KeyT key, const MessageT msg);

	void clear();

	Vec& get_buf(int pos);
	VecGroup& get_bufs();

	// apply combiner logic
	void combine();

	long long get_total_msg();

private:
	int np_;
	VecGroup vecs_;
	HashT hash_;
};


#include "vecs.tpp"

#endif /* VECS_HPP_ */
