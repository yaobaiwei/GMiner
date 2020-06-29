//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef BVERTEX_HPP_
#define BVERTEX_HPP_

#include <vector>

#include "util/communication.hpp"
#include "util/messagebuffer.hpp"

#include "partition/BType.hpp"

using namespace std;


//Default Hash Function =====================
struct DefaultHash
{
	template <class KeyT>
	int operator()(KeyT key);
};
//==========================================


template <class KeyT, class ValueT, class MessageT, class HashT = DefaultHash>
class BVertex
{
public:

	typedef KeyT KeyType;
	typedef ValueT ValueType;
	typedef MessageT MessageType;
	typedef HashT HashType;
	typedef vector<MessageType> MessageContainer;
	typedef typename MessageContainer::iterator MessageIter;
	typedef BVertex<KeyT, ValueT, MessageT, HashT> VertexT;
	typedef MessageBuffer<VertexT> MessageBufT;

	virtual void compute(MessageContainer& messages) = 0;

	BVertex();

	ValueT& value();
	const ValueT& value() const;

	bool operator<(const VertexT& rhs) const;
	bool operator==(const VertexT& rhs) const;
	bool operator!=(const VertexT& rhs) const;

	bool is_active();
	void activate();
	void vote_to_halt();

	void send_message(const KeyT& id, const MessageT& msg);

	friend ibinstream& operator<<(ibinstream& m, const VertexT& v)
	{
		m << v.id;
		m << v.value_;
		return m;
	}
	friend obinstream& operator >> (obinstream& m, VertexT& v)
	{
		m >> v.id;
		m >> v.value_;
		return m;
	}

	KeyT id;

private:
	ValueT value_;
	bool active_;
};


#include "BVertex.tpp"

#endif /* BVERTEX_HPP_ */
