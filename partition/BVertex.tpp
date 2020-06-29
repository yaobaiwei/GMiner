//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


//Default Hash Function =====================
template <class KeyT>
inline int DefaultHash::operator()(KeyT key)
{
	if (key >= 0)
		return key % _num_workers;
	else
		return (-key) % _num_workers;
}
//==========================================


template <class KeyT, class ValueT, class MessageT, class HashT>
BVertex<KeyT, ValueT, MessageT, HashT>::BVertex() : active_(true) { }

template <class KeyT, class ValueT, class MessageT, class HashT>
inline ValueT& BVertex<KeyT, ValueT, MessageT, HashT>::value()
{
	return value_;
}

template <class KeyT, class ValueT, class MessageT, class HashT>
inline const ValueT& BVertex<KeyT, ValueT, MessageT, HashT>::value() const
{
	return value_;
}

template <class KeyT, class ValueT, class MessageT, class HashT>
inline bool BVertex<KeyT, ValueT, MessageT, HashT>::operator<(const VertexT& rhs) const
{
	return id < rhs.id;
}

template <class KeyT, class ValueT, class MessageT, class HashT>
inline bool BVertex<KeyT, ValueT, MessageT, HashT>::operator==(const VertexT& rhs) const
{
	return id == rhs.id;
}

template <class KeyT, class ValueT, class MessageT, class HashT>
inline bool BVertex<KeyT, ValueT, MessageT, HashT>::operator!=(const VertexT& rhs) const
{
	return id != rhs.id;
}

template <class KeyT, class ValueT, class MessageT, class HashT>
inline bool BVertex<KeyT, ValueT, MessageT, HashT>::is_active()
{
	return active_;
}

template <class KeyT, class ValueT, class MessageT, class HashT>
inline void BVertex<KeyT, ValueT, MessageT, HashT>::activate()
{
	active_ = true;
}

template <class KeyT, class ValueT, class MessageT, class HashT>
inline void BVertex<KeyT, ValueT, MessageT, HashT>::vote_to_halt()
{
	active_ = false;
}

template <class KeyT, class ValueT, class MessageT, class HashT>
void BVertex<KeyT, ValueT, MessageT, HashT>::send_message(const KeyT& id, const MessageT& msg)
{
	((MessageBufT*)get_message_buffer())->add_message(id, msg);
}
