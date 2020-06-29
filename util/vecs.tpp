//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class KeyT, class MessageT>
MsgPair<KeyT, MessageT>::MsgPair()
{
}

template <class KeyT, class MessageT>
MsgPair<KeyT, MessageT>::MsgPair(KeyT v1, MessageT v2)
{
	key = v1;
	msg = v2;
}

template <class KeyT, class MessageT>
inline bool MsgPair<KeyT, MessageT>::operator < (const MsgPair<KeyT, MessageT>& rhs) const
{
	return key < rhs.key;
}

template <class KeyT, class MessageT>
ibinstream& operator<<(ibinstream& m, const MsgPair<KeyT, MessageT>& v)
{
	m << v.key;
	m << v.msg;
	return m;
}

template <class KeyT, class MessageT>
obinstream& operator>>(obinstream& m, MsgPair<KeyT, MessageT>& v)
{
	m >> v.key;
	m >> v.msg;
	return m;
}

//===============================================

template <class KeyT, class MessageT, class HashT>
Vecs<KeyT, MessageT, HashT>::Vecs()
{
	np_ = _num_workers;
	vecs_.resize(_num_workers);
}

template <class KeyT, class MessageT, class HashT>
void Vecs<KeyT, MessageT, HashT>::append(const KeyT key, const MessageT msg)
{
	MsgPair<KeyT, MessageT> item(key, msg);
	vecs_[hash_(key)].push_back(item);
}

template <class KeyT, class MessageT, class HashT>
typename Vecs<KeyT, MessageT, HashT>::Vec& Vecs<KeyT, MessageT, HashT>::get_buf(int pos)
{
	return vecs_[pos];
}

template <class KeyT, class MessageT, class HashT>
typename Vecs<KeyT, MessageT, HashT>::VecGroup& Vecs<KeyT, MessageT, HashT>::get_bufs()
{
	return vecs_;
}

template <class KeyT, class MessageT, class HashT>
void Vecs<KeyT, MessageT, HashT>::clear()
{
	for (int i = 0; i < np_; i++)
	{
		vecs_[i].clear();
	}
}

//============================
//apply combiner logic
template <class KeyT, class MessageT, class HashT>
void Vecs<KeyT, MessageT, HashT>::combine()
{
	Combiner<MessageT>* combiner = (Combiner<MessageT>*)get_combiner();
	for (int i = 0; i < np_; i++)
	{
		sort(vecs_[i].begin(), vecs_[i].end());
		Vec new_vec;
		int size = vecs_[i].size();
		if (size > 0)
		{
			new_vec.push_back(vecs_[i][0]);
			KeyT preKey = vecs_[i][0].key;
			for (int j = 1; j < size; j++)
			{
				MsgPair<KeyT, MessageT>& cur = vecs_[i][j];
				if (cur.key != preKey)
				{
					new_vec.push_back(cur);
					preKey = cur.key;
				}
				else
				{
					combiner->combine(new_vec.back().msg, cur.msg);
				}
			}
		}
		new_vec.swap(vecs_[i]);
	}
}

template <class KeyT, class MessageT, class HashT>
long long Vecs<KeyT, MessageT, HashT>::get_total_msg()
{
	long long sum = 0;
	for (int i = 0; i < vecs_.size(); i++)
	{
		sum += vecs_[i].size();
	}
	return sum;
}
