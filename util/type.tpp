//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class KeyT, class TaskT>
ktpair<KeyT, TaskT>::ktpair() {}

template <class KeyT, class TaskT>
ktpair<KeyT, TaskT>::ktpair(KeyT _key, TaskT* _task)
{
	key = _key;
	task = _task;
}

template <class KeyT, class TaskT>
inline bool ktpair<KeyT, TaskT>::operator<(const ktpair<KeyT, TaskT> & tmp) const
{
	return key < tmp.key;
}

template <class KeyT, class TaskT>
inline bool ktpair<KeyT, TaskT>::operator<=(const ktpair<KeyT, TaskT> & tmp) const
{
	return key <= tmp.key;
}
