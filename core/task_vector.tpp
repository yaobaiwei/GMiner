//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class T>
TaskVector<T>::TaskVector()
{
	pointer_ = 0;
}

template <class T>
void TaskVector<T>::push_back(T& item)
{
	lock_guard<mutex> lck(m_mutex_);
	if(pointer_ % 2 == 0)
		vector1_.push_back(item);
	else
		vector2_.push_back(item);
}

template <class T>
void TaskVector<T>::content(vector<T>& copy)
{
	lock_guard<mutex> lck(m_mutex_);
	copy.clear();
	if(pointer_ % 2 == 0)
	{
		copy = vector1_;
		vector1_.clear();
	}
	else
	{
		copy = vector2_;
		vector2_.clear();
	}
	pointer_ = 1 - pointer_;
}

template <class T>
int TaskVector<T>::size()
{
	lock_guard<mutex> lck(m_mutex_);
	if(pointer_ % 2 == 0)
		return vector1_.size();
	else
		return vector2_.size();
}

template <class T>
bool TaskVector<T>::empty()
{
	lock_guard<mutex> lck(m_mutex_);
	return (vector1_.size() == 0) && (vector2_.size() == 0);
}
