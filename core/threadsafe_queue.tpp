//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class T>
ThreadsafeQueue<T>::ThreadsafeQueue(): stop_(false) {}

template <class T>
void ThreadsafeQueue<T>::push_back(T& item)
{
	m_mutex_.lock();
	m_queue_.push_back(item);
	m_mutex_.unlock();
	m_condv_.notify_all();
}

template <class T>
void ThreadsafeQueue<T>::push_back(vector<T>& items)
{
	m_mutex_.lock();
	for(int i = 0 ; i < items.size(); i++)
	{
		m_queue_.push_back(items[i]);
	}
	m_mutex_.unlock();
	m_condv_.notify_all();
}

template <class T>
bool ThreadsafeQueue<T>::pop_front(T& task)
{
	unique_lock<mutex> lck(m_mutex_);
	while(m_queue_.empty() && !stop_)
	{
		m_condv_.wait(lck);
	}
	if(stop_)
		return false;

	task = m_queue_.front();
	m_queue_.pop_front();
	return true;
}

template <class T>
bool ThreadsafeQueue<T>::pop_front(vector<T>& tasks)
{
	unique_lock<mutex> lck(m_mutex_);
	while(m_queue_.empty() && !stop_)
	{
		m_condv_.wait(lck);
	}
	if(stop_)
		return false;
	int pop_num;
	if(m_queue_.size() < PIPE_POP_NUM) pop_num = m_queue_.size();
	else pop_num = PIPE_POP_NUM;

	for(int i = 0 ; i < pop_num; i++)
	{
		T t = m_queue_.front();
		m_queue_.pop_front();
		tasks.push_back(t);
	}
	return true;
}

template <class T>
int ThreadsafeQueue<T>::size()
{
	lock_guard<mutex> lck(m_mutex_);
	int size = m_queue_.size();
	return size;
}

template <class T>
bool ThreadsafeQueue<T>::empty()
{
	lock_guard<mutex> lck(m_mutex_);
	return m_queue_.size() == 0;
}

template <class T>
void ThreadsafeQueue<T>::close()
{
	m_mutex_.lock();
	stop_ = true;
	m_mutex_.unlock();
	m_condv_.notify_all();
}
