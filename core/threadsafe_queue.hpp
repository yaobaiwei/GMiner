//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef THREADSAFE_QUEUE_HPP__
#define THREADSAFE_QUEUE_HPP__

#include <atomic>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <thread>

using namespace std;


template <class T>
class ThreadsafeQueue
{
public:
	ThreadsafeQueue();

	void push_back(T& item);
	void push_back(vector<T>& items);

	bool pop_front(T& task);
	bool pop_front(vector<T>& tasks);

	int size();
	bool empty();
	void close();

private:
	deque<T> m_queue_;
	mutex m_mutex_;
	condition_variable m_condv_;
	atomic_bool stop_;
};


#include "threadsafe_queue.tpp"

#endif /* THREADSAFE_QUEUE_HPP__ */
