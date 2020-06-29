//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef TASK_VECTOR_HPP_
#define TASK_VECTOR_HPP_

#include <thread>
#include <mutex>
#include <condition_variable>
#include <vector>

using namespace std;

template <class T>
class TaskVector
{
public:
	TaskVector();

	void push_back(T& item);

	void content(vector<T>& copy);

	int size();

	bool empty();

private:
	vector<T> vector1_;
	vector<T> vector2_;
	mutex m_mutex_;
	int pointer_;
};


#include "task_vector.tpp"

#endif /* TASK_VECTOR_HPP_ */
