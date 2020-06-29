//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef THREADPOOL_HPP_
#define THREADPOOL_HPP_

#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>


class ThreadPool
{
public:
	ThreadPool(size_t);
	~ThreadPool();

	template<class F, class... Args>
	auto enqueue(F&& f, Args&&... args) -> std::future<typename std::result_of<F(Args...)>::type>;

private:
	// need to keep track of threads so we can join them
	std::vector<std::thread> workers_;

	// the task queue
	std::queue< std::function<void()> > tasks_;

	// synchronization
	std::mutex queue_mutex_;
	std::condition_variable condition_;
	bool stop_;
};

#endif /* THREADPOOL_HPP_ */
