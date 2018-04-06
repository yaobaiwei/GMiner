//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#include "timer.hpp"

const int N_Timers = 10; //currently, 10 timers are available
double _timers[N_Timers]; // timers
double _acc_time[N_Timers]; // accumulated time

void init_timers()
{
	for (int i = 0; i < N_Timers; i++)
	{
		_acc_time[i] = 0;
	}
}

double get_current_time()
{
	timeval t;
	gettimeofday(&t, 0);
	return (double)t.tv_sec + (double)t.tv_usec / 1000000;
}

//currently, only 4 timers are used, others can be defined by users

void start_timer(int i)
{
	_timers[i] = get_current_time();
}

void reset_timer(int i)
{
	_timers[i] = get_current_time();
	_acc_time[i] = 0;
}

void stop_timer(int i)
{
	double t = get_current_time();
	_acc_time[i] += t - _timers[i];
}

double get_timer(int i)
{
	return _acc_time[i];
}
