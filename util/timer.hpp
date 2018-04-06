//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef TIME_HPP_
#define TIME_HPP_

#include <sys/time.h>
#include <stdio.h>

#define StartTimer(i) start_timer((i))
#define StopTimer(i) stop_timer((i))
#define ResetTimer(i) reset_timer((i))

// change from MASTER_RANK to 0
#define PrintTimer(str, i) \
    if (get_worker_id() == 0) \
        printf("%s : %f seconds\n", (str), get_timer((i)));

#define MasterPrintTimer(str, i) \
    if (get_worker_id() == MASTER_RANK) \
        printf("%s : %f seconds\n", (str), get_timer((i)));

extern const int N_Timers; //currently, 10 timers are available

enum TIMERS
{
	MASTER_TIMER = 0,
	WORKER_TIMER = 1,
	SERIALIZATION_TIMER = 2,
	TRANSFER_TIMER = 3,
	COMMUNICATION_TIMER = 4
};

void init_timers();
double get_current_time();

//currently, only 4 timers are used, others can be defined by users

void start_timer(int i);
void reset_timer(int i);
void stop_timer(int i);
double get_timer(int i);


#endif /* TIME_HPP_ */
