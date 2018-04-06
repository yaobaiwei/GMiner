//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef BGLOBAL_HPP_
#define BGLOBAL_HPP_

#include "util/global.hpp"

using namespace std;


//========== Voronoi cell partitioning parameters ===========
extern double global_sampling_rate;
void set_sample_rate(double rate);

extern int global_max_hop; //maximum allowed #supersteps for a round of Voronoi partitioning
void set_max_hop(int hop);

extern int global_max_vcsize; //maximum allowed size of a Voronoi cell
void set_max_vc_size(int sz);

extern double global_stop_ratio; //switch to HashMin when #{this_round}/#{last_round}=98.9409% > global_stop_ratio
void set_stop_ratio(double ratio);

extern double global_max_rate; //switch to HashMin when sampling_rate > global_max_rate
void set_max_rate(double ratio);

extern double global_factor; //switch to HashMin when #{this_round}/#{last_round}=98.9409% > global_stop_ratio
void set_factor(double factor);
//============================================================

extern void* global_bmessage_buffer;
void set_bmessage_buffer(void* mb);
void* get_bmessage_buffer();

extern int global_bnum;
int& get_bnum();

extern int global_active_bnum;
int& active_bnum();

extern void* global_bcombiner;
void set_bcombiner(void* cb);
void* get_bcombiner();


#endif /* BGLOBAL_HPP_ */
