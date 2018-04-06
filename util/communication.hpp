//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu
//Acknowledgements: this code is implemented by referencing pregel-mpi (https://code.google.com/p/pregel-mpi/) by Chuntao Hong.


#ifndef COMMUNICATION_HPP_
#define COMMUNICATION_HPP_

#include <mpi.h>
#include <mutex>

#include "util/global.hpp"
#include "util/serialization.hpp"
#include "util/timer.hpp"

//============================================

int all_sum(int my_copy);

long long master_sum_LL(long long my_copy);

long long all_sum_LL(long long my_copy);

char all_bor(char my_copy);

bool all_lor(bool my_copy);

bool all_land(bool my_copy);

//============================================

void pregel_send(void* buf, int size, int dst, int tag = COMM_CHANNEL_200);

int pregel_recv(void* buf, int size, int src, int tag = COMM_CHANNEL_200); //return the actual source, since "src" can be MPI_ANY_SOURCE

//============================================

void send_ibinstream(ibinstream& m, int dst, int tag = COMM_CHANNEL_200);

obinstream recv_obinstream(int src, int tag = COMM_CHANNEL_200);

obinstream recv_obinstream(int src, int tag, int& true_src);

//============================================
//obj-level send/recv
template <class T>
void send_data(const T& data, int dst, int tag);

template <class T>
void send_data(const T& data, int dst);

template <class T>
T recv_data(int src, int tag);

template <class T>
T recv_data(int src);

//============================================
//all-to-all
template <class T>
void all_to_all(std::vector<T>& to_exchange);

template <class T>
void all_to_all(vector<vector<T*> > & to_exchange);

template <class T, class T1>
void all_to_all(vector<T>& to_send, vector<T1>& to_get);

template <class T, class T1>
void all_to_all_cat(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2);

template <class T, class T1, class T2>
void all_to_all_cat(std::vector<T>& to_exchange1, std::vector<T1>& to_exchange2, std::vector<T2>& to_exchange3);

//============================================
//scatter
template <class T>
void master_scatter(vector<T>& to_send);

template <class T>
void slave_scatter(T& to_get);

//================================================================
//gather
template <class T>
void master_gather(vector<T>& to_get);

template <class T>
void slave_gather(T& to_send);

//================================================================
//bcast
template <class T>
void master_bcast(T& to_send);

template <class T>
void slave_bcast(T& to_get);

void master_bcastCMD(MSG cmd);

MSG slave_bcastCMD();

//============================================

#include "communication.tpp"

#endif /* COMMUNICATION_HPP_ */
