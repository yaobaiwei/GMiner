//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef SLAVE_COMMUN_HPP_
#define SLAVE_COMMUN_HPP_

#include <mpi.h>

#include "util/communication.hpp"
#include "util/global.hpp"


int slave_all_sum(int my_copy)
{
	int tmp = my_copy;
	if( _my_rank != SLAVE_LEADER)
	{
		send_data(my_copy, SLAVE_LEADER, MSCOMMUN_CHANNEL);
	}
	else
	{
		for (int i = 0; i < _num_workers - 2; i++)
		{
			tmp += recv_data<int>(MPI_ANY_SOURCE, MSCOMMUN_CHANNEL);
		}
	}
	return tmp;
}


#endif /* SLAVE_COMMUN_HPP_ */
