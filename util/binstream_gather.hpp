//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef BINSTREAM_GATHER_HPP_
#define BINSTREAM_GATHER_HPP_

#include <mpi.h>

#include "util/global.hpp"
#include "util/serialization.hpp"
#include "util/timer.hpp"


class BinStreamGather
{
	typedef long long CompType;

public:
	static void master_gather(obinstream& obm, bool compress = false);
	static void slave_gather(ibinstream& ibm, bool compress = false);

private:
	static void master_gather_impl(obinstream& obm);
	static void master_gather_impl_compress(obinstream& obm);

	static void slave_gather_impl(ibinstream& ibm);
	static void slave_gather_impl_compress(ibinstream& ibm);
};

#include "binstream_gather.tpp"

#endif /* BINSTREAM_GATHER_HPP_ */
