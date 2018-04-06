//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu

#define USE_HDFS2_

#include "util/hdfs_core.hpp"

int main(int argc, char** argv)
{
	load_hdfs_config();
	const char* input = argv[1];
	const char* output = argv[2];
	put(input, output);
	return 0;
}
