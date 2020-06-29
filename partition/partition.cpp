//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


bool USE_MULTI_ATTR = false;

#include "../util/global.hpp"
#include "partition.hpp"


void partitioner_exec(int argc, char** argv, const WorkerParams& param)
{
	const string partiton_mode(argv[3]);
	if(argc>=5 && strcmp(argv[4], "multi_attr")==0)
		USE_MULTI_ATTR=true;

	if(partiton_mode=="normal_bdg")
		normal_BDGPartitioner().run(param);
	else if((partiton_mode=="label_bdg"))
		label_BDGPartitioner().run(param);
	else if((partiton_mode=="attr_bdg"))
		attr_BDGPartitioner().run(param);

	else if((partiton_mode=="normal_hash"))
		normal_HashPartitioner().run(param);
	else if((partiton_mode=="label_hash"))
		label_HashPartitioner().run(param);
	else if((partiton_mode=="attr_hash"))
		attr_HashPartitioner().run(param);

	else
		cout<<"wrong partition mode! partition exits!"<<endl;
}


int main(int argc, char** argv)
{
	init_worker(&argc, &argv);
	load_hdfs_config();

	WorkerParams param;
	param.input_path = argv[1];
	param.output_path = argv[2];
	param.force_write = true;
	param.native_dispatcher = false;

	set_sample_rate(0.001);
	set_max_hop(10);
	set_max_vc_size(100000);
	set_factor(2.0);
	set_stop_ratio(0.9);
	set_max_rate(0.1);

	partitioner_exec(argc, argv, param);

	worker_finalize();
	return 0;
}
