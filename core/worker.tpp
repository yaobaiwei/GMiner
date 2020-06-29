//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class Master, class Slave, class AggregatorT>
Worker<Master, Slave, AggregatorT>::Worker()
{
	global_aggregator = NULL;
	global_agg = NULL;
}

template <class Master, class Slave, class AggregatorT>
void Worker<Master, Slave, AggregatorT>::set_aggregator(AggregatorT* ag, FinalT init_value)
{
	global_aggregator = ag;
	global_agg = new FinalT;
	*((FinalT*)global_agg) = init_value;
}

template <class Master, class Slave, class AggregatorT>
void Worker<Master, Slave, AggregatorT>::run(const WorkerParams& params)
{
	if (_my_rank == MASTER_RANK)
	{
		MasterT master;
		master.run(params);
	}
	else
	{
		SlaveT slave;
		slave.run(params);
	}
}
