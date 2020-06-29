//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


void BinStreamGather::master_gather(obinstream& obm, bool compress = false)
{
	//gather
	if (compress == false)
		master_gather_impl(obm);
	else
		master_gather_impl_compress(obm);
}

void BinStreamGather::slave_gather(ibinstream& ibm, bool compress = false)
{
	//gather
	if (compress == false)
		slave_gather_impl(ibm);
	else
		slave_gather_impl_compress(ibm);
}


void BinStreamGather::master_gather_impl(obinstream& obm)
{
	//gather
	StartTimer(COMMUNICATION_TIMER);
	int sendcount = 0;
	int* recvcounts = new int[_num_workers];
	int* recvoffset = new int[_num_workers];

	StartTimer(TRANSFER_TIMER);
	MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);

	for (int i = 0; i < _num_workers; i++)
	{
		recvoffset[i] = (i == 0 ? 0 : recvoffset[i - 1] + recvcounts[i - 1]);
	}

	char* sendbuf;
	int recv_tot = recvoffset[_num_workers - 1] + recvcounts[_num_workers - 1];
	char* recvbuf = new char[recv_tot]; //obinstream will delete it

	StartTimer(TRANSFER_TIMER);
	MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);

	obm.assign(recvbuf, recv_tot);
	delete[] recvcounts;
	delete[] recvoffset;
	StopTimer(COMMUNICATION_TIMER);
}

void BinStreamGather::master_gather_impl_compress(obinstream& obm)
{
	//gather
	StartTimer(COMMUNICATION_TIMER);
	int compcount = 0;
	int tailcount = 0;
	int* compcounts = new int[_num_workers];
	int* tailcounts = new int[_num_workers];
	int* compoffset = new int[_num_workers];
	int* tailoffset = new int[_num_workers];
	const int compressed_time = sizeof(CompType);

	//MPI_Barrier(MPI_COMM_WORLD);
	StartTimer(TRANSFER_TIMER);
	MPI_Gather(&compcount, 1, MPI_INT, compcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
	MPI_Gather(&tailcount, 1, MPI_INT, tailcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);

	size_t recv_tot = 0;
	for (int i = 0; i < _num_workers; i++)
	{
		compoffset[i] = (i == 0 ? 0 : compoffset[i - 1] + compcounts[i - 1]);
		tailoffset[i] = (i == 0 ? 0 : tailoffset[i - 1] + tailcounts[i - 1]);
		recv_tot += (compcounts[i] * compressed_time + tailcounts[i]);
	}
	size_t comp_tot = compoffset[_num_workers - 1] + compcounts[_num_workers - 1];
	size_t tail_tot = tailoffset[_num_workers - 1] + tailcounts[_num_workers - 1];

	CompType* recvbuf0 = new CompType[comp_tot];
	char* recvbuf1 = new char[tail_tot];

	CompType* compbuf = NULL;
	char* tailbuf = NULL;

	StartTimer(TRANSFER_TIMER);
	MPI_Gatherv(compbuf, compcount, MPI_LONG_LONG, recvbuf0, compcounts, compoffset, MPI_LONG_LONG, MASTER_RANK, MPI_COMM_WORLD);
	MPI_Gatherv(tailbuf, tailcount, MPI_CHAR, recvbuf1, tailcounts, tailoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

	char* recvbuf = new char[recv_tot]; //obinstream will delete it
	size_t recvindex = 0;
	for (int i = 0; i < _num_workers; i++)
	{
		memcpy(recvbuf + recvindex, (char*)(recvbuf0 + compoffset[i]), compcounts[i] * compressed_time);
		recvindex += compcounts[i] * compressed_time;
		memcpy(recvbuf + recvindex, (char*)(recvbuf1 + tailoffset[i]), tailcounts[i]);
		recvindex += tailcounts[i];
	}
	delete[] recvbuf0;
	delete[] recvbuf1;
	StopTimer(TRANSFER_TIMER);

	obm.assign(recvbuf, recv_tot);
	delete[] compcounts;
	delete[] tailcounts;
	delete[] compoffset;
	delete[] tailoffset;
	StopTimer(COMMUNICATION_TIMER);
}


void BinStreamGather::slave_gather_impl(ibinstream& ibm)
{
	//gather
	StartTimer(COMMUNICATION_TIMER);
	int sendcount;
	int* recvcounts;
	int* recvoffset;

	sendcount = ibm.size();

	StartTimer(TRANSFER_TIMER);
	MPI_Gather(&sendcount, 1, MPI_INT, recvcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);

	char* sendbuf = ibm.get_buf(); //ibinstream will delete it
	char* recvbuf;

	StartTimer(TRANSFER_TIMER);
	MPI_Gatherv(sendbuf, sendcount, MPI_CHAR, recvbuf, recvcounts, recvoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);
	StopTimer(COMMUNICATION_TIMER);
}

void BinStreamGather::slave_gather_impl_compress(ibinstream& ibm)
{
	//gather
	StartTimer(COMMUNICATION_TIMER);
	int compcount;
	int tailcount;
	int* compcounts = NULL;
	int* tailcounts = NULL;
	int* compoffset = NULL;
	int* tailoffset = NULL;
	const int compressed_time = sizeof(CompType);

	compcount = ibm.size() / compressed_time;
	tailcount = ibm.size() % compressed_time;

	StartTimer(TRANSFER_TIMER);
	MPI_Gather(&compcount, 1, MPI_INT, compcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
	MPI_Gather(&tailcount, 1, MPI_INT, tailcounts, 1, MPI_INT, MASTER_RANK, MPI_COMM_WORLD);
	StopTimer(TRANSFER_TIMER);

	CompType* compbuf = (CompType*)ibm.get_buf(); //ibinstream will delete it
	char* tailbuf;
	tailbuf = tailcount != 0 ? (ibm.get_buf() + compcount*compressed_time) : NULL;
	CompType* recvbuf0 = NULL;
	char* recvbuf1 = NULL;

	StartTimer(TRANSFER_TIMER);
	MPI_Gatherv(compbuf, compcount, MPI_LONG_LONG, recvbuf0, compcounts, compoffset, MPI_LONG_LONG, MASTER_RANK, MPI_COMM_WORLD);
	MPI_Gatherv(tailbuf, tailcount, MPI_CHAR, recvbuf1, tailcounts, tailoffset, MPI_CHAR, MASTER_RANK, MPI_COMM_WORLD);

	StopTimer(TRANSFER_TIMER);
	StopTimer(COMMUNICATION_TIMER);
}
