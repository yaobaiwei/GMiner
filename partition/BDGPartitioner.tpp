//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template<class BDGPartVertexT>
void BDGPartitioner<BDGPartVertexT>::block_assign()
{
	//collect Voronoi cell size and neighbors' bids
	BInfoMap b_info_map;   //key: color   value: blockInfo (color, size, neighbor bid)
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID color = (*it)->value().color;
		vector<kvpair>& nbs = (*it)->value().nbs_info;
		BIter b_iter = b_info_map.find(color);
		if (b_iter == b_info_map.end())
		{
			blockInfo b_info(color, 1);
			for (int i = 0; i < nbs.size(); i++)
			{
				b_info.nb_blocks.insert(nbs[i].value);
			}
			b_info_map[color] = b_info;
		}
		else
		{
			blockInfo & blk = b_iter->second;
			blk.size++;
			for (int i = 0; i < nbs.size(); i++)
			{
				blk.nb_blocks.insert(nbs[i].value);
			}
		}
	}

	//use shuffle to balance the workload of aggregation on blockInfo to each worker
	vector<BInfoMap>  maps(_num_workers);
	for (BIter it = b_info_map.begin(); it != b_info_map.end(); it++)
	{
		maps[hash_(it->first)][it->first] = it->second;
	}
	b_info_map.clear();

	//shuffle the b_info_map
	all_to_all(maps);

	BInfoMap& mypart = maps[_my_rank];

	for (int i = 0; i < _num_workers; i++)
	{
		if (i != _my_rank)
		{
			BInfoMap& part = maps[i];
			for (BIter it = part.begin(); it != part.end(); it++)
			{
				VertexID color = it->first;
				BIter bit = mypart.find(color);
				if (bit == mypart.end())
				{
					b_info_map[color] = it->second;
				}
				else
				{
					bit->second.size += it->second.size;
					set<VertexID> & nb_block = it->second.nb_blocks;
					set<VertexID>::iterator setIter;
					for (setIter = nb_block.begin(); setIter != nb_block.end(); setIter++)
					{
						bit->second.nb_blocks.insert(*setIter);
					}
				}
			}
			part.clear();
		}
	}

	//aggregation
	if (_my_rank != MASTER_RANK)
	{
		send_data(mypart, MASTER_RANK, 0);
		mypart.clear();
		slave_bcast(blk_to_slv_);
	}
	else
	{
		for (int i = 0; i < _num_workers; i++)
		{
			if (i != MASTER_RANK)
			{
				obinstream um = recv_obinstream(MPI_ANY_SOURCE, 0);
				BInfoMap part;
				um >> part;
				for (BIter it = part.begin(); it != part.end(); it++)
				{
					mypart[it->first] = it->second;
				}
				part.clear();
			}
		}

		//-----------------------------
		//%%% for print only %%%
		hash_map<int, int> histogram;
		for (BIter it = mypart.begin(); it != mypart.end(); it++)
		{
			//{ %%% for print only %%%
			int key = num_digits(it->second.size);
			hash_map<int, int>::iterator hit = histogram.find(key);
			if (hit == histogram.end())
				histogram[key] = 1;
			else
				hit->second++;
			//%%% for print only %%% }
		}

		//------------------- report begin -------------------
		cout << "* block size histogram:" << endl;
		for (hash_map<int, int>::iterator hit = histogram.begin(); hit != histogram.end(); hit++)
		{
			cout << "|V|<" << hit->first << ": " << hit->second << " blocks" << endl;
		}
		//------------------- report end ---------------------

		//convert map<color, blockInfo> to vector
		vector<blockInfo> blocks_info;
		for (BIter bit = mypart.begin(); bit != mypart.end(); bit++)
		{
			blocks_info.push_back(bit->second);
		}
		b_info_map.clear();

		partition(blocks_info, blk_to_slv_);   //user will set blk2slv

		//%%%%%% scattering blk2slv
		master_bcast(blk_to_slv_);
	}
}


template<class BDGPartVertexT>
void BDGPartitioner<BDGPartVertexT>::partition(vector<blockInfo> & blocks_info, hash_map<VertexID, int> & blk_to_slv) //Implement the partition strategy base on blockInfo
{
	//Streaming Graph Partitioning for Large Distributed Graphs KDD 2013
	//strategy 4
	//blk2slv : set block B --> worker W

	double eps = 0.1;  //the ratio to adjust the capacity of bins
	int total_count = 0;
	for (int i = 0; i < blocks_info.size(); i++)
	{
		total_count += blocks_info[i].size;
	}

	//set the capacity of bin to the (1+eps)* [avg of number of v in each worker]
	int capacity = (1 + eps) * (total_count / get_num_workers());

	// sort in non-increasing order of block size
	sort(blocks_info.begin(), blocks_info.end());

	int* assigned = new int[_num_workers];
	int* bcount = new int[_num_workers]; //%%% for print only %%%
	hash_map<VertexID, int> * countmap = new  hash_map<VertexID, int>[_num_workers]; //store the sum of the size of adjacent blocks in each worker
	for (int i = 0; i < _num_workers; i++)
	{
		assigned[i] = 0;
		bcount[i] = 0;
	}

	//allocate each block to workers
	//strategy:
	//block B is always allocated to worker Wi with the highest priority
	// weight : wi = 1 - assigned[Wi] / capacity
	// Ci : the size of one adjacent block of B in worker Wi
	// Si : sum { Ci }
	//priority = Si * wi

	hash_map<VertexID, int>::iterator cmIter;
	for (int i = 0; i < blocks_info.size(); i++)
	{
		blockInfo & cur = blocks_info[i];
		double max = 0;
		int wid = 0;
		bool is_allocated = false;
		for (int j = 0; j < _num_workers; j++)
		{
			double priority = 0;
			cmIter = countmap[j].find(cur.color);
			if (cmIter != countmap[j].end())
			{
				priority = cmIter->second * (1 - assigned[j] / (double)capacity);  //calculate the priority of each work for current block
			}
			if ((priority > max) && (assigned[j] + cur.size <= capacity))
			{
				max = priority;
				wid = j;
				is_allocated = true;
			}
		}
		// allocation is failed, current block should be allocated to the work has much available space
		if (!is_allocated)
		{
			int minSize = assigned[0];
			for (int j = 1; j < _num_workers; j++)
			{
				if (minSize > assigned[j])
				{
					minSize = assigned[j];
					wid = j;
				}
			}
		}
		blk_to_slv[cur.color] = wid;
		assigned[wid] += cur.size;
		bcount[wid] ++;

		//update the countmap in worker W, insert or update the adjacent blocks' values with current block's size
		for (set<int>::iterator it = cur.nb_blocks.begin(); it != cur.nb_blocks.end(); it++)
		{
			cmIter = countmap[wid].find(*it);
			if (cmIter != countmap[wid].end())
				cmIter->second += cur.size;
			else
				countmap[wid][*it] = cur.size;
		}
	}


	cout << "* per-machine block assignment:" << endl;
	for (int i = 0; i < _num_workers; i++)
	{
		cout << "Machine_" << i << " is assigned " << bcount[i] << " blocks, " << assigned[i] << " vertices" << endl;
	}
	delete[] bcount;
	delete[] assigned;
	delete[] countmap;
}


//=====================================normal_BDGPartitioner=====================================
normal_BDGPartValue::normal_BDGPartValue() : color(-1) { }

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const normal_BDGPartValue& v)
{
	m << v.color;
	m << v.neighbors;
	m << v.nbs_info;
	return m;
}
obinstream& operator >> (obinstream& m, normal_BDGPartValue& v)
{
	m >> v.color;
	m >> v.neighbors;
	m >> v.nbs_info;
	return m;
}

void normal_BDGPartVorCombiner::combine(VertexID& old, const VertexID& new_msg) { } //ignore new_msg

void normal_BDGPartHashMinCombiner::combine(VertexID& old, const VertexID& new_msg)
{
	if (old > new_msg)
		old = new_msg;
}

normal_BDGPartVertex::normal_BDGPartVertex()
{
	srand((unsigned)time(NULL));
}

void normal_BDGPartVertex::broadcast(VertexID msg)
{
	vector<VertexID>& nbs = value().neighbors;
	for (int i = 0; i < nbs.size(); i++)
	{
		send_message(nbs[i], msg);
	}
}

void normal_BDGPartVertex::compute(MessageContainer& messages) //Voronoi Diagram partitioning algorithm
{
	if (step_num() == 1)
	{
		double samp = ((double)rand()) / RAND_MAX;
		if (samp <= global_sampling_rate)   //sampled
		{
			value().color = id;
			broadcast(id);
		}
		else   //not sampled
		{
			value().color = -1; //-1 means not assigned color
		}
		vote_to_halt();
	}
	else if (step_num() >= global_max_hop)
		vote_to_halt();
	else
	{
		if (value().color == -1)
		{
			VertexID msg = *(messages.begin());
			value().color = msg;
			broadcast(msg);
		}
		vote_to_halt();
	}
}


void normal_BDGPartitioner::set_voronoi_combiner()
{
	combiner_ = new normal_BDGPartVorCombiner;
	global_combiner = combiner_;
}

void normal_BDGPartitioner::set_hashmin_combiner()
{
	combiner_ = new normal_BDGPartHashMinCombiner;
	global_combiner = combiner_;
}

normal_BDGPartVertex* normal_BDGPartitioner::to_vertex(char* line)
{
	char* pch;
	pch = strtok(line, "\t");
	normal_BDGPartVertex* v = new normal_BDGPartVertex;
	v->id = atoi(pch);
	pch = strtok(NULL, " ");
	int num = atoi(pch);
	//v->value().color=-1;//default is -1
	while (num--)
	{
		pch = strtok(NULL, " ");
		v->value().neighbors.push_back(atoi(pch));
	}
	return v;
}

void normal_BDGPartitioner::to_line(normal_BDGPartVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
{
	sprintf(buf_, "%d %d\t", v->id, _my_rank);
	writer.write(buf_);
	vector<kvpair>& vec = v->value().nbs_info;
	for (int i = 0; i < vec.size(); i++)
	{
		sprintf(buf_, "%d %d ", vec[i].vid, vec[i].value);
		writer.write(buf_);
	}
	writer.write("\n");
}

void normal_BDGPartitioner::nb_info_exchange()
{
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 1 (send hash_table) =============" << endl;
	MapVec maps(_num_workers);
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID vid = (*it)->id;
		int blockID = (*it)->value().color;
		kvpair trip = { vid, blockID };
		vector<VertexID>& nbs = (*it)->value().neighbors;
		for (int i = 0; i < nbs.size(); i++)
		{
			maps[hash_(nbs[i])][vid] = trip;
		}
	}

	ExchangeT recvBuf(_num_workers);
	all_to_all(maps, recvBuf);
	hash_map<VertexID, kvpair>& mymap = maps[_my_rank];
	// gather all table entries
	for (int i = 0; i < _num_workers; i++)
	{
		if (i != _my_rank)
		{
			maps[i].clear(); //free sent table
			vector<IDTrip>& entries = recvBuf[i];
			for (int j = 0; j < entries.size(); j++)
			{
				IDTrip& idm = entries[j];
				mymap[idm.id] = idm.trip;
			}
		}
	}

	StopTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << get_timer(4) << " seconds elapsed" << endl;
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 2 =============" << endl;
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		normal_BDGPartVertex& vcur = **it;
		vector<VertexID>& nbs = vcur.value().neighbors;
		vector<kvpair>& infos = vcur.value().nbs_info;
		for (int i = 0; i < nbs.size(); i++)
		{
			VertexID nb = nbs[i];
			kvpair trip = mymap[nb];
			infos.push_back(trip);
		}
	}
	StopTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << get_timer(4) << " seconds elapsed" << endl;
}

//=====================================normal_BDGPartitioner=====================================


//=====================================label_BDGPartitioner======================================
label_BDGPartValue::label_BDGPartValue() : color(-1) { }

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const label_BDGPartValue& v)
{
	m << v.color;
	m << v.label;
	m << v.neighbors;
	m << v.nbs_info;
	return m;
}
obinstream& operator >> (obinstream& m, label_BDGPartValue& v)
{
	m >> v.color;
	m >> v.label;
	m >> v.neighbors;
	m >> v.nbs_info;
	return m;
}

void label_BDGPartVorCombiner::combine(VertexID& old, const VertexID& new_msg) { } //ignore new_msg

void label_BDGPartHashMinCombiner::combine(VertexID& old, const VertexID& new_msg)
{
	if (old > new_msg)
		old = new_msg;
}

label_BDGPartVertex::label_BDGPartVertex()
{
	srand((unsigned)time(NULL));
}

void label_BDGPartVertex::broadcast(VertexID msg)
{
	vector<nbInfo>& nbs = value().neighbors;
	for (int i = 0; i < nbs.size(); i++)
	{
		send_message(nbs[i].vid, msg);
	}
}

void label_BDGPartVertex::compute(MessageContainer& messages) //Voronoi Diagram partitioning algorithm
{
	if (step_num() == 1)
	{
		double samp = ((double)rand()) / RAND_MAX;
		if (samp <= global_sampling_rate)   //sampled
		{
			value().color = id;
			broadcast(id);
		}
		else   //not sampled
		{
			value().color = -1; //-1 means not assigned color
		}
		vote_to_halt();
	}
	else if (step_num() >= global_max_hop)
		vote_to_halt();
	else
	{
		if (value().color == -1)
		{
			VertexID msg = *(messages.begin());
			value().color = msg;
			broadcast(msg);
		}
		vote_to_halt();
	}
}


void label_BDGPartitioner::set_voronoi_combiner()
{
	combiner_ = new label_BDGPartVorCombiner;
	global_combiner = combiner_;
}

void label_BDGPartitioner::set_hashmin_combiner()
{
	combiner_ = new label_BDGPartHashMinCombiner;
	global_combiner = combiner_;
}

label_BDGPartVertex* label_BDGPartitioner::to_vertex(char* line)
{
	label_BDGPartVertex* v = new label_BDGPartVertex;
	char* pch;
	pch = strtok(line, " ");
	v->id = atoi(pch);
	pch = strtok(NULL, "\t");
	v->value().label = *pch;
	while ((pch = strtok(NULL, " ")) != NULL)
	{
		nbInfo item;
		item.vid = atoi(pch);
		pch = strtok(NULL, " ");
		item.label = *pch;
		v->value().neighbors.push_back(item);
	}
	return v;
}

void label_BDGPartitioner::to_line(label_BDGPartVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
{
	sprintf(buf_, "%d %c %d\t", v->id, v->value().label, _my_rank);
	writer.write(buf_);

	vector<nbInfo>& nbs = v->value().neighbors;
	vector<kvpair>& infos = v->value().nbs_info;

	for (int i = 0; i < nbs.size(); i++)
	{
		sprintf(buf_, "%d %c %d ", nbs[i].vid, nbs[i].label, infos[i].value);
		writer.write(buf_);
	}
	writer.write("\n");
}

void label_BDGPartitioner::nb_info_exchange()
{
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 1 (send hash_table) =============" << endl;
	MapVec maps(_num_workers);
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID vid = (*it)->id;
		int blockID = (*it)->value().color;
		kvpair trip = { vid, blockID };
		vector<nbInfo>& nbs = (*it)->value().neighbors;
		for (int i = 0; i < nbs.size(); i++)
		{
			maps[hash_(nbs[i].vid)][vid] = trip;
		}
	}

	ExchangeT recvBuf(_num_workers);
	all_to_all(maps, recvBuf);
	hash_map<VertexID, kvpair>& mymap = maps[_my_rank];
	// gather all table entries
	for (int i = 0; i < _num_workers; i++)
	{
		if (i != _my_rank)
		{
			maps[i].clear(); //free sent table
			vector<IDTrip>& entries = recvBuf[i];
			for (int j = 0; j < entries.size(); j++)
			{
				IDTrip& idm = entries[j];
				mymap[idm.id] = idm.trip;
			}
		}
	}

	StopTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << get_timer(4) << " seconds elapsed" << endl;
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 2 =============" << endl;
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		label_BDGPartVertex& vcur = **it;
		vector<nbInfo>& nbs = vcur.value().neighbors;
		vector<kvpair>& infos = vcur.value().nbs_info;
		for (int i = 0; i < nbs.size(); i++)
		{
			nbInfo & nb = nbs[i];
			kvpair trip = mymap[nb.vid];
			infos.push_back(trip);
		}
	}
	StopTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << get_timer(4) << " seconds elapsed" << endl;
}

//=====================================label_BDGPartitioner======================================


//======================================attr_BDGPartitioner======================================
attr_BDGPartValue::attr_BDGPartValue() : color(-1) { }

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const attr_BDGPartValue& v)
{
	m << v.color;
	m << v.attr;
	m << v.neighbors;
	m << v.nbs_info;
	return m;
}
obinstream& operator >> (obinstream& m, attr_BDGPartValue& v)
{
	m >> v.color;
	m >> v.attr;
	m >> v.neighbors;
	m >> v.nbs_info;
	return m;
}

void attr_BDGPartVorCombiner::combine(VertexID& old, const VertexID& new_msg)	{ } //ignore new_msg

void attr_BDGPartHashMinCombiner::combine(VertexID& old, const VertexID& new_msg)
{
	if (old > new_msg)
		old = new_msg;
}

attr_BDGPartVertex::attr_BDGPartVertex()
{
	srand((unsigned)time(NULL));
}

void attr_BDGPartVertex::broadcast(VertexID msg)
{
	vector<nbAttrInfo<AttrValueT> >& nbs = value().neighbors;
	for (int i = 0; i < nbs.size(); i++)
	{
		send_message(nbs[i].vid, msg);
	}
}

void attr_BDGPartVertex::compute(MessageContainer& messages) //Voronoi Diagram partitioning algorithm
{
	if (step_num() == 1)
	{
		double samp = ((double)rand()) / RAND_MAX;
		if (samp <= global_sampling_rate)   //sampled
		{
			value().color = id;
			broadcast(id);
		}
		else   //not sampled
		{
			value().color = -1; //-1 means not assigned color
		}
		vote_to_halt();
	}
	else if (step_num() >= global_max_hop)
		vote_to_halt();
	else
	{
		if (value().color == -1)
		{
			VertexID msg = *(messages.begin());
			value().color = msg;
			broadcast(msg);
		}
		vote_to_halt();
	}
}


void attr_BDGPartitioner::set_voronoi_combiner()
{
	combiner_ = new attr_BDGPartVorCombiner;
	global_combiner = combiner_;
}

void attr_BDGPartitioner::set_hashmin_combiner()
{
	combiner_ = new attr_BDGPartHashMinCombiner;
	global_combiner = combiner_;
}

attr_BDGPartVertex* attr_BDGPartitioner::to_vertex(char* line)
{
	//char sampleLine[] = "0\t1983 166 1 ball;tennis \t3 1 (0.1 )2 (0.2 )3 (0.3 )";
	attr_BDGPartVertex* v = new attr_BDGPartVertex;
	char* pch;
	pch = strtok(line, "\t"); //0\t
	v->id = atoi(pch);

	vector<AttrValueT> a;
	pch = strtok(NULL, "\t"); //1983 166 ball;tennis \t
	char* attr_line = new char[strlen(pch) + 1];
	strcpy(attr_line, pch);
	attr_line[strlen(pch)] = '\0';

	pch = strtok(NULL, " ");
	int num = atoi(pch);
	while (num--)
	{
		nbAttrInfo<AttrValueT> item;
		pch = strtok(NULL, " ");
		item.vid = atoi(pch);
		if(USE_MULTI_ATTR)
		{
			pch = strtok(NULL, " ");
			item.attr.get_attr_vec().push_back(pch);
		}
		v->value().neighbors.push_back(item);
	}

	pch = strtok(attr_line, " ");
	a.push_back(pch);
	while ((pch = strtok(NULL, " ")) != NULL)
		a.push_back(pch);
	v->value().attr.init_attr_vec(a);

	delete[] attr_line;
	return v;
}

void attr_BDGPartitioner::to_line(attr_BDGPartVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
{
	//char sampleOutputLine[] = "0 5\t1983 166 1 ball;tennis \t1 (0.1 )1 2 (0.2 )2 ";
	sprintf(buf_, "%d %d\t", v->id, _my_rank);
	writer.write(buf_);

	vector<AttrValueT>& attrVec = v->value().attr.get_attr_vec();
	for (size_t i = 0; i < attrVec.size(); ++i)
	{
		sprintf(buf_, "%s ", attrVec[i].c_str());
		writer.write(buf_);
	}
	writer.write("\t");

	vector<nbAttrInfo<AttrValueT> >& nbs = v->value().neighbors;
	vector<kvpair>& infos = v->value().nbs_info;

	for (int i = 0; i < infos.size(); i++)
	{
		if(!USE_MULTI_ATTR)
			sprintf(buf_, "%d %d ", infos[i].vid, infos[i].value);
		else
			sprintf(buf_, "%d %s %d ", nbs[i].vid, nbs[i].attr.get_attr_vec().front().c_str(), infos[i].value);
		writer.write(buf_);
	}
	writer.write("\n");
}

void attr_BDGPartitioner::nb_info_exchange()
{
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 1 (send hash_table) =============" << endl;
	MapVec maps(_num_workers);
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID vid = (*it)->id;
		int blockID = (*it)->value().color;
		kvpair trip = { vid, blockID };
		vector<nbAttrInfo<AttrValueT> >& nbs = (*it)->value().neighbors;
		for (int i = 0; i < nbs.size(); i++)
		{
			maps[hash_(nbs[i].vid)][vid] = trip;
		}
	}

	ExchangeT recvBuf(_num_workers);
	all_to_all(maps, recvBuf);
	hash_map<VertexID, kvpair>& mymap = maps[_my_rank];
	// gather all table entries
	for (int i = 0; i < _num_workers; i++)
	{
		if (i != _my_rank)
		{
			maps[i].clear(); //free sent table
			vector<IDTrip>& entries = recvBuf[i];
			for (int j = 0; j < entries.size(); j++)
			{
				IDTrip& idm = entries[j];
				mymap[idm.id] = idm.trip;
			}
		}
	}

	StopTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << get_timer(4) << " seconds elapsed" << endl;
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 2 =============" << endl;
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		attr_BDGPartVertex& vcur = **it;
		vector<nbAttrInfo<AttrValueT> >& nbs = vcur.value().neighbors;
		vector<kvpair>& infos = vcur.value().nbs_info;
		for (int i = 0; i < nbs.size(); i++)
		{
			nbAttrInfo<AttrValueT> & nb = nbs[i];
			kvpair trip = mymap[nb.vid];
			infos.push_back(trip);
		}
	}
	StopTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << get_timer(4) << " seconds elapsed" << endl;
}

//======================================attr_BDGPartitioner======================================
