//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template<class BHashVertexT>
HashPartitioner<BHashVertexT>::HashPartitioner()
{
	global_message_buffer = NULL;
	global_combiner = NULL;
	global_aggregator = NULL;
	global_agg = NULL;
}

template<class BHashVertexT>
HashPartitioner<BHashVertexT>::~HashPartitioner() { }

template<class BHashVertexT>
void HashPartitioner<BHashVertexT>::load_vertex(BHashVertexT* v)
{
	vertexes_.push_back(v);  //called by load_graph
}

template<class BHashVertexT>
void HashPartitioner<BHashVertexT>::sync_graph()
{
	//set send buffer
	vector<vector<BHashVertexT*> > _loaded_parts(_num_workers);
	for (int i = 0; i < vertexes_.size(); i++)
	{
		BHashVertexT* v = vertexes_[i];
		_loaded_parts[v->value().block].push_back(v);
	}
	//exchange vertices to add, delete sent vertices in all_to_all
	all_to_all(_loaded_parts);

	vertexes_.clear();
	//collect vertices to add
	for (int i = 0; i < _num_workers; i++)
	{
		vertexes_.insert(vertexes_.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
	}
	_loaded_parts.clear();
}

// run the worker
template<class BHashVertexT>
void HashPartitioner<BHashVertexT>::run(const WorkerParams& params)
{
	//check path + init
	if (_my_rank == MASTER_RANK)
	{
		if (dir_check(params.input_path.c_str(), params.output_path.c_str(), _my_rank == MASTER_RANK, params.force_write) == -1)
			return;
	}
	init_timers();

	//dispatch splits
	ResetTimer(WORKER_TIMER);
	vector<vector<string> >* arrangement;
	if (_my_rank == MASTER_RANK)
	{
		arrangement = params.native_dispatcher ? dispatch_locality(params.input_path.c_str()) : dispatch_run(params.input_path.c_str());
		master_scatter(*arrangement);
		vector<string>& assigned_splits = (*arrangement)[0];
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin();
		        it != assigned_splits.end(); it++)
			load_graph(it->c_str());
		delete arrangement;
        std::cout << _my_rank <<" vertex "<<vertexes_.size() <<std::endl;
	}
	else
	{
		vector<string> assigned_splits;
		slave_scatter(assigned_splits);
		//reading assigned splits (map)
		for (vector<string>::iterator it = assigned_splits.begin();
		        it != assigned_splits.end(); it++)
			load_graph(it->c_str());
        std::cout << _my_rank <<" vertex "<<vertexes_.size() <<std::endl;
	}

	//send vertices according to hash_id (reduce)
	sync_graph();

	//barrier for data loading
	worker_barrier();
	StopTimer(WORKER_TIMER);
	PrintTimer("Load Time", WORKER_TIMER);

	//=========================================================

	nb_info_exchange();

	// =============statistics of cross edges between workers===========
	long long cross_edges = 0;
	long long total_edges = 0;
	long long cross_vertexes = 0;
	long long total_vertexes = all_sum_LL(vertexes_.size());

	for (int i = 0; i < vertexes_.size(); i++)
	{
		BHashVertexT & item = *(vertexes_[i]);
		int nb_size = item.value().nbs_info.size();
		int cross_num = 0;
		total_edges += nb_size;
		for (int j = 0; j < nb_size; j++)
		{
			if (_my_rank != item.value().nbs_info[j].value)
				cross_num++;
		}
		if (cross_num)
			cross_vertexes++;
		cross_edges += cross_num;
	}

	cross_edges = all_sum_LL(cross_edges);
	total_edges = all_sum_LL(total_edges);
	cross_vertexes = all_sum_LL(cross_vertexes);

	if (_my_rank == MASTER_RANK)
	{
		cout << "===========The number of cross edges :  " << cross_edges / 2 << "  ============" << endl;
		cout << "===========The total number of  edges :  " << total_edges / 2 << "  ============" << endl;
		cout << "===========The number of cross vertexes :  " << cross_vertexes << "  ============" << endl;
		cout << "===========The total number of  vertexes :  " << total_vertexes << "  ============" << endl;
	}
	//========================================================================

	worker_barrier();
	StopTimer(WORKER_TIMER);
	PrintTimer("Communication Time", COMMUNICATION_TIMER);
	PrintTimer("- Serialization Time", SERIALIZATION_TIMER);
	PrintTimer("- Transfer Time", TRANSFER_TIMER);
	PrintTimer("Total Computational Time", WORKER_TIMER);
	// dump graph
	ResetTimer(WORKER_TIMER);
	char* fpath = new char[params.output_path.length() + 10];
	strcpy(fpath, params.output_path.c_str());
	strcat(fpath, "/part_");
	char buffer[5];
	sprintf(buffer, "%d", _my_rank);
	strcat(fpath, buffer);
	dump_partition(fpath);
	delete fpath;
	worker_barrier();
	StopTimer(WORKER_TIMER);
	PrintTimer("Dump Time", WORKER_TIMER);
}


//=====================================normal_HashPartitioner=====================================
normal_BHashValue::normal_BHashValue() : block(-1) { }

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const normal_BHashValue& v)
{
	m << v.block;
	m << v.neighbors;
	m << v.nbs_info;
	return m;
}
obinstream& operator >> (obinstream& m, normal_BHashValue& v)
{
	m >> v.block;
	m >> v.neighbors;
	m >> v.nbs_info;
	return m;
}

void normal_BHashVertex::compute(MessageContainer& messages) { } //not used


normal_BHashVertex* normal_HashPartitioner::to_vertex(char* line)
{
	char* pch;
	pch = strtok(line, "\t");
	normal_BHashVertex* v = new normal_BHashVertex;
	v->id = atoi(pch);
	v->value().block = hash_(v->id);
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

void normal_HashPartitioner::to_line(normal_BHashVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
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

void normal_HashPartitioner::nb_info_exchange()
{
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 1 (send hash_table) =============" << endl;
	MapVec maps(_num_workers);
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID vid = (*it)->id;
		kvpair trip = { vid,  _my_rank };
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
		normal_BHashVertex& vcur = **it;
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

//=====================================normal_HashPartitioner=====================================


//=====================================label_HashPartitioner======================================
label_BHashValue::label_BHashValue() : block(-1) { }

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const label_BHashValue& v)
{
	m << v.block;
	m << v.label;
	m << v.neighbors;
	m << v.nbs_info;
	return m;
}
obinstream& operator >> (obinstream& m, label_BHashValue& v)
{
	m >> v.block;
	m >> v.label;
	m >> v.neighbors;
	m >> v.nbs_info;
	return m;
}

void label_BHashVertex::compute(MessageContainer& messages) { } //not used


label_BHashVertex* label_HashPartitioner::to_vertex(char* line)
{
	label_BHashVertex* v = new label_BHashVertex;
	char* pch;
	pch = strtok(line, " ");
	v->id = atoi(pch);
	pch = strtok(NULL, "\t");
	v->value().label = *pch;
	v->value().block = hash_(v->id);
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

void label_HashPartitioner::to_line(label_BHashVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
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

void label_HashPartitioner::nb_info_exchange()
{
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 1 (send hash_table) =============" << endl;
	MapVec maps(_num_workers);
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID vid = (*it)->id;
		kvpair trip = { vid, _my_rank };
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
		label_BHashVertex& vcur = **it;
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

//=====================================label_HashPartitioner======================================


//======================================attr_HashPartitioner======================================
attr_BHashValue::attr_BHashValue() : block(-1) { }

//no vertex-add, will not be called
ibinstream& operator << (ibinstream& m, const attr_BHashValue& v)
{
	m << v.block;
	m << v.attr;
	m << v.neighbors;
	m << v.nbs_info;
	return m;
}
obinstream& operator >> (obinstream& m, attr_BHashValue& v)
{
	m >> v.block;
	m >> v.attr;
	m >> v.neighbors;
	m >> v.nbs_info;
	return m;
}

void attr_BHashVertex::compute(MessageContainer& messages) { } //not used


attr_BHashVertex* attr_HashPartitioner::to_vertex(char* line)
{
	//char sampleLine[] = "0\t1983 166 1 ball;tennis \t3 1 (0.1 )2 (0.2 )3 (0.3 )";
	attr_BHashVertex* v = new attr_BHashVertex;
	char* pch;
	pch = strtok(line, "\t"); //0\t
	v->id = atoi(pch);
	v->value().block = hash_(v->id);

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

void attr_HashPartitioner::to_line(attr_BHashVertex* v, BufferedWriter& writer) //key: "vertexID blockID slaveID"
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

void attr_HashPartitioner::nb_info_exchange()
{
	ResetTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "============= Neighbor InfoExchange Phase 1 (send hash_table) =============" << endl;
	MapVec maps(_num_workers);
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID vid = (*it)->id;
		kvpair trip = { vid, _my_rank };
		vector<nbAttrInfo<AttrValueT> >& nbs = (*it)->value().neighbors;
		for (int i = 0; i < nbs.size(); i++)
		{
			maps[hash_(nbs[i].vid)][vid] = trip;
		}
	}

	ExchangeT recv_buf(_num_workers);
	all_to_all(maps, recv_buf);
	hash_map<VertexID, kvpair>& mymap = maps[_my_rank];
	// gather all table entries
	for (int i = 0; i < _num_workers; i++)
	{
		if (i != _my_rank)
		{
			maps[i].clear(); //free sent table
			vector<IDTrip>& entries = recv_buf[i];
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
		attr_BHashVertex& vcur = **it;
		vector<nbAttrInfo<AttrValueT> >& nbs = vcur.value().neighbors;
		vector<kvpair>& infos = vcur.value().nbs_info;
		for (int i = 0; i < nbs.size(); i++)
		{
			nbAttrInfo<AttrValueT>  & nb = nbs[i];
			kvpair trip = mymap[nb.vid];
			infos.push_back(trip);
		}
	}

	StopTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << get_timer(4) << " seconds elapsed" << endl;
}

//======================================attr_HashPartitioner======================================
