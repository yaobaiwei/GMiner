//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template<class BPartVertexT>
void BPartitioner<BPartVertexT>::unset_combiner() //must call after Voronoi sampling
{
	global_combiner = NULL;
	delete combiner_;
	combiner_ = NULL;
}

template<class BPartVertexT>
void BPartitioner<BPartVertexT>::voronoi_compute()
{
	//returns true if all colored && no overflowing Voronoi cell
	//collect Voronoi cell size
	HashMap map;
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID color = (*it)->value().color;
		if (color != -1)
		{
			MapIter mit = map.find(color);
			if (mit == map.end())
				map[color] = 1;
			else
				mit->second++;
		}
	}

	//colorMap aggregating
	if (_my_rank != MASTER_RANK)   //send partialT to aggregator
	{
		//%%%%%% gathering colorMap
		slave_gather(map);
		//%%%%%% scattering FinalT
		slave_bcast(map);
	}
	else
	{
		//%%%%%% gathering colorMap
		vector<HashMap> parts(_num_workers);
		master_gather(parts);

		for (int i = 0; i < _num_workers; i++)
		{
			if (i != MASTER_RANK)
			{
				HashMap& part = parts[i];
				for (MapIter it = part.begin(); it != part.end(); it++)
				{
					VertexID color = it->first;
					MapIter mit = map.find(color);
					if (mit == map.end())
						map[color] = it->second;
					else
						mit->second += it->second;
				}
			}
		}
		//%%%%%% scattering FinalT
		master_bcast(map);
	}

	//===============================================
	active_count_ = 0;
	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		VertexID color = (*it)->value().color;
		if (color == -1) //case 1: not colored
		{
			(*it)->activate();
			active_count_++;
		}
		else
		{
			MapIter mit = map.find(color);

			if (mit->second > global_max_vcsize)   //case 2: Voronoi cell size overflow
			{
				(*it)->activate();
				(*it)->value().color = -1; //erase old color
				active_count_++;
			}
		}
	}
}

template<class BPartVertexT>
void BPartitioner<BPartVertexT>::subG_hashmin()
{
	if (_my_rank == MASTER_RANK)
		cout << "-------------------------- Switch to subgraph HashMin --------------------------" << endl;
	set_hashmin_combiner();
	//adapted HashMin: only works on the subgraph induced by active vertices
	int size = vertexes_.size();
	VertexContainer subgraph; //record subgraph vertices
	for (int i = 0; i < size; i++)
	{
		if (vertexes_[i]->is_active())
			subgraph.push_back(vertexes_[i]);
	}
	global_step_num = 0;
	while (true)
	{
		global_step_num++;
		ResetTimer(4);
		//===================
		char bits_bor = all_bor(global_bor_bitmap);
		active_vnum() = all_sum(active_count_);
		if (active_vnum() == 0 && get_bit(HAS_MSG_ORBIT, bits_bor) == 0)
			break;
		//===================
		clear_bits();
		//-------------------
		active_count_ = 0;
		MessageBufT* mbuf = (MessageBufT*)get_message_buffer();
		mbuf->reinit(subgraph);
		vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
		for (int pos = 0; pos < subgraph.size(); pos++)
		{
			BPartVertexT& v = *subgraph[pos];
			if (step_num() == 1)
			{
				//not all neighbors are in subgraph
				//cannot init using min{neighbor_id}
				VertexID min = v.id;
				v.value().color = min;
				v.broadcast(v.id);
				v.vote_to_halt();
			}
			else
			{
				if (v_msgbufs[pos].size() > 0)
				{
					vector<VertexID>& messages = v_msgbufs[pos];
					VertexID min = messages[0];
					for (int i = 1; i < messages.size(); i++)
					{
						if (min > messages[i])
							min = messages[i];
					}
					if (min < v.value().color)
					{
						v.value().color = min;
						v.broadcast(min);
					}
					v_msgbufs[pos].clear(); //clear used msgs
				}
				v.vote_to_halt();
			}
		}
		//-------------------
		message_buffer_->sync_messages();
		//===================
		//worker_barrier();
		StopTimer(4);
		if (_my_rank == MASTER_RANK)
			cout << "Superstep " << global_step_num << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
	}
	unset_combiner(); //stop subgraph HashMin
	if (_my_rank == MASTER_RANK)
		cout << "-------------------------- Subgraph HashMin END --------------------------" << endl;
}

template<class BPartVertexT>
int BPartitioner<BPartVertexT>::slave_of(BPartVertexT* v)
{
	return blk_to_slv_[v->value().color];
}

template<class BPartVertexT>
int BPartitioner<BPartVertexT>::num_digits(int number)
{
	int cur = 1;
	while (number != 0)
	{
		number /= 10;
		cur *= 10;
	}
	return cur;
}

template<class BPartVertexT>
void BPartitioner<BPartVertexT>::block_sync()
{
	//set send buffer
	vector<vector<BPartVertexT*> > _loaded_parts(_num_workers);
	for (int i = 0; i < vertexes_.size(); i++)
	{
		BPartVertexT* v = vertexes_[i];
		_loaded_parts[slave_of(v)].push_back(v);
	}
	//exchange vertices to add
	all_to_all(_loaded_parts);

	vertexes_.clear();
	//collect vertices to add
	for (int i = 0; i < _num_workers; i++)
	{
		vertexes_.insert(vertexes_.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
	}

	//===========set neighbor-value by the proper worker id ============
	for (int i = 0; i < vertexes_.size(); i++)
	{
		vector<kvpair> & nbs_info = vertexes_[i]->value().nbs_info;
		for (int j = 0; j < nbs_info.size(); j++)
		{
			nbs_info[j].value = blk_to_slv_[nbs_info[j].value];
		}
	}

	_loaded_parts.clear();
}

template<class BPartVertexT>
void BPartitioner<BPartVertexT>::active_compute()
{
	active_count_ = 0;
	MessageBufT* mbuf = (MessageBufT*)get_message_buffer();
	vector<MessageContainerT>& v_msgbufs = mbuf->get_v_msg_bufs();
	for (int i = 0; i < vertexes_.size(); i++)
	{
		if (v_msgbufs[i].size() == 0)
		{
			if (vertexes_[i]->is_active())
			{
				vertexes_[i]->compute(v_msgbufs[i]);
				if (vertexes_[i]->is_active())
					active_count_++;
			}
		}
		else
		{
			vertexes_[i]->activate();
			vertexes_[i]->compute(v_msgbufs[i]);
			v_msgbufs[i].clear(); //clear used msgs
			if (vertexes_[i]->is_active())
				active_count_++;
		}
	}
}

template<class BPartVertexT>
void BPartitioner<BPartVertexT>::load_vertex(BPartVertexT* v)
{
	//called by load_graph
	vertexes_.push_back(v);
	if (v->is_active())
	{
		active_count_ += 1;
	}
}

template<class BPartVertexT>
void BPartitioner<BPartVertexT>::sync_graph()
{
	//set send buffer
	vector<vector<BPartVertexT*> > _loaded_parts(_num_workers);
	for (int i = 0; i < vertexes_.size(); i++)
	{
		BPartVertexT* v = vertexes_[i];
		_loaded_parts[hash_(v->id)].push_back(v);
	}
	//exchange vertices to add
	all_to_all(_loaded_parts);

	vertexes_.clear();
	//collect vertices to add
	for (int i = 0; i < _num_workers; i++)
	{
		vertexes_.insert(vertexes_.end(), _loaded_parts[i].begin(), _loaded_parts[i].end());
	}
	//===== deprecated ======
	_loaded_parts.clear();

};

template<class BPartVertexT>
BPartitioner<BPartVertexT>::BPartitioner()
{
	combiner_ = NULL;
	message_buffer_ = new MessageBufT;
	global_message_buffer = message_buffer_;
	global_combiner = NULL;
	global_aggregator = NULL;
	global_agg = NULL;
}

template<class BPartVertexT>
BPartitioner<BPartVertexT>::~BPartitioner()
{
	delete message_buffer_;
}

// run the worker
template<class BPartVertexT>
void BPartitioner<BPartVertexT>::run(const WorkerParams& params)
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
		vector<string>& assignedSplits = (*arrangement)[0];
		//reading assigned splits (map)
		for (vector<string>::iterator it = assignedSplits.begin();
		        it != assignedSplits.end(); it++)
			load_graph(it->c_str());
		delete arrangement;
	}
	else
	{
		vector<string> assignedSplits;
		slave_scatter(assignedSplits);
		//reading assigned splits (map)
		for (vector<string>::iterator it = assignedSplits.begin();
		        it != assignedSplits.end(); it++)
			load_graph(it->c_str());
	}

	//send vertices according to hash_id (reduce)
	sync_graph();

	//barrier for data loading
	worker_barrier();
	StopTimer(WORKER_TIMER);
	PrintTimer("Load Time", WORKER_TIMER);
	message_buffer_->init(vertexes_);
	//=========================================================

	init_timers();
	ResetTimer(WORKER_TIMER);

	if (_my_rank == MASTER_RANK)
		cout << "-------------------------- Voronoi Partitioning BEGIN --------------------------" << endl;
	set_voronoi_combiner(); //start Voronoi sampling
	get_vnum() = all_sum(vertexes_.size());
	int lastNum = get_vnum();
	if (_my_rank == MASTER_RANK)
		cout << "* |V| = " << lastNum << endl;

	//supersteps
	int round = 1;
	if (_my_rank == MASTER_RANK)
		cout << "==================== Round " << round << " ====================" << endl;
	global_step_num = 0;

	while (true)
	{
		global_step_num++;
		ResetTimer(4);
		//===================
		char bits_bor = all_bor(global_bor_bitmap);
		active_vnum() = all_sum(active_count_);
		if (round > 1 && global_step_num == 1)   //{ reporting
		{
			if (active_vnum() == 0)
			{
				if (_my_rank == MASTER_RANK)
					cout << "-------------------------- Voronoi Partitioning END --------------------------" << endl;
				if (_my_rank == MASTER_RANK)
					cout << "* all vertices are processed by Voronoi partitioning" << endl;
				break;
			}
			double ratio = (double)active_vnum() / lastNum;
			if (_my_rank == MASTER_RANK)
				cout << "* " << active_vnum() << " vertices (" << 100 * ((double)active_vnum() / get_vnum()) << "%) activated, #{this_round}/#{last_round}=" << 100 * ratio << "%" << endl;
			if (ratio > global_stop_ratio)
			{
				if (_my_rank == MASTER_RANK)
					cout << "-------------------------- Voronoi Partitioning END --------------------------" << endl;
				break;
			}
			lastNum = active_vnum();
		} //reporting }

		if (active_vnum() == 0 && get_bit(HAS_MSG_ORBIT, bits_bor) == 0) //all_halt AND no_msg
		{
			if (_my_rank == MASTER_RANK)
				cout << "Inter-Round Aggregating ..." << endl;
			voronoi_compute();
			round++;
			global_step_num = 0;
			//--------------------
			global_sampling_rate *= global_factor; //increase sampling rate before voronoi_compute();
			if (_my_rank == MASTER_RANK)
				cout << "current sampling rate = " << global_sampling_rate << endl;
			if (global_sampling_rate > global_max_rate)
			{
				if (_my_rank == MASTER_RANK)
					cout << "Sampling rate > " << global_max_rate << ", Voronoi sampling terminates" << endl;
				active_vnum() = all_sum(active_count_); //important for correctness!!!
				break;
			}
			//--------------------
			if (_my_rank == MASTER_RANK)
				cout << "==================== Round " << round << " ====================" << endl;
			continue;
		}
		else
		{
			clear_bits();
			active_compute();
			message_buffer_->sync_messages();
		}
		//===================
		worker_barrier();
		StopTimer(4);
		if (_my_rank == MASTER_RANK)
			cout << "Superstep " << global_step_num << " of Round " << round << " done. Time elapsed: " << get_timer(4) << " seconds" << endl;
	}

	unset_combiner(); //stop Voronoi sampling

	if (active_vnum() > 0)
		subG_hashmin();

	nb_info_exchange();

	if (_my_rank == MASTER_RANK)
		cout << "============ blk2slv ASSIGNMENT ============" << endl;
	ResetTimer(4);

	block_assign();

	StopTimer(4);
	if (_my_rank == MASTER_RANK)
	{
		cout << "============ blk2slv DONE ============" << endl;
		cout << "Time elapsed: " << get_timer(4) << " seconds" << endl;
	}

	if (_my_rank == MASTER_RANK)
		cout << "SYNC VERTICES ......" << endl;
	ResetTimer(4);
	block_sync();
	StopTimer(4);
	if (_my_rank == MASTER_RANK)
		cout << "SYNC done in " << get_timer(4) << " seconds" << endl;

	// =============statistics of cross edges between workers===========
	long long cross_edges = 0;
	long long total_edges = 0;
	long long cross_vertexes = 0;
	long long total_vertexes = all_sum_LL(vertexes_.size());

	for (int i = 0; i < vertexes_.size(); i++)
	{
		BPartVertexT & item = *(vertexes_[i]);
		int nb_size = item.value().nbs_info.size();
		int cross_num = 0;
		total_edges += nb_size;
		for (int j = 0; j < nb_size; j++)
		{
			if (blk_to_slv_[item.value().color] != item.value().nbs_info[j].value)
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

	blk_to_slv_.clear();
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
