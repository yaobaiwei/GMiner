//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#define USE_ATTRIBUTE

#include "core/subg-dev.hpp"

using namespace std;


typedef double AttrValueT;
typedef hash_set<VertexID> VtxIdSet;


AttrValueT MIN_WEIGHT = 1;	// threshold that only add the new candidates into subgraph with weight >= MIN_WEIGHT
size_t MIN_CORE_SIZE = 10;	// threshold that only compute the seedtask with its subgraph size >= MIN_CORE_SIZE
size_t MIN_RESULT_SIZE = 0;	// threshold that only store the result cluster with size >= MIN_RESULT_SIZE
double DIFF_RATIO = 0.01;	// threshold for judging two weight is similarity or not
size_t ITER_ROUND_MAX = 10;	// threshold that only compute number of iterations < ITER_ROUND_MAX with each task
double CAND_MAX_TIME = 20;	// threshold that only compute the top CAND_MAX_TIME*subgraph_size candidates in each round during computation


inline bool is_sim_weight(const double pos_l_a, const double pos_g_b, const double DIFF_RATIO)
{
	return (pos_g_b / pos_l_a) <= (DIFF_RATIO + 1);
}


struct FocusContext
{
	vector<VertexID> cluster;
	VtxIdSet outlier;

	size_t comp_round;
	size_t iter_round;
	AttrValueT init_weight_phi;
	hash_map<VertexID, int> subg_wid_map;
	hash_map<VertexID, int> cand_wid_map;
};

ibinstream& operator << (ibinstream& m, const FocusContext& v)
{
	m << v.cluster;
	m << v.outlier;

	m << v.comp_round;
	m << v.iter_round;
	m << v.init_weight_phi;
	m << v.subg_wid_map;
	m << v.cand_wid_map;
	return m;
}

obinstream& operator >> (obinstream& m, FocusContext& v)
{
	m >> v.cluster;
	m >> v.outlier;

	m >> v.comp_round;
	m >> v.iter_round;
	m >> v.init_weight_phi;
	m >> v.subg_wid_map;
	m >> v.cand_wid_map;
	return m;
}

ifbinstream& operator << (ifbinstream& m, const FocusContext& v)
{
	m << v.cluster;
	m << v.outlier;

	m << v.comp_round;
	m << v.iter_round;
	m << v.init_weight_phi;
	m << v.subg_wid_map;
	m << v.cand_wid_map;
	return m;
}

ofbinstream& operator >> (ofbinstream& m, FocusContext & v)
{
	m >> v.cluster;
	m >> v.outlier;

	m >> v.comp_round;
	m >> v.iter_round;
	m >> v.init_weight_phi;
	m >> v.subg_wid_map;
	m >> v.cand_wid_map;
	return m;
}


struct FocusResult
{
	map<vector<VertexID>, VtxIdSet> co_map;
};

ibinstream& operator<<(ibinstream& m, const FocusResult& v)
{
	m << v.co_map;
	return m;
}

obinstream& operator >> (obinstream& m, FocusResult& v)
{
	m >> v.co_map;
	return m;
}

ifbinstream& operator<<(ifbinstream& m, const FocusResult& v)
{
	m << v.co_map;
	return m;
}

ofbinstream& operator >> (ofbinstream& m, FocusResult & v)
{
	m >> v.co_map;
	return m;
}


class CountAgg :public Aggregator<FocusContext, FocusResult, FocusResult>
{
public:
	virtual ~CountAgg() {}

	virtual void init() {}

	virtual void step_partial(FocusContext& v)
	{
		if (v.cluster.empty())
			return;
		count_.co_map.insert(make_pair(std::move(v.cluster), std::move(v.outlier)));
	}

	virtual void step_final(FocusResult* part)
	{
		count_.co_map.insert(part->co_map.begin(), part->co_map.end());
	}

	virtual FocusResult* finish_partial()
	{
		return &count_;
	}
	virtual FocusResult* finish_final()
	{
		return &count_;
	}

private:
	FocusResult count_;
};


class FocusTask :public Task<VertexID, FocusContext, Attribute<AttrValueT> >
{
public:

	virtual bool compute(SubgraphT & g, ContextType & context, vector<VertexT *> & frontier)
	{
		vector<VertexID>& cluster = context.cluster;
		VtxIdSet& outlier = context.outlier;
		size_t& comp_round = context.comp_round;
		++comp_round;
		hash_map<VertexID, int>& subg_wid_map = context.subg_wid_map;
		hash_map<VertexID, int>& cand_wid_map = context.cand_wid_map;

		if (comp_round == 1)
		{
			//cout<<"task::compute::CompRound==1 return"<<endl;
			return init_real_comp(frontier, subg_wid_map, cand_wid_map);
		}

		hash_map<VertexID, VertexT*> vertex_idx_map;
		for (vector<VertexT *>::iterator iter = frontier.begin(); iter != frontier.end(); ++iter)
			vertex_idx_map.insert(make_pair((*iter)->id, *iter));

		VtxIdSet subG;
		list<NodeT>& nodes = g.get_nodes();
		for (list<NodeT>::iterator iter = nodes.begin(); iter != nodes.end(); ++iter)
		{
			subG.insert(iter->id);
		}

		AttrValueT cur_weight_phi = get_phi(subG, true, vertex_idx_map);
		if (cur_weight_phi < 0)
		{
			cluster.clear();
			outlier.clear();
			//cout<<"task::compute curWeightPhi<0 return"<<endl;
			return false;
		}

		//if(_my_rank==22) // miao debug
		//cout << "CompRound=" << CompRound << " IterRound=" << context.IterRound << " subgSize=" << subG.size() << " candSize=" << cand_wid_map.size() << " curWeightPhi=" << curWeightPhi << " at " << _my_rank << endl;

		if (comp_round == 2)
			context.init_weight_phi = cur_weight_phi;

		//------------------------- expansion begin -------------------------
		AttrValueT cur_struct_phi = get_phi(subG, false, vertex_idx_map);
		VertexID best_weight_node, best_struct_node;
		AttrValueT best_weight_diff;
		expand_once(subG, cur_weight_phi, cur_struct_phi, best_weight_diff, best_weight_node, best_struct_node, cand_wid_map, vertex_idx_map);
		if (is_sim_weight(cur_weight_phi + best_weight_diff, cur_weight_phi, DIFF_RATIO))
		{
			best_struct_node = -1;
			best_weight_node = -1;
		}

		if (best_struct_node != -1)
			outlier.insert(best_struct_node);
		if (best_weight_node != -1)
		{
			hash_map<VertexID, int>::iterator new_node_iter = cand_wid_map.find(best_weight_node);
			NodeT new_node(new_node_iter->first);
			g.add_node(new_node);
			subG.insert(new_node_iter->first);
			subg_wid_map.insert(make_pair(new_node_iter->first, new_node_iter->second));

			cand_wid_map.clear();
			multimap<AttrValueT, VertexID, greater<AttrValueT> > topK_cands;
			for (VtxIdSet::iterator iter = subG.begin(); iter != subG.end(); ++iter)
			{
				hash_map<VertexID, VertexT*>::iterator vtx_pit = vertex_idx_map.find(*iter);
				if (vtx_pit == vertex_idx_map.end())
				{
					cout << "current VertexID is not in vertex_idx_map(fronter) in function FocusTask::compute contraction" << endl;
					abort();
				}
				VertexT* vtx = vtx_pit->second;
				AdjVtxList& adjlist = vtx->get_adjlist();
				for (AdjVtxIter it = adjlist.begin(); it != adjlist.end(); ++it)
				{
					AttrValueT weight = it->attr.get_attr_vec().front();
					if (subG.find(it->id) == subG.end() && weight > 0) //not in subg && edgeWeight is not illegal
					{
						pair<hash_map<VertexID, int>::iterator, bool> ist_flag = cand_wid_map.insert(make_pair(it->id, it->wid));
						if (ist_flag.second == true)
							topK_cands.insert(make_pair(weight, it->id));
					}
				}
			}
			if (topK_cands.size() > CAND_MAX_TIME*subG.size())
			{
				multimap<AttrValueT, VertexID, greater<AttrValueT> >::iterator new_end = topK_cands.begin();
				advance(new_end, CAND_MAX_TIME*subG.size());
				for (multimap<AttrValueT, VertexID, greater<AttrValueT> >::iterator it = new_end; it != topK_cands.end(); ++it)
				{
					hash_map<VertexID, int>::iterator cand_iter = cand_wid_map.find(it->second);
					if (cand_iter != cand_wid_map.end())
						cand_wid_map.erase(cand_iter);
				}
			}

			calc_and_pull_next(subg_wid_map, cand_wid_map);
			//cout<<"task::compute expandOnce newNode return"<<endl;
			return true;
		}
		//-------------------------- expansion end --------------------------

		//------------------------ contraction begin ------------------------
		vector<VertexID> del_vec;
		contract(subG, cur_weight_phi, del_vec, vertex_idx_map);
		if (!del_vec.empty())
		{
			for (vector<VertexID>::iterator iter = del_vec.begin(); iter != del_vec.end(); ++iter)
			{
				VertexID del_vtxID = *iter;
				g.del_node(del_vtxID);
				subg_wid_map.erase(del_vtxID);
			}

			cand_wid_map.clear();
			multimap<AttrValueT, VertexID, greater<AttrValueT> > topK_cands;
			for (VtxIdSet::iterator iter = subG.begin(); iter != subG.end(); ++iter)
			{
				hash_map<VertexID, VertexT*>::iterator vtx_pit = vertex_idx_map.find(*iter);
				if (vtx_pit == vertex_idx_map.end())
				{
					cout << "current VertexID is not in vertexIdxMap(fronter) in function FocusTask::compute contraction" << endl;
					abort();
				}
				VertexT* vtx = vtx_pit->second;
				AdjVtxList& adjlist = vtx->get_adjlist();
				for (AdjVtxIter it = adjlist.begin(); it != adjlist.end(); ++it)
				{
					AttrValueT weight = it->attr.get_attr_vec().front();
					if (subG.find(it->id) == subG.end() && weight > 0) //not in subg && edgeWeight is not illegal
					{
						pair<hash_map<VertexID, int>::iterator, bool> ist_flag = cand_wid_map.insert(make_pair(it->id, it->wid));
						if (ist_flag.second == true)
							topK_cands.insert(make_pair(weight, it->id));
					}
				}

				// BSN<-BSN\Cluster
				if (outlier.find(*iter) != outlier.end())
					outlier.erase(*iter);
			}
			if (topK_cands.size() > CAND_MAX_TIME*subG.size())
			{
				multimap<AttrValueT, VertexID, greater<AttrValueT> >::iterator new_end = topK_cands.begin();
				advance(new_end, CAND_MAX_TIME*subG.size());
				for (multimap<AttrValueT, VertexID, greater<AttrValueT> >::iterator it = new_end; it != topK_cands.end(); ++it)
				{
					hash_map<VertexID, int>::iterator candIter = cand_wid_map.find(it->second);
					if (candIter != cand_wid_map.end())
						cand_wid_map.erase(candIter);
				}
			}
		}
		//------------------------- contraction end -------------------------

		if (!is_sim_weight(cur_weight_phi, context.init_weight_phi, DIFF_RATIO) && context.iter_round < ITER_ROUND_MAX)
		{
			context.init_weight_phi = cur_weight_phi;
			context.iter_round++;
			calc_and_pull_next(subg_wid_map, cand_wid_map);
			//cout<<"task::compute new do-while return"<<endl;
			return true;
		}
		else
		{
			if (g.get_nodes().size() >= MIN_RESULT_SIZE)
			{
				cluster.clear();
				list<NodeT>& nodes = g.get_nodes();
				for (list<NodeT>::iterator iter = nodes.begin(); iter != nodes.end(); ++iter)
					cluster.push_back(iter->id);
			}
			else
			{
				cluster.clear();
				outlier.clear();
			}
			//cout << "task::compute done return" << endl;
			return false;
		}
	}

private:

	bool init_real_comp(vector<VertexT*>& frontier, hash_map<VertexID, int>& subg_wid_map, hash_map<VertexID, int>& cand_wid_map)
	{
		multimap<AttrValueT, VertexID, greater<AttrValueT> > topK_cands;
		for (vector<VertexT*>::iterator iter = frontier.begin(); iter != frontier.end(); ++iter)
		{
			VertexT* vtx = *iter;
			AdjVtxList& adjlist = vtx->get_adjlist();
			for (AdjVtxIter it = adjlist.begin(); it != adjlist.end(); ++it)
			{
				AttrValueT weight = it->attr.get_attr_vec().front();
				if (subg_wid_map.find(it->id) == subg_wid_map.end() && weight > 0) //not in subg && edgeWeight is not illegal
				{
					pair<hash_map<VertexID, int>::iterator, bool> istFlag = cand_wid_map.insert(make_pair(it->id, it->wid));
					if (istFlag.second == true)
						topK_cands.insert(make_pair(weight, it->id));
				}
			}
		}

		if (topK_cands.size() > CAND_MAX_TIME*frontier.size())
		{
			multimap<AttrValueT, VertexID, greater<AttrValueT> >::iterator new_end = topK_cands.begin();
			advance(new_end, CAND_MAX_TIME*frontier.size());
			for (multimap<AttrValueT, VertexID, greater<AttrValueT> >::iterator it = new_end; it != topK_cands.end(); ++it)
			{
				hash_map<VertexID, int>::iterator cand_iter = cand_wid_map.find(it->second);
				if (cand_iter != cand_wid_map.end())
					cand_wid_map.erase(cand_iter);
			}
		}

		calc_and_pull_next(subg_wid_map, cand_wid_map);
		return true;
	}

	AttrValueT get_phi(VtxIdSet& subG, bool use_weight, hash_map<VertexID, VertexT*>& vertex_idx_map)
	{
		double wCut = 0, wVol = 0;
		for (VtxIdSet::iterator iter = subG.begin(); iter != subG.end(); ++iter)
		{
			hash_map<VertexID, VertexT*>::iterator vtx_pit = vertex_idx_map.find(*iter);
			if (vtx_pit == vertex_idx_map.end())
			{
				cout << "current VertexID is not in vertexIdxMap(fronter) in function FocusTask::getPhi, vid=" << *iter << endl;
				abort();
			}
			VertexT* vtx = vtx_pit->second;
			AdjVtxList& adjlist = vtx->get_adjlist();
			for (AdjVtxIter it = adjlist.begin(); it != adjlist.end(); ++it)
			{
				AttrValueT weight = it->attr.get_attr_vec().front();
				if (weight < 0) //edgeWeight is illegal
					continue;

				if (subG.find(it->id) == subG.end())
					use_weight ? (wCut += weight) : (wCut += 1);
				else
				{
					if (vtx->id < it->id)
						use_weight ? (wVol += weight) : (wVol += 1);
				}
			}
		}
		return wVol != 0 ? (wCut / wVol) : -1;
	}

	void calc_and_pull_next(hash_map<VertexID, int>& subg_wid_map, hash_map<VertexID, int>& cand_wid_map)
	{
		for (hash_map<VertexID, int>::iterator iter = subg_wid_map.begin(); iter != subg_wid_map.end(); ++iter)
			this->pull(AdjVertex(iter->first, iter->second));
		for (hash_map<VertexID, int>::iterator iter = cand_wid_map.begin(); iter != cand_wid_map.end(); ++iter)
			this->pull(AdjVertex(iter->first, iter->second));
	}

	void expand_once(VtxIdSet& subG, const AttrValueT cur_weight_phi, const AttrValueT init_struct_phi, AttrValueT& best_weight_diff,
	                VertexID& best_weight_node, VertexID& best_struct_node, hash_map<VertexID, int>& cand_wid_map, hash_map<VertexID, VertexT*>& vertex_idx_map)
	{
		best_weight_node = -1, best_struct_node = -1;
		best_weight_diff = 0;
		AttrValueT best_struct_diff = 0;
		for (hash_map<VertexID, int>::iterator it = cand_wid_map.begin(); it != cand_wid_map.end(); ++it)
		{
			subG.insert(it->first);
			AttrValueT new_weight_phi = get_phi(subG, true, vertex_idx_map);
			AttrValueT new_struct_phi = get_phi(subG, false, vertex_idx_map);
			if (new_weight_phi < 0 || new_struct_phi < 0)
			{
				subG.erase(it->first);
				continue;
			}

			AttrValueT diff_weight_phi = new_weight_phi - cur_weight_phi;
			AttrValueT diff_struct_phi = new_struct_phi - init_struct_phi;
			if (diff_weight_phi < best_weight_diff)
			{
				best_weight_diff = diff_weight_phi;
				best_weight_node = it->first;
			}
			if (diff_struct_phi < best_struct_diff)
			{
				best_struct_diff = diff_struct_phi;
				best_struct_node = it->first;
			}
			subG.erase(it->first);
		}
	}

	void contract(VtxIdSet& subG, AttrValueT& cur_weight_phi, vector<VertexID>& del_vec, hash_map<VertexID, VertexT*>& vertex_idx_map)
	{
		bool removed;
		vector<VertexID> temp_subG(subG.begin(), subG.end());
		do
		{
			removed = false;
			for (vector<VertexID>::iterator iter = temp_subG.begin(); iter != temp_subG.end(); ++iter)
			{
				VertexID cur_vtxID = *iter;
				subG.erase(cur_vtxID);

				AttrValueT new_weight_phi = get_phi(subG, true, vertex_idx_map);
				if (new_weight_phi < 0)
				{
					subG.insert(cur_vtxID);
					continue;
				}

				double diff_weight_phi = new_weight_phi - cur_weight_phi;
				if (diff_weight_phi <= 0)
				{
					removed = true;
					cur_weight_phi += diff_weight_phi;
					del_vec.push_back(cur_vtxID);
				}
				else
				{
					subG.insert(cur_vtxID);
				}
			}
			if (removed == true)
				temp_subG.assign(subG.begin(), subG.end());
		}
		while (removed != false);
	}
};

//-----------------------------------------------------------------

class FocusSlave :public Slave<FocusTask, CountAgg>
{
public:
	virtual FocusTask * create_task(VertexT * v)
	{
		AdjVtxList core_node_cands;
		AdjVertex seed_vtx(v->id, get_worker_id());
		core_node_cands.push_back(seed_vtx);
		AdjVtxList& adjlist = v->get_adjlist();
		for (AdjVtxIter v_iter = adjlist.begin(); v_iter != adjlist.end(); ++v_iter)
		{
			AttrValueT weight = v_iter->attr.get_attr_vec().front();
			if (weight >= MIN_WEIGHT)
				core_node_cands.push_back(*v_iter);
		}
		if (core_node_cands.size() < MIN_CORE_SIZE)
			return NULL;

		FocusTask * task = new FocusTask;
		task->context.comp_round = 0;
		task->context.iter_round = 0;
		hash_map<VertexID, int>& subg_wid_map = task->context.subg_wid_map;
		for (AdjVtxIter v_iter = core_node_cands.begin(); v_iter != core_node_cands.end(); ++v_iter)
		{
			NodeT core_node(v_iter->id);
			task->subG.add_node(core_node);
			subg_wid_map.insert(make_pair(v_iter->id, v_iter->wid));
		}
		task->pull(core_node_cands);
		return task;
	}

	virtual VertexT* to_vertex(char* line)
	{
		//char sampleLine[] = "0 5\t0.1 0.2 0.3 0.4 \t1 (0.1 )1 2 (0.2 )2 ";
		VertexT* v = new VertexT;
		char * pch;
		pch = strtok(line, " ");
		v->id = atoi(pch);
		strtok(NULL, "\t");
		pch = strtok(NULL, "\t"); //0.1 0.2 0.3 0.4

		while ((pch = strtok(NULL, " ")) != NULL)
		{
			AdjVertex item;
			item.id = atoi(pch);
			pch = strtok(NULL, " ");
			item.attr.get_attr_vec().push_back(atof(pch));
			pch = strtok(NULL, " ");
			item.wid = atoi(pch);
			v->adjlist.push_back(std::move(item));
		}

		sort(v->adjlist.begin(), v->adjlist.end());
		return v;
	}
};

class FocusMaster :public Master<CountAgg>
{
public:
	void size_count(map<vector<VertexID>, VtxIdSet>& co_map)
	{
		map<size_t, int> count_map;
		for (map<vector<VertexID>, VtxIdSet>::const_iterator iter = co_map.begin(); iter != co_map.end(); ++iter)
		{
			map<size_t, int>::iterator it = count_map.find(iter->first.size());
			if (it != count_map.end())
				++(it->second);
			else
				count_map.insert(make_pair(iter->first.size(), 1));
		}
		for (map<size_t, int>::iterator iter = count_map.begin(); iter != count_map.end(); ++iter)
			cout << iter->first << "  " << iter->second << endl;
		cout << endl;
	}

	virtual void print_result()
	{
		CountAgg::FinalType* agg = (CountAgg::FinalType*)get_agg();
		map<vector<VertexID>, VtxIdSet>& co_map = agg->co_map;
		stringstream ss;
		for (map<vector<VertexID>, VtxIdSet>::iterator iter = co_map.begin(); iter != co_map.end(); ++iter)
		{
			const vector<VertexID>& cluster = iter->first;
			const VtxIdSet& outlier = iter->second;

			ss << "cluster: ";
			for (vector<VertexID>::const_iterator it = cluster.begin(); it != cluster.end(); ++it)
				ss << *it << " ";
			ss << endl;

			ss << "outlier: ";
			for (VtxIdSet::const_iterator it = outlier.begin(); it != outlier.end(); ++it)
				ss << *it << " ";
			ss << endl;

			ss << endl;
		}
		cout << ss.str();

		size_count(co_map);
	}
};

class FocusWorker :public Worker<FocusMaster, FocusSlave, CountAgg> {};

int main(int argc, char* argv[])
{
	init_worker(&argc, &argv);

	if (argc >= 2)
		MIN_WEIGHT = atof(argv[1]);
	if (argc >= 3)
		MIN_CORE_SIZE = atoi(argv[2]);
	if (argc >= 4)
		MIN_RESULT_SIZE = atoi(argv[3]);
	if (argc >= 5)
		DIFF_RATIO = atof(argv[4]);
	if (argc >= 6)
		ITER_ROUND_MAX = atoi(argv[5]);
	if (argc >= 7)
		CAND_MAX_TIME = atoi(argv[6]);

	WorkerParams param;
	load_system_parameters(param);
	load_hdfs_config();

	FocusWorker worker;
	CountAgg agg;
	FocusResult result;
	worker.set_aggregator(&agg, result);
	worker.run(param);

	worker_finalize();
	return 0;
}
