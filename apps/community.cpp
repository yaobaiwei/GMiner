//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#define USE_ATTRIBUTE

#include "core/subg-dev.hpp"

using namespace std;


typedef map<VertexID, set<VertexID>> Graph;
typedef set<vector<VertexID> > QSet;
typedef string AttrValueT;
typedef set<AttrValueT> ComAttrSet;


int K_THRESHOLD = 3;  // threshold that only store the result communities with size >= K_THRESHOLD


struct CommunityContext
{
	int max_size;
	vector<VertexID> attrQ;
	ComAttrSet com_attr_set;

	CommunityContext(): max_size(0) {}
};

ibinstream& operator<<(ibinstream& m, const CommunityContext& v)
{
	m << v.max_size;
	m << v.attrQ;
	m << v.com_attr_set;
	return m;
}

obinstream& operator >> (obinstream& m, CommunityContext& v)
{
	m >> v.max_size;
	m >> v.attrQ;
	m >> v.com_attr_set;
	return m;
}

ifbinstream& operator<<(ifbinstream& m, const CommunityContext& v)
{
	m << v.max_size;
	m << v.attrQ;
	m << v.com_attr_set;
	return m;
}

ofbinstream& operator >> (ofbinstream& m, CommunityContext & v)
{
	m >> v.max_size;
	m >> v.attrQ;
	m >> v.com_attr_set;
	return m;
}


struct Result
{
	int max_size;
	QSet q_set;
	Result(): max_size(0) {}
};

ibinstream& operator << (ibinstream& m, const Result& v)
{
	m << v.max_size;
	m << v.q_set;
	return m;
}

obinstream& operator >> (obinstream& m, Result& v)
{
	m >> v.max_size;
	m >> v.q_set;
	return m;
}

ifbinstream& operator << (ifbinstream& m, const Result& v)
{
	m << v.max_size;
	m << v.q_set;
	return m;
}

ofbinstream& operator >> (ofbinstream& m, Result & v)
{
	m >> v.max_size;
	m >> v.q_set;
	return m;
}


class CountAgg :public Aggregator<CommunityContext, Result, Result>
{
public:
	virtual ~CountAgg() {}

	virtual void init() {}

	virtual void step_partial(CommunityContext& v)
	{
		if (v.max_size > count_.max_size)
			count_.max_size = v.max_size;
		if (!v.attrQ.empty())
			count_.q_set.insert(v.attrQ);
	}

	virtual void step_final(Result* part)
	{
		if (part->max_size > count_.max_size)
			count_.max_size = part->max_size;
		count_.q_set.insert(part->q_set.begin(), part->q_set.end());
	}

	virtual Result* finish_partial()
	{
		return &count_;
	}
	virtual Result* finish_final()
	{
		return &count_;
	}

private:
	Result count_;
};

class CommunityTask :public Task<VertexID, CommunityContext, Attribute<string> >
{
public:
	typedef hash_map<VertexID, VertexT*> VertexIdxMap;

	struct VtxDegree //VertexID with its degree
	{
		VertexID vid;
		int degree;

		inline bool operator<(const VtxDegree& rhs) const
		{
			return degree > rhs.degree;
		}
	};

	/*
	* Output: bool
	* compute whether a Vertex p has some common attribute(s) with comAttrSet or not
	*/
	bool has_common_attr(VertexID p, VertexIdxMap& vertex_idx_map, ComAttrSet& com_attr_set)
	{
		VertexIdxMap::iterator iter = vertex_idx_map.find(p);
		if (iter == vertex_idx_map.end())
		{
			cout << "current VertexID p is not in vertexIdxMap(fronter) in function CliqueTask::compCommonAttr" << endl;
			abort();
		}
		if (com_attr_set.empty())
		{
			cout << "task.context.comAttrSet was not inited before in function CliqueTask::compCommonAttr" << endl;
			abort();
		}

		VertexT* vtx = iter->second;
		VertexT::AttrT& attr = vtx->attr;
		const vector<AttrValueT>& attr_vec = attr.get_attr_vec();

		bool has_common = false;
		ComAttrSet common;
		std::set_intersection(com_attr_set.begin(), com_attr_set.end(), attr_vec.begin(), attr_vec.end(), inserter(common, common.begin()));
		if (!common.empty())
		{
			com_attr_set.swap(common);
			has_common = true;
		}

		return has_common;
	}

	/*
	* Output: ListR
	* sort the vertexes based on their degrees in descending order
	*/
	void get_listR(Graph & g, vector<VertexID> & listR)
	{
		Graph::iterator g_iter;
		vector<VtxDegree> vtx_vec;
		for (g_iter = g.begin(); g_iter != g.end(); g_iter++)
		{
			VtxDegree item;
			item.vid = g_iter->first;
			item.degree = g_iter->second.size();
			vtx_vec.push_back(item);
		}
		sort(vtx_vec.begin(), vtx_vec.end());
		for (int i = 0; i < vtx_vec.size(); i++)
		{
			listR.push_back(vtx_vec[i].vid);
		}
	}

	/*
	* Output color, update the ListR based on color
	*/
	void color_sort(Graph & g, vector<VertexID> & listR, map<VertexID, int> & color)
	{
		vector<set<VertexID>> CG;

		for (int i = 0; i < listR.size(); i++)
		{
			VertexID p = listR[i];
			bool match = false; //is matched in current CGs or not
			for (int j = 0; j < CG.size(); j++)
			{
				set<VertexID> & C = CG[j];
				set<VertexID> inter_vtx;
				set_intersection(C.begin(), C.end(), g[p].begin(), g[p].end(), inserter(inter_vtx, inter_vtx.begin()));
				if (inter_vtx.empty()) //if the intersection of C & p.adjList is empty, add the p into C
				{
					C.insert(p);
					match = true;
					break;
				}
			}
			if (!match) //add a new C into CG if p is mismatched with current all Colors
			{
				set<VertexID> newC;
				newC.insert(p);
				CG.push_back(newC);
			}
		}

		listR.clear();  // listR is sorted by increasing order of color value
		set<VertexID>::iterator c_iter;
		for (int i = 0; i < CG.size(); i++)
		{
			for (c_iter = CG[i].begin(); c_iter != CG[i].end(); c_iter++)
			{
				listR.push_back(*c_iter);
				color[*c_iter] = i + 1;
			}
		}
	}

	void community(Graph & g, vector<VertexID> & listR, map<VertexID, int> & color, vector<VertexID> & Q, \
	               int & max_size, vector<VertexID> & attrQ, const size_t K_THRESHOLD, VertexIdxMap & vertex_idx_map, ComAttrSet & com_attr_set)
	{
		while (!listR.empty())
		{
			VertexID p = listR.back();
			listR.pop_back();
			if (Q.size() + color[p] > max_size)
			{
				Q.push_back(p);
				set<VertexID> & adjlist = g[p];
				vector<VertexID> tmp = listR;
				sort(tmp.begin(), tmp.end()); //the ListR should be sorted by VertexID before intersecting, requested by std::set_intersection
				set<VertexID> inter_vtx;
				set_intersection(tmp.begin(), tmp.end(), adjlist.begin(), adjlist.end(), inserter(inter_vtx, inter_vtx.begin()));

				bool has_common = has_common_attr(p, vertex_idx_map, com_attr_set);
				bool ept = inter_vtx.empty();
				if (!ept && has_common)
				{
					map<VertexID, set<VertexID>> subg;
					set<VertexID>::iterator g_iter;
					for (g_iter = inter_vtx.begin(); g_iter != inter_vtx.end(); g_iter++)
					{
						set<VertexID> & adjlist = g[*g_iter];
						set<VertexID> & sub_adj = subg[*g_iter];
						set_intersection(inter_vtx.begin(), inter_vtx.end(), adjlist.begin(), adjlist.end(), inserter(sub_adj, sub_adj.begin()));
					}
					vector<VertexID> listR;
					map<VertexID, int> color;

					get_listR(subg, listR);
					color_sort(subg, listR, color);
					community(subg, listR, color, Q, max_size, attrQ, K_THRESHOLD, vertex_idx_map, com_attr_set);
				}
				else
				{
					if (Q.size() > max_size)
					{
						max_size = Q.size();

						bool case1 = (!has_common && (Q.size() >= K_THRESHOLD + 1));
						bool case2 = (has_common && (Q.size() >= K_THRESHOLD));
						if (case1 || case2)
						{
							vector<VertexID> temp(Q);
							if (!has_common)
								temp.pop_back();
							sort(temp.begin(), temp.end());
							attrQ.swap(temp);
						}
					}
					else if (Q.size() < max_size)
					{
						++max_size;
					}
				}
				Q.pop_back();
			}
			else
			{
				return;
			}
		}
	}

	virtual bool compute(SubgraphT & g, ContextType & context, vector<VertexT *> & frontier)
	{
		int max_size = 0;
		ComAttrSet& com_attr_set = this->context.com_attr_set;
		vector<VertexID>& attrQ = this->context.attrQ;

		vector<VertexID> Q;
		Q.push_back(g.get_nodes().front().id);
		int size = frontier.size();

		if (size != 0)
		{
			set<VertexID> subg;
			for (int i = 0; i < size; i++)
			{
				VertexID id = frontier[i]->id;
				subg.insert(id);
			}

			Graph tempG;  //get the subgraph and their adjLists based on frontier, which is the adjList of seed point
			for (int i = 0; i < size; i++)
			{
				AdjVtxList & adjlist = frontier[i]->get_adjlist();
				vector<VertexID> tmp_adjlist;
				for (int k = 0; k < adjlist.size(); k++)
					tmp_adjlist.push_back(adjlist[k].id);

				set<VertexID> & sub_adj = tempG[frontier[i]->id];
				set_intersection(tmp_adjlist.begin(), tmp_adjlist.end(), subg.begin(), subg.end(), inserter(sub_adj, sub_adj.begin()));
			}

			vector<VertexID> listR;
			map<VertexID, int> color;
			VertexIdxMap vertex_idx_map; // build a vertexIdxMap for attribute simularity computing between two vertexes
			for (vector<VertexT *>::iterator iter = frontier.begin(); iter != frontier.end(); ++iter)
				vertex_idx_map.insert(make_pair((*iter)->id, (*iter)));

			get_listR(tempG, listR);
			color_sort(tempG, listR, color);
			community(tempG, listR, color, Q, max_size, attrQ, K_THRESHOLD, vertex_idx_map, com_attr_set); //pass the map
		}
		else if (Q.size() > max_size)
		{
			max_size = Q.size();
		}

		if (this->context.attrQ.size() > this->context.max_size)
			this->context.max_size = this->context.attrQ.size();

		return false;
	}
};

//-----------------------------------------------------------------

class CommunitySlave :public Slave<CommunityTask, CountAgg>
{
public:
	virtual CommunityTask * create_task(VertexT * v)
	{
		AdjVtxList & adjlist = v->get_adjlist();
		if (adjlist.size() <= 1)
			return NULL;

		AdjVtxList candidates; //the vertexes to be pulled in the next round
		VertexID vid = v->id;
		AdjVtxIter v_iter = adjlist.begin();
		while ((v_iter < adjlist.end()) && (v_iter->id <= vid))
			v_iter++;
		candidates.insert(candidates.end(), v_iter, adjlist.end());

		const vector<AttrValueT>& attr_vec = v->attr.get_attr_vec();
		if ((candidates.size() >= K_THRESHOLD - 1) && (!attr_vec.empty()))
		{
			CommunityTask * task = new CommunityTask;
			task->pull(candidates);
			NodeT node;
			v->set_node(node);
			task->subG.add_node(node);
			task->context.max_size = 0;

			//initialization with the whole attributes of the first vertex
			task->context.com_attr_set.insert(attr_vec.begin(), attr_vec.end());

			return task;
		}
		return NULL;
	}

	virtual VertexT* to_vertex(char* line)
	{
		//char sampleLine[] = "0 5\t1983 166 1 ball;tennis \t1 1 2 2 ";
		VertexT* v = new VertexT;
		char * pch;
		pch = strtok(line, " ");
		v->id = atoi(pch);
		strtok(NULL, "\t");

		vector<AttrValueT> a;
		pch = strtok(NULL, "\t");
		char* attr_line = new char[strlen(pch) + 1];
		strcpy(attr_line, pch);
		attr_line[strlen(pch)] = '\0';

		while ((pch = strtok(NULL, " ")) != NULL)
		{
			AdjVertex item;
			item.id = atoi(pch);
			pch = strtok(NULL, " ");
			item.wid = atoi(pch);
			v->adjlist.push_back(item);
		}

		//pch = strtok(attr_line, " ");
		//pch = strtok(NULL, " ");
		//pch = strtok(NULL, " ");
		//pch = strtok(NULL, "; ");

		pch = strtok(attr_line, " ");

		if (strcmp(pch, "0") != 0) a.push_back(pch);
		while ((pch = strtok(NULL, "; ")) != NULL)
			if (strcmp(pch, "0") != 0) a.push_back(pch);
		sort(a.begin(), a.end());
		vector<AttrValueT>::iterator unique_iter = unique(a.begin(), a.end());
		a.resize(distance(a.begin(), unique_iter));
		v->attr.init_attr_vec(a);

		sort(v->adjlist.begin(), v->adjlist.end());
		delete[] attr_line;
		return v;
	}
};

class CommunityMaster :public Master<CountAgg>
{
public:
	virtual void print_result()
	{
		CountAgg::FinalType* agg = (CountAgg::FinalType*)get_agg();
		stringstream ss;
		ss << "The size of max attr community is " << agg->max_size << endl;
		QSet& q_set = agg->q_set;
		ss << "The size of communities containing at least " << K_THRESHOLD << " members is " << q_set.size() << endl;
		for (QSet::const_iterator iter = q_set.begin(); iter != q_set.end(); ++iter)
		{
			ss << "Attr Community: ";
			const vector<VertexID>& Q = (*iter);
			for (vector<VertexID>::const_iterator i = Q.begin(); i != Q.end(); ++i)
				ss << *i << " ";
			ss << endl;
		}
		cout << ss.str();
	}
};

class CommunityWorker :public Worker<CommunityMaster, CommunitySlave, CountAgg> {};


int main(int argc, char* argv[])
{
	init_worker(&argc, &argv);

	if (argc >= 2)
		K_THRESHOLD = atoi(argv[1]);

	WorkerParams param;
	load_system_parameters(param);
	load_hdfs_config();

	CommunityWorker worker;
	CountAgg agg;
	Result result;
	worker.set_aggregator(&agg, result);
	worker.run(param);

	worker_finalize();
	return 0;
}
