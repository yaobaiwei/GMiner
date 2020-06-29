//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#include "core/subg-dev.hpp"

using namespace std;


typedef map<VertexID, set<VertexID>> Graph;


class CountAgg :public Aggregator<int, int, int>  //Context = int
{
public:

	virtual ~CountAgg() {}

	virtual void init()
	{
		count_ = 0;
	}

	virtual void step_partial(int & v)
	{
		if (v > count_)
			count_ = v;
	}

	virtual void step_final(int* part)
	{
		if (*part > count_)
			count_ = *part;
	}

	virtual int* finish_partial()
	{
		return &count_;
	}
	virtual int* finish_final()
	{
		return &count_;
	}

private:
	int count_;
};

class CliqueTask :public Task<VertexID, int>   //Context = int
{
public:
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
	* Output: ListR
	* sort the vertexes based on their degrees in descending order
	*/
	void get_listR(Graph g, vector<VertexID> & listR)
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
	*
	*/
	//refer: An efficient branch-and-bound algorithm for finding a maximum clique
	//Link:  http://dl.acm.org/citation.cfm?id=1783736
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

		listR.clear();  // LIstR is sorted by increasing order of color value
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

	/*
	* Input :
	* Graph g: the subgraph calculated at current stage
	* ListR: the list of vertexes in increasing order of color
	* Q: the candidates of maxclique calculated before current stage
	* maxSize: the size of MaxClique mined out currently
	*/
	void max_clique(Graph & g, vector<VertexID> & listR, map<VertexID, int> & color, vector<VertexID> & Q, int & max_size)
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
				sort(tmp.begin(), tmp.end());  //the ListR should be sorted by VertexID before intersecting, requested by std::set_intersection
				set<VertexID> inter_vtx;
				set_intersection(tmp.begin(), tmp.end(), adjlist.begin(), adjlist.end(), inserter(inter_vtx, inter_vtx.begin()));

				if (!inter_vtx.empty())
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
					max_clique(subg, listR, color, Q, max_size);
				}
				else if (Q.size() > max_size)
				{
					max_size = Q.size();
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
		int  max_size = *(int*)get_agg();
		vector<VertexID> Q;
		Q.push_back(g.get_nodes().front().id);
		int size = frontier.size();
		if (size != 0)
		{
			set<VertexID> subg;
			for (int i = 0; i < size; i++)
				subg.insert(frontier[i]->id);

			map<VertexID, set<VertexID>> tempG; //get the subgraph and their adjLists based on frontier, which is the adjList of seed point
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

			get_listR(tempG, listR);
			color_sort(tempG, listR, color);
			max_clique(tempG, listR, color, Q, max_size);
		}
		else if (Q.size() > max_size)
		{
			max_size = Q.size();
		}
		//get the maxClique in current task
		context = max_size;

		return false;
	}
};

//-----------------------------------------------------------
class CliqueSlave :public Slave<CliqueTask, CountAgg>
{
public:

	virtual CliqueTask* create_task(VertexT * v)
	{
		AdjVtxList & adjlist = v->get_adjlist();
		AdjVtxList candidates; //the vertexes to be pulled in the next round
		VertexID vid = v->id;
		AdjVtxIter v_iter = adjlist.begin();
		while ((v_iter < adjlist.end()) && (v_iter->id <= vid))
			v_iter++;
		candidates.insert(candidates.end(), v_iter, adjlist.end());

		int max_size = *(int*)get_agg();
		if (candidates.size() >= max_size)
		{
			CliqueTask * task = new CliqueTask;
			task->pull(candidates);
			NodeT node;
			v->set_node(node);
			task->subG.add_node(node);
			task->context = 0;

			return task;
		}

		return NULL;
	}

	virtual VertexT* to_vertex(char* line)
	{
		VertexT* v = new VertexT;
		char * pch;
		pch = strtok(line, " ");
		v->id = atoi(pch);
		strtok(NULL, "\t");
		while ((pch = strtok(NULL, " ")) != NULL)
		{
			AdjVertex item;
			item.id = atoi(pch);
			pch = strtok(NULL, " ");
			item.wid = atoi(pch);
			v->adjlist.push_back(item);
		}
		sort(v->adjlist.begin(), v->adjlist.end());

		return v;
	}
};

class CliqueMaster :public Master<CountAgg>
{
public:
	virtual void print_result()
	{
		int* agg = (int*)get_agg();
		cout << "The size of max clique is " << *agg << endl;
	}
};

class CliqueWorker :public Worker<CliqueMaster, CliqueSlave, CountAgg> {};

int main(int argc, char* argv[])
{
	init_worker(&argc, &argv);

	WorkerParams param;
	load_system_parameters(param);
	load_hdfs_config();

	CliqueWorker worker;
	CountAgg agg;
	worker.set_aggregator(&agg, 0);
	worker.run(param);

	worker_finalize();
	return 0;
}
