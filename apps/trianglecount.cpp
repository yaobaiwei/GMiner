//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#include "core/subg-dev.hpp"

using namespace std;


struct TriangleContext
{
	int count;
	VertexID last_id;
};

ibinstream& operator << (ibinstream& m, const TriangleContext& v)
{
	m << v.count;
	m << v.last_id;
	return m;
}

obinstream& operator >> (obinstream& m, TriangleContext& v)
{
	m >> v.count;
	m >> v.last_id;
	return m;
}

ifbinstream& operator << (ifbinstream& m, const TriangleContext& v)
{
	m << v.count;
	m << v.last_id;
	return m;
}

ofbinstream& operator >> (ofbinstream& m, TriangleContext & v)
{
	m >> v.count;
	m >> v.last_id;
	return m;
}


class CountAgg :public Aggregator<TriangleContext, unsigned long long, unsigned long long>
{
public:

	virtual ~CountAgg() {}

	virtual void init()
	{
		count_ = 0;
	}

	virtual void step_partial(TriangleContext&  v)
	{
		count_ += v.count;
	}

	virtual void step_final(unsigned long long* part)
	{
		count_ += *part;
	}

	virtual unsigned long long* finish_partial()
	{
		return &count_;
	}
	virtual unsigned long long* finish_final()
	{
		return &count_;
	}

private:
	unsigned long long count_;
};


class TriangleTask :public Task<VertexID, TriangleContext>
{
public:

	virtual bool compute(SubgraphT & g, ContextType & context, vector<VertexT *> & frontier)
	{
		VertexT last_v;
		last_v.id = context.last_id;
		frontier.push_back(&last_v);
		int size = frontier.size();

		for (int i = 0; i < size - 1; i++)
		{
			AdjVtxList & adj = frontier[i]->get_adjlist();
			int size_i = 0;  //pos in frontier[i]’s adj to search next
			int size_j = adj.size();
			for (int j = i + 1; j < size; j++)
			{
				VertexID id_j = frontier[j]->id;  ////frontier[j]’s ID
				while ((size_i < size_j) && (adj[size_i].id < id_j))
					size_i++;
				if (size_i >= size_j)
					break;
				if (adj[size_i].id == id_j)
				{
					context.count++;
					size_i++;
				}
			}
		}
		return false;
	}
};


class TriangleSlave :public Slave<TriangleTask, CountAgg>
{
public:

	virtual VertexT* respond(VertexT * v)
	{
		KeyT vid = v->id;

		VertexT * tmp = new VertexT;
		tmp->id = vid;

		AdjVtxList & adjlist = v->get_adjlist();
		AdjVtxIter v_iter = adjlist.begin();
		while ((v_iter < adjlist.end()) && (v_iter->id < vid))
			v_iter++;
		tmp->adjlist.insert(tmp->adjlist.end(), v_iter, adjlist.end());

		return tmp;
	}

	virtual TriangleTask * create_task(VertexT * v)
	{

		AdjVtxList & adjlist = v->get_adjlist();
		if (adjlist.size() <= 1)
			return NULL;

		AdjVtxList candidates; //the vertexes to be pulled in the next round
		VertexID vid = v->id;
		AdjVtxIter vIter = adjlist.begin();
		while ((vIter < adjlist.end() - 1) && (vIter->id <= vid))
			vIter++;

		candidates.insert(candidates.end(), vIter, adjlist.end() - 1);
		if (candidates.size() >= 1)
		{
			TriangleTask * task = new TriangleTask;
			task->pull(candidates);
			task->context.count = 0;
			task->context.last_id = adjlist[adjlist.size() - 1].id;

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


class TriangleMaster :public Master<CountAgg>
{
public:

	virtual void print_result()
	{
		unsigned long long* agg = (unsigned long long*)get_agg();
		cout << "The sum of all triangles is " << *agg << endl;
	}
};


class TriangleWorker :public Worker<TriangleMaster, TriangleSlave, CountAgg> {};


int main(int argc, char* argv[])
{
	init_worker(&argc, &argv);

	WorkerParams param;
	load_system_parameters(param);
	load_hdfs_config();

	TriangleWorker worker;
	CountAgg agg;
	worker.set_aggregator(&agg, 0);
	worker.run(param);

	worker_finalize();
	return 0;
}
