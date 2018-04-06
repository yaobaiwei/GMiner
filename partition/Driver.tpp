//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class BVertexT>
void Driver<BVertexT>::load_graph(const char* inpath)
{
	hdfsFS fs = get_hdfs_fs();
	hdfsFile in = get_r_handle(inpath, fs);
	LineReader reader(fs, in);
	while (true)
	{
		reader.read_line();
		if (!reader.eof())
			load_vertex(to_vertex(reader.get_line()));
		else
			break;
	}
	hdfsCloseFile(fs, in);
	hdfsDisconnect(fs);
}


template <class BVertexT>
void Driver<BVertexT>::dump_partition(const char* outpath)
{
	//only writes to one file "part_machineID"
	hdfsFS fs = get_hdfs_fs();
	BufferedWriter* writer = new BufferedWriter(outpath, fs);

	for (VertexIter it = vertexes_.begin(); it != vertexes_.end(); it++)
	{
		writer->check();
		to_line(*it, *writer);
	}

	delete writer;
	hdfsDisconnect(fs);
}


template <class BVertexT>
Driver<BVertexT>::Driver() { }

template <class BVertexT>
Driver<BVertexT>::~Driver()
{
	for (size_t i = 0; i < vertexes_.size(); i++)
		delete vertexes_[i];
}
