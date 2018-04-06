//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


template <class VertexT>
void MessageBuffer<VertexT>::init(vector<VertexT*> & vertexes)
{
	v_msg_bufs_.resize(vertexes.size());
	for (int i = 0; i < vertexes.size(); i++)
	{
		VertexT* v = vertexes[i];
		in_messages_[v->id] = i; //CHANGED FOR VADD
	}
}

template <class VertexT>
void MessageBuffer<VertexT>::reinit(vector<VertexT*> & vertexes)
{
	v_msg_bufs_.resize(vertexes.size());
	in_messages_.clear();
	for (int i = 0; i < vertexes.size(); i++)
	{
		VertexT* v = vertexes[i];
		in_messages_[v->id] = i; //CHANGED FOR VADD
	}
}

//*** FT-change: newly added to emptize the msgbuf a normal process (respawned process just call init())
template <class VertexT>
void MessageBuffer<VertexT>::clear_msgs()
{
	out_messages_.clear();
	for(int i=0; i<v_msg_bufs_.size(); i++) v_msg_bufs_[i].clear();
}

template <class VertexT>
void MessageBuffer<VertexT>::add_message(const MessageBuffer::KeyT& id, const MessageBuffer::MessageT& msg)
{
	has_msg(); //cannot end yet even every vertex halts
	out_messages_.append(id, msg);
}

template <class VertexT>
typename MessageBuffer<VertexT>::Map& MessageBuffer<VertexT>::get_messages()
{
	return in_messages_;
}

template <class VertexT>
void MessageBuffer<VertexT>::sync_messages()
{
	int np = get_num_workers();
	int me = get_worker_id();
	//exchange msgs
	all_to_all(out_messages_.get_bufs());
	// gather all messages
	for (int i = 0; i < np; i++)
	{
		Vec& msgBuf = out_messages_.get_buf(i);
		for (int j = 0; j < msgBuf.size(); j++)
		{
			MapIter it = in_messages_.find(msgBuf[j].key);
			if (it != in_messages_.end()) //filter out msgs to non-existent vertices
				v_msg_bufs_[it->second].push_back(msgBuf[j].msg); //CHANGED FOR VADD
		}
	}
	//clear out-msg-buf
	out_messages_.clear();
}

template <class VertexT>
void MessageBuffer<VertexT>::combine()
{
	//apply combiner
	Combiner<MessageT>* combiner = (Combiner<MessageT>*)get_combiner();
	if (combiner != NULL)
		out_messages_.combine();
}

template <class VertexT>
long long MessageBuffer<VertexT>::get_total_msg()
{
	return out_messages_.get_total_msg();
}

template <class VertexT>
vector<typename MessageBuffer<VertexT>::MessageContainerT>& MessageBuffer<VertexT>::get_v_msg_bufs()
{
	return v_msg_bufs_;
}
