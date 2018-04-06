//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef MESSAGEBUFFER_HPP_
#define MESSAGEBUFFER_HPP_

#include <vector>

#include "util/combiner.hpp"
#include "util/communication.hpp"
#include "util/global.hpp"
#include "util/vecs.hpp"

using namespace std;

template <class VertexT>
class MessageBuffer
{
public:
	typedef typename VertexT::KeyType KeyT;
	typedef typename VertexT::MessageType MessageT;
	typedef typename VertexT::HashType HashT;
	typedef vector<MessageT> MessageContainerT;
	typedef hash_map<KeyT, int> Map; //int = position in v_msg_bufs //CHANGED FOR VADD
	typedef Vecs<KeyT, MessageT, HashT> VecsT;
	typedef typename VecsT::Vec Vec;
	typedef typename VecsT::VecGroup VecGroup;
	typedef typename Map::iterator MapIter;

	void init(vector<VertexT*> & vertexes);
	void reinit(vector<VertexT*> & vertexes);

	//*** FT-change: newly added to emptize the msgbuf a normal process (respawned process just call init())
	void clear_msgs();
	void add_message(const KeyT& id, const MessageT& msg);
	Map& get_messages();
	void sync_messages();

	void combine();
	long long get_total_msg();
	vector<MessageContainerT>& get_v_msg_bufs();

private:
	VecsT out_messages_;
	Map in_messages_;
	vector<MessageContainerT> v_msg_bufs_;
	HashT hash_;
};

#include "messagebuffer.tpp"

#endif /* MESSAGEBUFFER_HPP_ */
