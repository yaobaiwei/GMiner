//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef COMBINER_HPP_
#define COMBINER_HPP_

template <class MessageT>
class Combiner
{
public:
	virtual void combine(MessageT& old, const MessageT& new_msg) = 0;
};

#endif /* COMBINER_HPP_ */
