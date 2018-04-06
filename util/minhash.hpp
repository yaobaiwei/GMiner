//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#ifndef MINHASH_HPP_
#define MINHASH_HPP_

#include <limits.h>
#include <math.h>
#include <vector>

#include "util/global.hpp"

class Minhash
{
public:
	Minhash();
	void init(int k, int V);
	signT operator()(vector<VertexID> & to_request);

private:
	static const int NUM_TRAILS = 30;

	// This function calculates (ab)%c
	int modulo(int a,int b,int c);

	// this function calculates (a*b)%c taking into account that a*b might overflow
	long long mulmod(long long a,long long b,long long c);

	// Miller-Rabin primality test, iteration signifies the accuracy of the test
	// K*lgN
	// URL: https://www.topcoder.com/community/data-science/data-science-tutorials/primality-testing-non-deterministic-algorithms/
	bool miller(long long N,int K);

	int sign_size_;
	VertexID  p_;
	vector<unsigned long long> a_;
	vector<unsigned long long> b_;
};


#endif /* MINHASH_HPP_ */
