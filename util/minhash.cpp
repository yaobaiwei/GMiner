//Copyright 2018 Husky Data Lab, CUHK
//Authors: Hongzhi Chen, Miao Liu


#include "minhash.hpp"

Minhash::Minhash() {}

void Minhash::init(int k, int V)
{
	sign_size_ = k;
	srand( (unsigned)time( NULL ) );
	for (int i = 0; i < sign_size_; i++)
	{
		a_.push_back( rand() % V);
		b_.push_back( rand() % V);
	}

	if (V % 2 == 0)
		p_ = V + 1;
	else
		p_ = V;

	for (; p_ < 2 * V; p_ += 2) // the prime should exsit from [numVer, 2*numVer]
		if (miller(p_, NUM_TRAILS))
			break;

}

signT Minhash::operator()(vector<VertexID> & to_request)
{
	signT sign;
	for(int i = 0; i < sign_size_; i++)
	{
		VertexID min = INT_MAX;
		for(int j = 0; j < to_request.size(); j++)
		{
			VertexID tmp = ( a_[i] * (to_request[j]) + b_[i] ) % p_;
			if(min > tmp)
				min = tmp;
		}
		sign.push_back(min);
	}
	return sign;
}

// This function calculates (ab)%c
int Minhash::modulo(int a, int b, int c)
{
	long long x=1,y=a; // long long is taken to avoid overflow of intermediate results
	while(b > 0)
	{
		if(b%2 == 1)
		{
			x=(x*y)%c;
		}
		y = (y*y)%c; // squaring the base
		b /= 2;
	}
	return x%c;
}

// this function calculates (a*b)%c taking into account that a*b might overflow
long long Minhash::mulmod(long long a,long long b,long long c)
{
	long long x = 0,y=a%c;
	while(b > 0)
	{
		if(b%2 == 1)
		{
			x = (x+y)%c;
		}
		y = (y*2)%c;
		b /= 2;
	}
	return x%c;
}

// Miller-Rabin primality test, iteration signifies the accuracy of the test
// K*lgN
// URL: https://www.topcoder.com/community/data-science/data-science-tutorials/primality-testing-non-deterministic-algorithms/
bool Minhash::miller(long long N,int K)
{
	if(N<2)
	{
		return false;
	}
	if(N !=2 && N %2==0)
	{
		return false;
	}
	long long s = N-1;
	while(s%2==0)
	{
		s/=2;
	}
	for(int i=0; i<K; i++)
	{
		long long a=rand()%(N-1)+1,temp=s;
		long long mod=modulo(a,temp, N);
		while(temp!= N -1 && mod!=1 && mod  != N -1)
		{
			mod=mulmod(mod,mod, N);
			temp *= 2;
		}
		if(mod != N-1 && temp % 2==0 )
		{
			return false;
		}
	}
	return true;
}
