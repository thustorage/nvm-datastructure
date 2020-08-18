#ifndef index_h
#define index_h

#include "util.h"
#include "timer.h"
#include <iostream>
template<typename K, typename V, int size>
class Index{
public:
	virtual ~Index(){}
	virtual bool insert(K a, V b) = 0;
	virtual bool remove(K a) = 0;
	virtual V get(K v, double t[3]) = 0;
	virtual bool update(K a, V b, double t[3]) = 0;
	virtual void scan(K key, bool (*function)(K key, V value)) = 0;
	virtual void rebuild(){ }
	bool time_on;
};

//#define PERF_LATENCY

#endif
