#ifndef _MICRO_BEHCN_H
#define _MICRO_BEHCN_H

#include "util.h"
#include "config.h"
#include "index.h"
#include <utility>
#include <assert.h>

enum OperationType{
	INSERT,
	REMOVE,
	UPDATE,
	GET,
	SCAN,
	MIXED,
	_OpreationTypeNumber
};


template<typename T>
inline void swap(T&a, T&b){
	T tmp = a;
	a = b;
	b = tmp;
}

long long* random_shuffle(int s){
	long long* x = new long long[s];
	for(int i=0; i<s; i++){
		x[i] = i;
	}
	for(int i=0; i<s; i++){
		swap(x[i], x[random()%s]);
	}
	return x;
}

class Benchmark{
public:
	WorkloadGenerator* workload;
	long long key_range;
	long long init_key;
	long long *x;
	Config _conf;

	Benchmark(Config& conf):init_key(0), _conf(conf){
		if (conf.workload == RANDOM){
			workload = new RandomGenerator();
		}else{
			workload = new ZipfWrapper(conf.skewness, conf.init_keys);
		}

		x = NULL;
	}
	virtual ~Benchmark(){
	    if (x  != NULL)
    	    delete[] x;
	}

	virtual std::pair<OperationType, long long> nextOperation(){
		return std::make_pair(INSERT, workload->Next());
	}

	virtual long long nextInitKey(){
	    if (x == NULL)
            x = random_shuffle(_conf.init_keys);
		return x[init_key++ % _conf.init_keys];
		//return init_key++ % _conf.init_keys;
	}
};

class ReadOnlyBench:public Benchmark{
public:
	ReadOnlyBench(Config& conf): Benchmark(conf){
	}
	std::pair<OperationType, long long> nextOperation(){
		long long d = workload->Next();
		return std::make_pair(GET, d % _conf.init_keys);
	}
};

class InsertOnlyBench:public Benchmark{
	RandomGenerator* salt;
public:
	InsertOnlyBench(Config& conf): Benchmark(conf){
		salt = new RandomGenerator();
	}
	std::pair<OperationType, long long> nextOperation(){
		long long d = workload->Next() % _conf.init_keys;
		long long x = salt->Next() % 128;
		return std::make_pair(INSERT, d*x);
	}
	long long nextInitKey(){
		return 128*Benchmark::nextInitKey();
	}
};

class UpdateOnlyBench:public Benchmark{
public:
	UpdateOnlyBench(Config& conf): Benchmark(conf){
	}
	OperationType nextOp(){
		return UPDATE;
	}
	std::pair<OperationType, long long> nextOperation(){
		long long d = workload->Next() % _conf.init_keys;
		return std::make_pair(UPDATE, d);
	}
};

class DeleteOnlyBench:public Benchmark{
public:

	DeleteOnlyBench(Config& conf): Benchmark(conf){
	}
	std::pair<OperationType, long long> nextOperation(){
		long long d = workload->Next() % _conf.init_keys;
		return std::make_pair(REMOVE, d);
	}
};

class MixedBench:public Benchmark{
	RandomGenerator rdm;
	int round;
	long long key;
public:
	MixedBench(Config& conf): Benchmark(conf){
	}
	std::pair<OperationType, long long> nextOperation(){
		std::pair<OperationType, long long> result;
		long long _key = workload->Next()% _conf.init_keys;
		switch (round){
			case 0:
				key = workload->Next() % _conf.init_keys;
				result = std::make_pair(REMOVE, key);
				break;
			case 1:
				result = std::make_pair(INSERT, key);
				break;
			case 2:
				result = std::make_pair(UPDATE, _key);
				break;
			case 3:
				result = std::make_pair(GET, _key);
				break;
			default:
				assert(0);
		}
		round++;
		round %= 4;
		return result;
	}
};

class ScanBench:public Benchmark{
public:
	ScanBench(Config& conf): Benchmark(conf){}

	std::pair<OperationType, long long> nextOperation(){
		long long d = workload->Next() % _conf.init_keys;
		return std::make_pair(SCAN, d);
	}
};

class YSCBA:public Benchmark{
public:
//	readRate = 0.5;
//	writeRate = 0.5;
	int read_ratio = 90;

	RandomGenerator rdm;
	YSCBA(Config& conf): Benchmark(conf){
		read_ratio = conf.read_ratio;
	}
	virtual std::pair<OperationType, long long> nextOperation(){
		int k = rdm.randomInt()%100;
		if (k > read_ratio){
			return std::make_pair(UPDATE, workload->Next() % _conf.init_keys);
		}else{
			return std::make_pair(GET, workload->Next() % _conf.init_keys);
		}
	}
};

class YSCBB:public Benchmark{
public:
//	readRate = 0.95;
//	writeRate = 0.05;
	RandomGenerator rdm;
	YSCBB(Config& conf): Benchmark(conf){
	}
	virtual std::pair<OperationType, long long> nextOperation(){
		int k = rdm.randomInt() % 100;
		if (k < _conf.read_ratio){
			return std::make_pair(GET, workload->Next() % _conf.init_keys);
		}else{
			return std::make_pair(UPDATE, workload->Next() % _conf.init_keys);
		}
	}
};

class YSCBC:public Benchmark{
public:
	YSCBC(Config& conf): Benchmark(conf){
	}
	OperationType nextOp(){
		return GET;
	}
};

class YSCBD:public Benchmark{
public:
	YSCBD(Config& conf): Benchmark(conf){

	}
	OperationType nextOp(){
		return GET;
	}
};

class YSCBE:public Benchmark{
public:
	YSCBE(Config& conf): Benchmark(conf){

	}
	OperationType nextOp(){
		return GET;
	}
};

#endif
