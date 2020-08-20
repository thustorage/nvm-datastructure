#ifndef _BENCHMARKS_H_
#define _BENCHMARKS_H_

#include "microbench.h"
#include "config.h"

static Benchmark* getBenchmark(Config conf){
	switch (conf.benchmark){
		case READ_ONLY:
			return new ReadOnlyBench(conf);
		case INSERT_ONLY:
			return new InsertOnlyBench(conf);
		case UPDATE_ONLY:
			return new UpdateOnlyBench(conf);
		case DELETE_ONLY:
			return new DeleteOnlyBench(conf);
		case MIXED_BENCH:
			return new MixedBench(conf);
		case YCSB_A:
			return new YSCBA(conf);
		case YCSB_B:
			return new YSCBB(conf);
		case YCSB_C:
			return new YSCBC(conf);
		case YCSB_D:
			return new YSCBD(conf);
		case YCSB_E:
			return new YSCBE(conf);
		case SCAN_BENCH:
			return new ScanBench(conf);
		case RECOVERY_BENCH:
			return new UpdateOnlyBench(conf);
		default:
			printf("none support benchmark %d\n", conf.benchmark);
			exit(0);
	}
	return NULL;
}
#endif
