#ifndef coordinator_h
#define coordinator_h

#include "config.h"
#include "nv_tree.h"
#include "rtm_tree.h"
#include "rtm_tree_r.h"
#include "fptree.h"
#include "wbtree.h"
#include "wbtree2.h"
#include "util.h"
#include "benchmarks.h"
#include <thread>
#include <boost/thread/barrier.hpp>
#include <unistd.h>
#include "index_factory.h"
#include "nvm_mgr.h"

namespace nvindex{

template<typename K, typename V, int size>
class Coordinator{
	class Result{
	public:
		double throughput;
		double update_latency_breaks[3];
		double find_latency_breaks[3];

		void operator +=(Result& r){
			this->throughput += r.throughput;
			for(int i=0; i<3; i++){
				this->update_latency_breaks[i] += r.update_latency_breaks[i];
				this->find_latency_breaks[i] += r.find_latency_breaks[i];
			}
		}
		void operator /= (double r){
			this->throughput /= r;
			for(int i=0; i<3; i++){
				this->update_latency_breaks[i] /= r;
				this->find_latency_breaks[i] /= r;
			}
		}
	};
public:
	Coordinator(Config _conf):conf(_conf){
	}
	int stick_this_thread_to_core(int core_id) {
		int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
   		if (core_id < 0 || core_id >= num_cores)
      			return EINVAL;

   		cpu_set_t cpuset;
   		CPU_ZERO(&cpuset);
   		CPU_SET(core_id, &cpuset);

   		pthread_t current_thread = pthread_self();
   		return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
	}
	void worker(Index<K, V, size>* btree, int workerid, Result* result, Benchmark* b){
		Benchmark* benchmark = getBenchmark(conf);
		printf("[WORKER]\thello, I am worker %d\n", workerid);
		register_threadinfo();
		stick_this_thread_to_core(workerid);
		bar->wait();

		unsigned long tx = 0;

		double total_update_latency_breaks[3] = {0.0, 0.0, 0.0};
		double update_latency_breaks[3] = {0.0, 0.0, 0.0};
		int update_count = 0;

		double total_find_latency_breaks[3] = {0.0, 0.0, 0.0};
		double find_latency_breaks[3] = {0.0, 0.0, 0.0};
		int find_count = 0;

		memset(total_update_latency_breaks, 0, sizeof(double)*3);
		memset(total_find_latency_breaks, 0, sizeof(double)*3);

		#define sync_latency(a, b, count) do{ \
		  if (b[0] > 0.0 || (count > 100) && (b[0] > 10*a[0]/count)){ \
		  	for(int i=0; i<3; i++){ a[i] += b[i]; }  \
		  	count++; \
		  }}while(0)

		static int scan_values = 0;
		static int scan_length;
		scan_length = conf.scan_length;

		auto scan_func = [](K key, V value)->bool{
			scan_values++;

			if (scan_values > scan_length){
				return true;
			}
			return false;
		};

		int frequency = conf.throughput / conf.num_threads;
		int submit_time = 1000000000.0/frequency;
		while(done == 0){
			volatile auto next_operation = benchmark->nextOperation();

			OperationType op =  next_operation.first;
			long long d = next_operation.second;
			V result = 1;

			cpuCycleTimer t;
			if (conf.latency_test){
				t.start();
			}

			switch (op){
				case INSERT:
					result = btree->insert(d, d);
					break;
				case REMOVE:
					btree->remove(d);
					break;
				case UPDATE:
					result = btree->update(d, d, update_latency_breaks);
					if(tx % 100 == 0){
						sync_latency(total_update_latency_breaks, update_latency_breaks, update_count);
					}
					break;
				case GET:
					result = btree->get(d, find_latency_breaks);
					if(tx % 100 == 0){
						sync_latency(total_find_latency_breaks, find_latency_breaks, find_count);
					}
					break;
				case SCAN:
					btree->scan(d, scan_func);
					scan_values = 0;
					break;
				default:
					printf("not support such operation: %d\n", op);
					exit(-1);
			}
			if (conf.latency_test){
				t.end();
				while (t.duration() < submit_time){
					t.start();
					t.end();
				}
			}

			tx++;
		}
		result->throughput = tx;

	#ifdef PERF_LATENCY
		for (int i=0; i<3; i++){
			result->update_latency_breaks[i] = total_update_latency_breaks[i] / update_count;
			result->find_latency_breaks[i] = total_find_latency_breaks[i] / find_count;
			printf("%d result %lf %lf\n", workerid, total_update_latency_breaks[i], total_find_latency_breaks[i]);
		}
	#endif  // PERF_LATENCY

		unregister_threadinfo();

		printf("[WORKER]\tworker %d finished\n", workerid);
	}

	void run(){
		printf("[COORDINATOR]\tStart benchmark..\n");

		Index<K,V,size>* btree = getIndex<K, V, size>(conf.type);

		register_threadinfo();
		Benchmark* benchmark = getBenchmark(conf);

		for(unsigned long i=0; i<conf.init_keys; i++){
			long key = benchmark->nextInitKey();
			btree->insert(key, key);
		}
		if (conf.benchmark == RECOVERY_BENCH){
			timer t;
			t.start();
			btree->rebuild();
			t.end();
			printf("rebuild takes time: %.2lf ms\n", t.duration()/1000000.0);
			exit(0);
		}

		Result* results = new Result[conf.num_threads];
		memset(results, 0, sizeof(Result)*conf.num_threads);

		std::thread** pid = new std::thread*[conf.num_threads];
		bar = new boost::barrier(conf.num_threads+1);

		for (int i=0; i<conf.num_threads; i++){
			pid[i] = new std::thread(&Coordinator::worker, this, btree, i, &results[i], benchmark);
		}

		bar->wait();
		usleep(conf.duration*1000000);
		done = 1;

		Result final_result;
		for (int i=0; i<conf.num_threads; i++){
			pid[i]->join();
			final_result += results[i];
			printf("[WORKER]\tworker %d result %lf\n", i, results[i].throughput);
		}

		printf("[COORDINATOR]\tFinish benchmark..\n");
		printf("[COORDINATOR]\ttotal throughput: %.3lf Mtps\n", (double)final_result.throughput/1000000.0/conf.duration);

		#ifdef PERF_LATENCY
		printf("update latency: \t");
		final_result /= conf.num_threads;
		for(int i=0; i<3; i++){
			printf("%lf, ", final_result.update_latency_breaks[i]);
		}
		printf("%lf\n", final_result.update_latency_breaks[0] - final_result.update_latency_breaks[1]\
						-final_result.update_latency_breaks[2]);
		printf("find latency: ");
		printf("%lf, %lf, 0, %lf\t", final_result.find_latency_breaks[0] + final_result.find_latency_breaks[1],
			final_result.find_latency_breaks[0], final_result.find_latency_breaks[1]);
		printf("\n");
		#endif  // PERF_LATENCY

		delete btree;
		delete[] pid;
		delete[] results;

		unregister_threadinfo();
		close_nvm_mgr();
	}

private:
	Config conf __attribute__((aligned(64)));
	volatile int done __attribute__((aligned(64))) = 0;
	boost::barrier* bar __attribute__((aligned(64))) = 0;
};

}//nvindex

#endif
