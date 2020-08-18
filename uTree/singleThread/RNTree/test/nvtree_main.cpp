
#include "index_factory.h"
#include <assert.h>
#include <fcntl.h>
#include <gperftools/profiler.h>
#include <iostream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <time.h>
using namespace std;

#define OP_NUM 10000000
uint64_t keys[OP_NUM];

struct timeval start_time, end_time;
uint64_t time_interval;
char *per_thread_start_addr;
char *curr_addr;

//#define TEST_SCAN
#define SHUFFLE
#define DEFAULT_RANGE 0x7FFFFFFF
unsigned int seed;

inline long rand_range_re(unsigned int *seed, long r, long max_range) {
  int m = RAND_MAX;
  int d, v = 0;

  do {
    d = (m > r ? r : m);
    v += 1 + (int)(d * ((double)rand_r(seed) / ((double)(m) + 1.0)));
    r -= m;
  } while (r > 0);

  v = v % max_range;
  return v;
}

void print_statics_insert(uint64_t t = 0)
{
#ifdef STATICS_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
    others_latency = time_interval * 1000 - sort_latency - unsort_latency - balance_latency - insert_latency;
    printf("average sort\t unsor\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_latency * 1.0 / OP_NUM, unsort_latency * 1.0 / OP_NUM, balance_latency * 1.0 / OP_NUM, 
            insert_latency * 1.0 / OP_NUM, others_latency * 1.0 / OP_NUM);
    others_flush = flush_sum - sort_flush - unsort_flush - balance_flush - insert_flush;
    printf("average sort\t unsor\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_flush * 1.0 / OP_NUM, unsort_flush * 1.0 / OP_NUM, balance_flush * 1.0 / OP_NUM, 
            insert_flush * 1.0 / OP_NUM, others_flush * 1.0 / OP_NUM);
    others_wear= wear_byte_sum - sort_wear- unsort_wear - balance_wear - insert_wear;
    printf("average sort\t unsor\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_wear * 1.0 / OP_NUM, unsort_wear * 1.0 / OP_NUM, balance_wear * 1.0 / OP_NUM, 
            insert_wear * 1.0 / OP_NUM, others_wear * 1.0 / OP_NUM);
    flush_sum = wear_byte_sum = 0; 
#endif  
#ifdef STATICS_MALLOC
    printf("scm_size=%lu dram_size=%lu\n", scm_size, dram_size);
#endif
#ifdef TREE_LIST_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
    printf("average In\t Ln\t\n average: %.3lf\t %.3lf\t\n", t - ln_latency * 1.0 / OP_NUM, ln_latency * 1.0 / OP_NUM);
    printf("average In\t Ln\t\n average: %.3lf\t %.3lf\t\n", flush_sum * 1.0 / OP_NUM - ln_flush * 1.0 / OP_NUM, ln_flush * 1.0 / OP_NUM);
    printf("average In\t Ln\t\n average: %.3lf\t %.3lf\t\n", wear_byte_sum * 1.0 / OP_NUM - ln_wear * 1.0 / OP_NUM, ln_wear * 1.0 / OP_NUM);
    flush_sum = wear_byte_sum = 0; 
#endif  
}

void print_statics_update()
{
#ifdef STATICS_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
    flush_sum = wear_byte_sum = 0;
#endif
#ifdef STATICS_MALLOC
    printf("scm_size=%lu dram_size=%lu\n", scm_size, dram_size);
#endif    
}

void print_statics_delete(int i)
{
#ifdef STATICS_MALLOC
        if (i % ((int)(OP_NUM * 0.2)) == 0) {
            int num = i * 1.0 / OP_NUM * 100;
            printf("Delete %d: scm_size=%lu dram_size=%lu pmalloc_sum=%lu pfree_sum=%lu\n", num, scm_size, dram_size, pmalloc_sum, pfree_sum);
        }
#endif    
}

void print_statics_delete_final()
{
#ifdef STATICS_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
#endif    
}

int scanCnt = 0;
int total;
bool scanFunc(uint64_t key, void *value) {
  scanCnt++;
  // printf("scanCnt=%d, total=%d\n", scanCnt, total);
  return scanCnt >= total;
}

int main()
{
    register int i, j;
    int fd = open("/dev/dax0.0", O_RDWR);
    void *pmem = mmap(NULL, (uint64_t)600ULL * 1024ULL * 1024ULL * 1024ULL,
                      PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    per_thread_start_addr = (char *)pmem;
    curr_addr = per_thread_start_addr;
    Index<uint64_t, void *, 64> *t = nvindex::getIndex(NV_TREE);

    srand((int)time(0));
    for (i = 0; i < OP_NUM; i++) keys[i] = i+1;
#ifdef SHUFFLE
    for (i = 0; i < OP_NUM; i++) {
        int swap_pos = rand() % OP_NUM;
        uint64_t temp = keys[i];
        keys[i] = keys[swap_pos];
        keys[swap_pos] = temp;
    }
#endif
    printf("\n*********************************** The insert operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("nvtree_insert");
    for (i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        //emulate_latency_ns(EXTRA_SCM_LATENCY);
        //printf("insert keys[%d]=%d\n", i, keys[i]);
        t->insert(keys[i], &keys[i]);
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Insert time_interval = %lu ns\n", time_interval * 1000);
    printf("average insert op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_insert();
#ifndef TEST_SCAN
    printf("\n*********************************** The read operations ********************************\n");
    double latency_breaks[3];
    gettimeofday(&start_time, NULL);
    //ProfilerStart("nvtree_search");
    for (int i = 0; i < OP_NUM; i++) {
      //printf("Lookup key[%d]=%u\n", i, keys[i]);
      t->get(keys[i], latency_breaks);
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Read time_interval = %lu ns\n", time_interval * 1000);
    printf("average update op = %lu ns\n", time_interval * 1000 / OP_NUM);

    printf("\n*********************************** The update operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("nvtree_update");
    for (int i = 0; i < OP_NUM; i = (i + 1) % OP_NUM) {
      // simulate the 64B-value-persist latency.
      // emulate_latency_ns(EXTRA_SCM_LATENCY);
      t->update(keys[i], &keys[i], latency_breaks);
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Update time_interval = %lu ns\n", time_interval * 1000);
    printf("average read op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_update();

    printf("\n*********************************** The delete operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("nvtree_delete");
    for (i = 0; i < OP_NUM; i++) { /// 10 * 9
        print_statics_delete(i);
        t->remove(keys[i]);
    }
    //ProfilerStop();
    print_statics_delete(OP_NUM);
    
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Delete time_interval = %lu ns\n", time_interval * 1000);
    printf("average delete op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_delete_final();
#else
    printf("\n*********************************** The scan operations ********************************\n");
    seed = rand();
    int scan_op_num = 1000000;
    uint64_t k;
    int range[7] = {5, 10, 20, 50, 100, 200, 500};
    uint64_t first_keys[scan_op_num];
    for (i = 0; i < scan_op_num; i++) {
      first_keys[i] = rand_range_re(&seed, DEFAULT_RANGE, OP_NUM - 500);
    }
    for (i = 0; i < 7; i++) {
      total = range[i];
      gettimeofday(&start_time, NULL);
      for (j = 0; j < scan_op_num; j++) {
        scanCnt = 0;
        t->scan(first_keys[j], scanFunc);
      }
      gettimeofday(&end_time, NULL);
      time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) +
                      end_time.tv_usec - start_time.tv_usec;

      printf("Scan(%d keys) time_interval = %lu ns\n", range[i],
             time_interval * 1000);
      printf("average scan(%d keys) op = %lu ns\n", range[i],
             time_interval * 1000 / scan_op_num);
    }
#endif
    return 0;
}
