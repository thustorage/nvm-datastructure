#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "utree.h"
#include <gperftools/profiler.h>
#include <unistd.h>
using namespace std;
typedef uint64_t setkey_t;
typedef void *setval_t;

#define OP_NUM 10000000
setkey_t keys[OP_NUM];

struct timeval start_time, end_time;
uint64_t time_interval;

char * start_addr;
char * curr_addr;

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

void print_statics_insert()
{
#ifdef STATICS_EVALUATION
    printf("flushes=%lu modified_bytes=%lu\n", flush_sum, wear_byte_sum);
    others_latency = time_interval * 1000 - sort_latency - unsort_latency - balance_latency - insert_latency;
    printf("average sort\t unsort\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_latency * 1.0 / OP_NUM, unsort_latency * 1.0 / OP_NUM, balance_latency * 1.0 / OP_NUM, 
            insert_latency * 1.0 / OP_NUM, others_latency * 1.0 / OP_NUM);
    others_flush = flush_sum - sort_flush - unsort_flush - balance_flush - insert_flush;
    printf("average sort\t unsort\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_flush * 1.0 / OP_NUM, unsort_flush * 1.0 / OP_NUM, balance_flush * 1.0 / OP_NUM, 
            insert_flush * 1.0 / OP_NUM, others_flush * 1.0 / OP_NUM);
    others_wear= wear_byte_sum - sort_wear- unsort_wear - balance_wear - insert_wear;
    printf("average sort\t unsort\t balance\t insert\t other\t\n average: %.3lf\t %.3lf\t %.3lf\t %.3lf\t %.3lf\t\n", 
            sort_wear * 1.0 / OP_NUM, unsort_wear * 1.0 / OP_NUM, balance_wear * 1.0 / OP_NUM, 
            insert_wear * 1.0 / OP_NUM, others_wear * 1.0 / OP_NUM);
    flush_sum = wear_byte_sum = 0; 
#endif  
#ifdef STATICS_MALLOC
    printf("scm_size=%lu dram_size=%lu\n", scm_size, dram_size);
    printf("leaf_num=%lu inner_num=%lu index_num=%lu kv_num=%lu\n", leaf_sum, inner_sum, index_sum, kv_sum);
    m_stats.avgfill_leaves();
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
    printf("leaf_num=%lu inner_num=%lu index_num=%lu kv_num=%lu\n", leaf_sum, inner_sum, index_sum, kv_sum);
    m_stats.avgfill_leaves();
#endif    
}

void print_statics_delete(int i)
{
#ifdef STATICS_MALLOC
        if (i % (int)(OP_NUM * 0.2) == 0) {
            int num = i / OP_NUM * 100;
            printf("Delete %d: scm_size=%lu dram_size=%lu\n", num, scm_size, dram_size);
            printf("leaf_num=%lu inner_num=%lu index_num=%lu kv_num=%lu\n", leaf_sum, inner_sum, index_sum, kv_sum);
            m_stats.avgfill_leaves();
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
#ifndef USE_PMDK
    int fd = open("/dev/dax0.0", O_RDWR);
    void *pmem = mmap(NULL, (uint64_t)600ULL * 1024ULL * 1024ULL * 1024ULL,
                      PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    start_addr = (char *)pmem;
    curr_addr = start_addr;
    printf("start_addr=%p, end_addr=%p\n", start_addr,
           start_addr + (uint64_t)600ULL * 1024ULL * 1024ULL * 1024ULL);
#endif
    btree *bt;
    bt = new btree();
	srand((int)time(0));
    for (i = 0; i < OP_NUM; i++) keys[i] = i*3+1;
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
    //ProfilerStart("utree_insert");
    for (i = 0; i < OP_NUM; i++) {
      // printf("insert keys[%d]=%d\n", i, keys[i]);
      bt->insert(keys[i], (char *)i);
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) +
                    end_time.tv_usec - start_time.tv_usec;
    printf("Insert time_interval = %lu ns\n", time_interval * 1000);
	printf("average insert op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_insert();
    sleep(10);
#ifndef TEST_SCAN
    char *ret;
    printf("\n*********************************** The read operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("utree_search");
    for (int i = 0; i < OP_NUM; i = (i + 1) % OP_NUM) {
      ret = bt->search(keys[i]);
      if (ret == NULL){
        printf("fail to get keys[%d]=%lu", i, keys[i]);
      }
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Read time_interval = %lu ns\n", time_interval * 1000);
    printf("average update op = %lu ns\n", time_interval * 1000 / OP_NUM);
    sleep(10);
    printf("\n*********************************** The update operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("utree_update");
    for (int i = 0; i < OP_NUM; i = (i + 1) % OP_NUM) {
      bt->insert(keys[i], (char *)i);
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Update time_interval = %lu ns\n", time_interval * 1000);
    printf("average read op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_update();

    printf("\n*********************************** The delete operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("utree_delete");
    for (i = 0; i < 100000; i++) {
        print_statics_delete(i);
        //printf("remove keys[%d]=%d\n", i, keys[i]);
        bt->remove(keys[i*2]);
    }
    print_statics_delete(OP_NUM);
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
	time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
	printf("Delete time_interval = %lu ns\n", time_interval * 1000);
	printf("average delete op = %lu ns\n", time_interval * 1000 / 100000);
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
    uint64_t buf[1000];
    for (i = 0; i < 7; i++) {
      total = range[i];
      gettimeofday(&start_time, NULL);
      for (j = 0; j < scan_op_num; j++) {
        scanCnt = 0;
        bt->scan(first_keys[j], range[i], buf);
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
