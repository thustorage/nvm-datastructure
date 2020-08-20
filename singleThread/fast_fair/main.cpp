#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "btree.h"
#include <gperftools/profiler.h>
#include <unistd.h>

using namespace std;

//#define CDF
#define SHUFFLE
#define OP_NUM 10000000
uint64_t keys[OP_NUM];

struct timeval start_time, end_time;
uint64_t time_interval;
char * start_addr;
char * curr_addr;
struct timespec T1, T2;
uint64_t record[1000000];
uint64_t latency;

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


int main()
{
    register int i;
    btree *bt;
    uint64_t cnt, insert_nb, delete_nb;

    bt = new btree();

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

    //FILE *f = fopen("put_cdf.txt", "w");
    //memset(record, 0, sizeof(record));
    gettimeofday(&start_time, NULL);
    //ProfilerStart("fast_fair_insert");
    for (i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        //emulate_latency_ns(EXTRA_SCM_LATENCY);
#ifdef STATICS_MALLOC 
        scm_size += sizeof(64);
#endif
#ifdef CDF
        clock_gettime(CLOCK_MONOTONIC, &T1);
#endif
        bt->btree_insert(keys[i], (char *)keys[i]);
#ifdef CDF
        clock_gettime(CLOCK_MONOTONIC, &T2);
        latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec) + 99) / 100;
        record[latency] += 1;
#endif
    }
    //ProfilerStop();
#ifdef CDF
    cnt = record[0];
    insert_nb = OP_NUM;
    for (int i = 1; i<1000000 && cnt < insert_nb; i++){
      cnt += record[i];
      fprintf(f, "%.1f, %lf\n", (float)i / 10.0, (double)cnt / (double)insert_nb);
    }
#endif
  gettimeofday(&end_time, NULL);
  time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) +
                  end_time.tv_usec - start_time.tv_usec;
  printf("Insert time_interval = %lu ns\n", time_interval * 1000);
  printf("average insert op = %lu ns\n", time_interval * 1000 / OP_NUM);
  print_statics_insert();
  //fclose(f);
  sleep(10);
  printf("\n*********************************** The read operations "
         "********************************\n");
  gettimeofday(&start_time, NULL);
  // ProfilerStart("fast_fair_search");
  for (int i = 0; i < OP_NUM; i++) {
    bt->btree_search(keys[i]);
  }
  // ProfilerStop();
  gettimeofday(&end_time, NULL);
  time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) +
                  end_time.tv_usec - start_time.tv_usec;
  printf("Read time_interval = %lu ns\n", time_interval * 1000);
  printf("average update op = %lu ns\n", time_interval * 1000 / OP_NUM);
  sleep(10);
  printf("\n*********************************** The update operations "
         "********************************\n");
  gettimeofday(&start_time, NULL);
  // ProfilerStart("fast_fair_update");
  for (int i = 0; i < OP_NUM; i = (i + 1) % OP_NUM) {
    // simulate the 64B-value-persist latency.
    //emulate_latency_ns(EXTRA_SCM_LATENCY);
    bt->btree_insert(keys[i], (char *)keys[i]);
  }
  // ProfilerStop();
  gettimeofday(&end_time, NULL);
  time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) +
                  end_time.tv_usec - start_time.tv_usec;
  printf("Update time_interval = %lu ns\n", time_interval * 1000);
  printf("average read op = %lu ns\n", time_interval * 1000 / OP_NUM);
  print_statics_update();

  printf("\n*********************************** The delete operations ********************************\n");

  //f = fopen("delete_cdf.txt", "w");
  //memset(record, 0, sizeof(record));
  gettimeofday(&start_time, NULL);
  // ProfilerStart("fast_fair_delete");
  for (i = 0; i < 100000; i++) { /// 10 * 9
    // printf("delete keys[%d]=%lu\n", i, keys[i]);
    print_statics_delete(i);
#ifdef CDF
    clock_gettime(CLOCK_MONOTONIC, &T1);
#endif
    //printf("delete keys[%d]=%lu\n", i, keys[i]);
    bt->btree_delete(keys[i]); // bug.
#ifdef CDF
    clock_gettime(CLOCK_MONOTONIC, &T2);
    latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 +
               (T2.tv_nsec - T1.tv_nsec) + 99) /
              100;
    record[latency] += 1;
#endif
  }
  // ProfilerStop();
#ifdef CDF
  cnt = record[0];
  delete_nb = 100000;
  for (int i = 1; i < 1000000 && cnt < delete_nb; i++) {
    cnt += record[i];
    fprintf(f, "%.1f, %lf\n", (float)i / 10.0, (double)cnt / (double)delete_nb);
  }
#endif
  print_statics_delete(OP_NUM);
  gettimeofday(&end_time, NULL);
  time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) +
                  end_time.tv_usec - start_time.tv_usec;
  printf("Delete time_interval = %lu ns\n", time_interval * 1000);
  printf("average delete op = %lu ns\n", time_interval * 1000 / 100000);
  print_statics_delete_final();
  //fclose(f);

  return 0;
}
