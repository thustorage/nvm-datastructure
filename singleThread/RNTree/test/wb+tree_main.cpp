#include <iostream>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <assert.h>
#include <sys/mman.h>
#include <fcntl.h>
#include "wbtree.h"
#include <gperftools/profiler.h>
#include "index_factory.h"
using namespace std;

#define OP_NUM 10000000
uint64_t keys[OP_NUM];

struct timeval start_time, end_time;
uint64_t time_interval;
char *per_thread_start_addr;
char *curr_addr;

void print_statics_insert()
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
    int fd = open("/dev/dax0.0", O_RDWR);
    void *pmem = mmap(NULL, (uint64_t)640ULL * 1024ULL * 1024ULL * 1024ULL,
                      PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    per_thread_start_addr = (char *)pmem;
    curr_addr = per_thread_start_addr;
    Index<uint64_t, void *, 64> *t = nvindex::getIndex(WB_TREE);

    srand((int)time(0));
    for (i = 0; i < OP_NUM; i++) keys[i] = i;
    for (i = 0; i < OP_NUM; i++) {
        int swap_pos = rand() % OP_NUM;
        uint64_t temp = keys[i];
        keys[i] = keys[swap_pos];
        keys[swap_pos] = temp;
    }

    printf("\n*********************************** The insert operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("wb+tree_insert");
    for (i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        //emulate_latency_ns(EXTRA_SCM_LATENCY);

        t->insert(keys[i], (void *)i);
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Insert time_interval = %lu ns\n", time_interval * 1000);
    printf("average insert op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_insert();

    printf("\n*********************************** The read operations ********************************\n");
    double latency_breaks[3];
    gettimeofday(&start_time, NULL);
    //ProfilerStart("wb+tree_search");
    for (int i = 0; i < OP_NUM; i++) {
      t->get(keys[i], latency_breaks);
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Update time_interval = %lu ns\n", time_interval * 1000);
    printf("average update op = %lu ns\n", time_interval * 1000 / OP_NUM);

    printf("\n*********************************** The update operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("wb+tree_update");
    for (int i = 0; i < OP_NUM; i++) {
        //simulate the 64B-value-persist latency.
        //emulate_latency_ns(EXTRA_SCM_LATENCY);
        t->update(keys[i], &keys[i], latency_breaks);
    }
    //ProfilerStop();
    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Read time_interval = %lu ns\n", time_interval * 1000);
    printf("average read op = %lu ns\n", time_interval * 1000 / OP_NUM);
    print_statics_update();

    printf("\n*********************************** The delete operations ********************************\n");
    gettimeofday(&start_time, NULL);
    //ProfilerStart("wb+tree_delete");
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

    return 0;
}
