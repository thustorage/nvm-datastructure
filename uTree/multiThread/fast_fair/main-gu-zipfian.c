#include <getopt.h>
#include <limits.h>
#include <signal.h>
#include <sys/time.h>
#include <cmath>
#include "zipfian_util.h"
#include "zipfian.h"
#include <sys/mman.h>
#include <fcntl.h>
#include "btree.h"
#include <stdlib.h>
#include <stdio.h>
#include <gperftools/profiler.h>
#include <algorithm>
extern "C"
{
    #include <atomic_ops.h>
}  

typedef uint64_t            setkey_t;
typedef void*               setval_t;


#define DEFAULT_DURATION                5000
#define DEFAULT_INITIAL                 10000000
#define DEFAULT_NB_THREADS              4
#define DEFAULT_RANGE                   0x7FFFFFFF
#define DEFAULT_SEED                    0
#define DEFAULT_UPDATE                  100
#define DEFAULT_ALTERNATE               0
#define DEFAULT_EFFECTIVE               0 
#define DEFAULT_UNBALANCED              0

#define XSTR(s)                         STR(s)
#define STR(s)                          #s

#define VAL_MIN                         INT_MIN
#define VAL_MAX                         INT_MAX
#define DETECT_LATENCY
//#define UNIFORM
#define CDF

inline long rand_range(long r); /* declared in test.c */

volatile AO_t stop;
unsigned int global_seed;
#ifdef TLS
    __thread unsigned int *rng_seed;
#else /* ! TLS */
    pthread_key_t rng_seed_key;
#endif /* ! TLS */
unsigned int levelmax;

char * thread_space_start_addr[2];
__thread char * start_addr;
__thread char * curr_addr;
uint64_t record[1000000];
uint64_t latency, insert_nb = 0;
__thread struct timespec T1, T2;
__thread PMEMobjpool *pop;
cpu_set_t cpuset[2];
//uint64_t zipfianData[10000000];

void bindCPU() {
  CPU_ZERO(&cpuset[0]);
  CPU_ZERO(&cpuset[1]);
  for (int j = 0; j < 18; j++) {
    CPU_SET(j, &cpuset[0]);
    CPU_SET(j + 18, &cpuset[1]);
  }
}

typedef struct barrier {
    pthread_cond_t complete;
    pthread_mutex_t mutex;
    int count;
    int crossing;
} barrier_t;

void barrier_init(barrier_t *b, int n)
{
    pthread_cond_init(&b->complete, NULL);
    pthread_mutex_init(&b->mutex, NULL);
    b->count = n;
    b->crossing = 0;
}

void barrier_cross(barrier_t *b)
{
    pthread_mutex_lock(&b->mutex);
    /* One more thread through */
    b->crossing++;
    /* If not all here, wait */
    if (b->crossing < b->count) {
        pthread_cond_wait(&b->complete, &b->mutex);
    } else {
        pthread_cond_broadcast(&b->complete);
        /* Reset for next time */
        b->crossing = 0;
    }
    pthread_mutex_unlock(&b->mutex);
}

int floor_log_2(unsigned int n) {
  int pos = 0;
  if (n >= 1<<16) { n >>= 16; pos += 16; }
  if (n >= 1<< 8) { n >>=  8; pos +=  8; }
  if (n >= 1<< 4) { n >>=  4; pos +=  4; }
  if (n >= 1<< 2) { n >>=  2; pos +=  2; }
  if (n >= 1<< 1) {           pos +=  1; }
  return ((n == 0) ? (-1) : pos);
}

/* 
 * Returns a pseudo-random value in [1;range).
 * Depending on the symbolic constant RAND_MAX>=32767 defined in stdlib.h,
 * the granularity of rand() could be lower-bounded by the 32767^th which might 
 * be too high for given values of range and initial.
 *
 * Note: this is not thread-safe and will introduce futex locks
 */
inline long rand_range(long r) {
    int m = RAND_MAX;
    int d, v = 0;
    
    do {
        d = (m > r ? r : m);		
        v += 1 + (int)(d * ((double)rand()/((double)(m)+1.0)));
        r -= m;
    } while (r > 0);
    return v;
}
long rand_range(long r);

bool simulate_conflict = false;
long max_range = 0;

/* Thread-safe, re-entrant version of rand_range(r) */
inline long rand_range_re(unsigned int *seed, long r) {
    int m = RAND_MAX;
    int d, v = 0;
    
    do {
        d = (m > r ? r : m);		
        v += 1 + (int)(d * ((double)rand_r(seed)/((double)(m)+1.0)));
        r -= m;
    } while (r > 0);
    v = v % max_range;
    return v;
}
long rand_range_re(unsigned int *seed, long r);

typedef struct thread_data {
    int           id;
    unsigned int  first;
    long          range;
    int           update;
    int           alternate;
    int           effective;
    unsigned long nb_add;
    unsigned long nb_added;
    unsigned long nb_remove;
    unsigned long nb_removed;
    unsigned long nb_contains;
    unsigned long nb_found;
    unsigned long nb_aborts;
    unsigned long nb_aborts_locked_read;
    unsigned long nb_aborts_locked_write;
    unsigned long nb_aborts_validate_read;
    unsigned long nb_aborts_validate_write;
    unsigned long nb_aborts_validate_commit;
    unsigned long nb_aborts_invalid_memory;
    unsigned long nb_aborts_double_write;
    unsigned long max_retries;
    unsigned int  seed;
    btree         *set;
    barrier_t     *barrier;
    unsigned long failures_because_contention;
    char * start_addr;
    int affinityNodeID;
    uint64_t padding[16];
} thread_data_t;

int global_id;
void *test(void *data) 
{
    int unext, last = -1;                                             
    setkey_t val = 0;                                            
    insert_nb = 0;
    pthread_t thread = pthread_self();
    char pathname[100] = "/home/fkd/CPTree-202006/mount/pmem0/pool-";
    char str_num[10];

    thread_data_t *d = (thread_data_t *)data;           
#ifdef USE_PMDK
    if (d->affinityNodeID == 1){
      pathname[34] = '1';
    }
    sprintf(str_num, "%d", d->id);
    strcat(pathname, str_num);
    printf("open %s\n", pathname);
    openPmemobjPool(pathname);
#endif
    int ret = pthread_setaffinity_np(thread, sizeof(cpu_set_t),
                                     &cpuset[d->affinityNodeID]);
    if (ret)
      perror("pthread_setaffinity_np");

    start_addr = d->start_addr;
    curr_addr = start_addr;
    barrier_cross(d->barrier);                                         /* Wait on barrier */
    unext = (rand_range_re(&d->seed, 100) - 1 < d->update);            /* Is the first op an update? */
#ifndef UNIFORM
    ZipfianGenerator zf(max_range);
#endif
    if (d->id == 1){
      ProfilerStart("profile_pm_0.99");
    }
    while (stop == 0) {

        if (unext) {   
#ifdef UNIFORM
                val = rand_range_re(&d->seed, d->range);
#else
                val = zf.Next();
#endif
        
#ifdef DETECT_LATENCY
                if (d->id == 1){
                  //zipfianData[(uint64_t)val]++;
                  clock_gettime(CLOCK_MONOTONIC, &T1);
                }
#endif
#ifdef NEW_CPTREE
                d->set->insert(val, (char*) val);
#else
                d->set->btree_insert(val, (char*) val);
#endif
                
#ifdef DETECT_LATENCY
                if (d->id == 1){
                  clock_gettime(CLOCK_MONOTONIC, &T2);
                  latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 +
                             (T2.tv_nsec - T1.tv_nsec) + 99) /
                            100;
                  record[latency] += 1;
                  insert_nb += 1;
                }
                
#endif
                d->nb_added++;
                last = val;
                d->nb_add++;
        } 
        else {                                                            
#ifdef UNIFORM
            if (d->alternate) {
                if (d->update == 0) {
                    if (last < 0) {
                        val = d->first;
                        last = val;
                    } else {                                           // last >= 0
                        val = rand_range_re(&d->seed, d->range);
                        last = -1;
                    }
                } else {                                               // update != 0
                    if (last < 0) {
                        val = rand_range_re(&d->seed, d->range);
                    } else {
                        val = last;
                    }
                }
            }	
            else val = rand_range_re(&d->seed, d->range);
#else
            val = zf.Next();
#endif
            
#ifdef DETECT_LATENCY
            if (d->id == 1) {
              //zipfianData[(uint64_t)val]++;
              clock_gettime(CLOCK_MONOTONIC, &T1);
            }
#endif
#ifdef NEW_CPTREE
            if (d->set->search(val) != NULL) d->nb_found++;
#else
            if (d->set->btree_search(val) != NULL) d->nb_found++;
#endif
#ifdef DETECT_LATENCY
                if (d->id == 1){
                    clock_gettime(CLOCK_MONOTONIC, &T2);
                    latency = ((T2.tv_sec - T1.tv_sec) * 1000000000 + (T2.tv_nsec - T1.tv_nsec) + 99) / 100;
                    record[latency] += 1;
                    insert_nb += 1;
                }
                
#endif
            d->nb_contains++;
        }

        /* Is the next op an update? */
        if (d->effective)                                              // a failed remove/add is a read-only tx
            unext = ((100 * (d->nb_added + d->nb_removed)) < (d->update * (d->nb_add + d->nb_remove + d->nb_contains)));
        else                                                           // remove/add (even failed) is considered as an update
            unext = (rand_range_re(&d->seed, 100) - 1 < d->update);

    }
    if (d->id == 1) {
      ProfilerStop();
    }
#ifdef DETECT_LATENCY
    if (d->id == 1){
        uint64_t cnt = 0;
        uint64_t nb_50 = insert_nb / 2;
        uint64_t nb_90 = insert_nb * 0.9;
        uint64_t nb_99 = insert_nb * 0.99;
        bool flag_50 = false, flag_90 = false, flag_99 = false;
        double latency_50, latency_90, latency_99;

        for (int i=0; i < 1000000 && !(flag_50 && flag_90 && flag_99); i++){
            cnt += record[i];
            if (!flag_50 && cnt >= nb_50){
                latency_50 = (double)i / 10.0;
                flag_50 = true;
            }
            if (!flag_90 && cnt >= nb_90){
                latency_90 = (double)i / 10.0;
                flag_90 = true;
            }
            if (!flag_99 && cnt >= nb_99){
                latency_99 = (double)i / 10.0;
                flag_99 = true;
            }
        }
        printf("medium latency is %.1lfus\n90%% latency is %.1lfus\n99%% latency is %.1lfus\n", latency_50, latency_90, latency_99);
#ifdef CDF
        FILE *f = fopen("cdf.txt", "w");
        cnt = 0;
        for (int i = 1; i < 1000000 && cnt < insert_nb * 0.99; i++) {
          cnt += record[i];
          fprintf(f, "%.1f,%lf\n", (float)i / 10.0,
                  (double)cnt / (double)insert_nb);
        }
        fclose(f);
#endif
        /*f = fopen("uniformData.txt", "w");
        sort(zipfianData, zipfianData + max_range, greater<uint64_t>());
        for (int i=0; i<10000; i++){
          fprintf(f, "%d, %d\n", i, zipfianData[i]);
        }
        fclose(f);*/
    }
#endif
    return NULL;
}

void catcher(int sig)
{
    printf("CAUGHT SIGNAL %d\n", sig);
}

int main(int argc, char **argv)
{
    struct option long_options[] = {
        // These options don't set a flag
        {"help",                      no_argument,       NULL, 'h'},
        {"duration",                  required_argument, NULL, 'd'},
        {"initial-size",              required_argument, NULL, 'i'},
        {"thread-num",                required_argument, NULL, 't'},
        {"range",                     required_argument, NULL, 'r'},
        {"seed",                      required_argument, NULL, 'S'},
        {"update-rate",               required_argument, NULL, 'u'},
        {"unbalance",                 required_argument, NULL, 'U'},
        {"elasticity",                required_argument, NULL, 'x'},
        {NULL,                        0,                 NULL, 0  }
    };


    int               i, c;
    unsigned long     size;
    setkey_t          last = 0;
    setkey_t          val = 0;
    unsigned long     reads, effreads, updates, effupds, aborts, aborts_locked_read, aborts_locked_write,
                      aborts_validate_read, aborts_validate_write, aborts_validate_commit,
                      aborts_invalid_memory, aborts_double_write, max_retries, failures_because_contention;
    thread_data_t     *data;
    pthread_t         *threads;
    pthread_attr_t    attr;
    barrier_t         barrier;
    struct timeval    start, end;
    struct timespec   timeout;
    int duration =    DEFAULT_DURATION;   
    int initial =     DEFAULT_INITIAL;    
    int nb_threads =  DEFAULT_NB_THREADS;  
    long range =      DEFAULT_RANGE;
    int seed =        DEFAULT_SEED;
    int update =      DEFAULT_UPDATE;    
    int alternate =   DEFAULT_ALTERNATE;
    int effective =   DEFAULT_EFFECTIVE;  
    int unbalanced =  DEFAULT_UNBALANCED;
    sigset_t          block_set;

#if defined(USE_PM) && !defined(USE_PMDK)
    int fd[2];
    void *pmem[2];
    uint64_t allocate_size = 700ULL * 1024ULL * 1024ULL * 1024ULL;
    fd[0] = open("/dev/dax0.0", O_RDWR);
    fd[1] = open("/dev/dax1.0", O_RDWR);
    for (int i = 0; i < 2; i++) {
      pmem[i] = mmap(NULL, allocate_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd[i], 0);
      thread_space_start_addr[i] = (char *)pmem[i] + SPACE_OF_MAIN_THREAD;
    }
    start_addr = (char *)pmem[0];
    curr_addr = start_addr;
#endif
    bindCPU();
    memset(record, 0, sizeof(record));
    //memset(zipfianData, 0, sizeof(zipfianData));

    while(1) {
        i = 0;
        c = getopt_long(argc, argv, "hAf:d:i:t:r:S:u:U:c:", long_options, &i);
        if(c == -1) break;
        if(c == 0 && long_options[i].flag == 0) c = long_options[i].val;

        switch(c) {
                case 0:
                    break;
                case 'h':
                    printf("intset -- STM stress test "
                                 "(skip list)\n\n"
                                 "Usage:\n"
                                 "  intset [options...]\n\n"
                                 "Options:\n"
                                 "  -h, --help\n"
                                 "        Print this message\n"
                                 "  -A, --Alternate\n"
                                 "        Consecutive insert/remove target the same value\n"
                                 "  -f, --effective <int>\n"
                                 "        update txs must effectively write (0=trial, 1=effective, default=" XSTR(DEFAULT_EFFECTIVE) ")\n"
                                 "  -d, --duration <int>\n"
                                 "        Test duration in milliseconds (0=infinite, default=" XSTR(DEFAULT_DURATION) ")\n"
                                 "  -i, --initial-size <int>\n"
                                 "        Number of elements to insert before test (default=" XSTR(DEFAULT_INITIAL) ")\n"
                                 "  -t, --thread-num <int>\n"
                                 "        Number of threads (default=" XSTR(DEFAULT_NB_THREADS) ")\n"
                                 "  -r, --range <int>\n"
                                 "        Range of integer values inserted in set (default=" XSTR(DEFAULT_RANGE) ")\n"
                                 "  -s, --seed <int>\n"
                                 "        RNG seed (0=time-based, default=" XSTR(DEFAULT_SEED) ")\n"
                                 "  -u, --update-rate <int>\n"
                                 "        Percentage of update transactions (default=" XSTR(DEFAULT_UPDATE) ")\n"
                                 "  -U, --unbalance <int>\n"
                                 "        Percentage of skewness of the distribution of values (default=" XSTR(DEFAULT_UNBALANCED) ")\n"
                                 "  -c, --conflict ratio <int>\n"
                                 "        Percentage of conflict among threads \n"
                                 );
                    exit(0);
                case 'A':
                    alternate = 1;
                    break;
                case 'f':
                    effective =  atoi(optarg);
                    break;
                case 'd':
                    duration =   atoi(optarg);
                    break;
                case 'i':
                    initial =    atoi(optarg);
                    break;
                case 't':
                    nb_threads = atoi(optarg);
                    break;
                case 'r':
                    range =      atol(optarg);
                    break;
                case 'S':
                    seed =       atoi(optarg);
                    break;
                case 'u':
                    update =     atoi(optarg);
                    break;
                case 'U':
                    unbalanced = atoi(optarg);
                    break;
                case 'c':
                    //simulate_conflict = true;
                    //max_range = NODE_MAX / 2 * (100.0 / atoi(optarg));
                    break;
                case '?':
                    printf("Use -h or --help for help\n");
                    exit(0);
                default:
                    exit(1);
        }
    }

    max_range = initial;

    assert(duration >= 0);
    assert(initial >= 0);
    assert(nb_threads > 0);
    assert(range > 0 && range >= initial);
    assert(update >= 0 && update <= 100);

    printf("Set type     : skip list\n");
    printf("Duration     : %d\n",  duration);
    printf("Initial size : %u\n",  initial);
    printf("Nb threads   : %d\n",  nb_threads);
    printf("Value range  : %ld\n", range);
    printf("Seed         : %d\n",  seed);
    printf("Update rate  : %d\n",  update);
    printf("Alternate    : %d\n",  alternate);
    printf("Efffective   : %d\n",  effective);
    printf("Type sizes   : int=%d/long=%d/ptr=%d/word=%d\n",
                                   (int)sizeof(int), (int)sizeof(long), (int)sizeof(void *), (int)sizeof(uintptr_t));
    timeout.tv_sec =               duration / 1000;
    timeout.tv_nsec =              (duration % 1000) * 1000000;

    if ((data = (thread_data_t *)malloc(nb_threads * sizeof(thread_data_t))) == NULL) {
        perror("malloc");
        exit(1);
    }
    if ((threads = (pthread_t *)malloc(nb_threads * sizeof(pthread_t))) == NULL) {
        perror("malloc");
        exit(1);
    }

    if (seed == 0) srand((int)time(0));
    else srand(seed);

    levelmax = floor_log_2((unsigned int) initial);

    /* create the skip list set and do inits */
    global_id = nb_threads * update / 100;
    btree *bt;
#ifdef USE_PMDK
    char pathname[100] = "/home/fkd/CPTree-202006/mount/pmem0/main_pool";
    openPmemobjPool(pathname);
    printf("open %s\n", pathname);
#endif
    bt = new btree();

    stop = 0;

    global_seed = rand();
#ifdef TLS
    rng_seed = &global_seed;
#else /*! TLS */
    if (pthread_key_create(&rng_seed_key, NULL) != 0) {
        fprintf(stderr, "Error creating thread local\n");
        exit(1);
    }
    pthread_setspecific(rng_seed_key, &global_seed);
#endif /* ! TLS */

    // Populate set
    printf("Adding %d entries to set\n", initial);
    i = 0;

    struct timeval start_time, end_time;
    uint64_t       time_interval;
    gettimeofday(&start_time, NULL);

    while (i < initial) {
#ifdef NEW_CPTREE
        bt->insert(i, (char*) i);
#else
        bt->btree_insert(i, (char*) i);
#endif
        
        last = val;
        i++;
    }

    gettimeofday(&end_time, NULL);
    time_interval = 1000000 * (end_time.tv_sec - start_time.tv_sec) + end_time.tv_usec - start_time.tv_usec;
    printf("Insert time_interval = %lu ns\n", time_interval * 1000);
    printf("average insert op = %lu ns\n",    time_interval * 1000 / initial);
    printf("Level max    : %d\n",             levelmax);

    // Access set from all threads
    barrier_init(&barrier, nb_threads + 1);
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    int nodeID;
    for (i = 0; i < nb_threads; i++) {
      nodeID = i & 0x1;
      printf("Creating thread %d\n", i);
      data[i].id = i + 1;
      data[i].first = last;
      data[i].range = range;
      data[i].update = update;
      data[i].alternate = alternate;
      data[i].effective = effective;
      data[i].nb_add = 0;
      data[i].nb_added = 0;
      data[i].nb_remove = 0;
      data[i].nb_removed = 0;
      data[i].nb_contains = 0;
      data[i].nb_found = 0;
      data[i].nb_aborts = 0;
      data[i].nb_aborts_locked_read = 0;
      data[i].nb_aborts_locked_write = 0;
      data[i].nb_aborts_validate_read = 0;
      data[i].nb_aborts_validate_write = 0;
      data[i].nb_aborts_validate_commit = 0;
      data[i].nb_aborts_invalid_memory = 0;
      data[i].nb_aborts_double_write = 0;
      data[i].max_retries = 0;
      data[i].seed = rand();
      data[i].set = bt;
      data[i].barrier = &barrier;
      data[i].failures_because_contention = 0;
      data[i].start_addr = thread_space_start_addr[nodeID] + (i / 2) * SPACE_PER_THREAD;
      data[i].affinityNodeID = nodeID;
      if (pthread_create(&threads[i], &attr, test, (void *)(&data[i])) != 0) {
        fprintf(stderr, "Error creating thread\n");
        exit(1);
      }
    }
    pthread_attr_destroy(&attr);

    // Catch some signals
    if (signal(SIGHUP, catcher) == SIG_ERR || 
        signal(SIGTERM, catcher) == SIG_ERR) {
        perror("signal");
        exit(1);
    }

    printf("CPU_SETSIZE=%d\n", CPU_SETSIZE);
    for (int i = 0; i < 2; i++) {
      printf("cpuset[%d] contained:", i);
      for (int j = 0; j < CPU_SETSIZE; j++)
        if (CPU_ISSET(j, &cpuset[i]))
          printf(" CPU %d", j);
      printf("\n");
    }

    // Start threads
    barrier_cross(&barrier);                                           

    printf("STARTING...\n");
    gettimeofday(&start, NULL);
    if (duration > 0) {
        nanosleep(&timeout, NULL);
    } else {
        sigemptyset(&block_set);
        sigsuspend(&block_set);
    }

#ifdef ICC
    stop = 1;
#else
    AO_store_full(&stop, 1);
#endif /* ICC */

    stop = 1;
    gettimeofday(&end, NULL);
    printf("STOPPING...\n");

    // Wait for thread completion
    for (i = 0; i < nb_threads; i++){
        if (pthread_join(threads[i], NULL) != 0) {
            fprintf(stderr, "Error waiting for thread completion\n");
            exit(1);
        }
    }
    

    duration =                    (end.tv_sec * 1000 + end.tv_usec / 1000) - (start.tv_sec * 1000 + start.tv_usec / 1000);
    aborts =                      0;
    aborts_locked_read =          0;
    aborts_locked_write =         0;
    aborts_validate_read =        0;
    aborts_validate_write =       0;
    aborts_validate_commit =      0;
    aborts_invalid_memory =       0;
    aborts_double_write =         0;
    failures_because_contention = 0;
    reads =                       0;
    effreads =                    0;
    updates =                     0;
    effupds =                     0;
    max_retries =                 0;
    for (i = 0; i < nb_threads; i++) {
        aborts +=                       data[i].nb_aborts;
        aborts_locked_read +=           data[i].nb_aborts_locked_read;
        aborts_locked_write +=          data[i].nb_aborts_locked_write;
        aborts_validate_read +=         data[i].nb_aborts_validate_read;
        aborts_validate_write +=        data[i].nb_aborts_validate_write;
        aborts_validate_commit +=       data[i].nb_aborts_validate_commit;
        aborts_invalid_memory +=        data[i].nb_aborts_invalid_memory;
        aborts_double_write +=          data[i].nb_aborts_double_write;
        failures_because_contention +=  data[i].failures_because_contention;
        reads +=                        data[i].nb_contains;
        effreads +=                     data[i].nb_contains + (data[i].nb_add - data[i].nb_added) + (data[i].nb_remove - data[i].nb_removed);
        updates +=                      (data[i].nb_add + data[i].nb_remove);
        effupds +=                      data[i].nb_removed + data[i].nb_added;
        if (max_retries < data[i].max_retries)
            max_retries = data[i].max_retries;
    }
    printf("Duration      : %d (ms)\n",          duration);
    printf("#txs          : %lu (%f / s)\n",     reads + updates, (reads + updates) * 1000.0 / duration);

    printf("#read txs     : ");
    if (effective) {
        printf("%lu (%f / s)\n",                 effreads,        effreads * 1000.0 / duration);
        printf("  #contains   : %lu (%f / s)\n", reads,           reads * 1000.0 / duration);
    } else printf("%lu (%f / s)\n",              reads,           reads * 1000.0 / duration);

    printf("#eff. upd rate: %f \n",              100.0 * effupds / (effupds + effreads));

    printf("#update txs   : ");
    if (effective) {
        printf("%lu (%f / s)\n",                 effupds, effupds * 1000.0 / duration);
        printf("  #upd trials : %lu (%f / s)\n", updates, updates * 1000.0 / duration);
    } else printf("%lu (%f / s)\n",              updates, updates * 1000.0 / duration);

    printf("#aborts       : %lu (%f / s)\n",     aborts, aborts * 1000.0 / duration);
    printf("  #lock-r     : %lu (%f / s)\n",     aborts_locked_read, aborts_locked_read * 1000.0 / duration);
    printf("  #lock-w     : %lu (%f / s)\n",     aborts_locked_write, aborts_locked_write * 1000.0 / duration);
    printf("  #val-r      : %lu (%f / s)\n",     aborts_validate_read, aborts_validate_read * 1000.0 / duration);
    printf("  #val-w      : %lu (%f / s)\n",     aborts_validate_write, aborts_validate_write * 1000.0 / duration);
    printf("  #val-c      : %lu (%f / s)\n",     aborts_validate_commit, aborts_validate_commit * 1000.0 / duration);
    printf("  #inv-mem    : %lu (%f / s)\n",     aborts_invalid_memory, aborts_invalid_memory * 1000.0 / duration);
    printf("  #dup-w      : %lu (%f / s)\n",     aborts_double_write, aborts_double_write * 1000.0 / duration);
    printf("  #failures   : %lu\n",              failures_because_contention);
    printf("Max retries   : %lu\n",              max_retries);

#ifndef TLS
    pthread_key_delete(rng_seed_key);
#endif /* ! TLS */

    free(threads);
    free(data);

    return 0;
}


