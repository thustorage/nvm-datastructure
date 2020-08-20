#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <x86intrin.h>
#include <malloc.h>
#include <stdint.h>
#include <time.h>
#include <stdbool.h>
#include <emmintrin.h>
#include <libpmemobj.h>
#include <sys/stat.h>

typedef uint64_t keyType;
typedef uint64_t valueType;

/****************************************************************************************************************************/
//#define TREE_LIST_EVALUATION 
#ifdef TREE_LIST_EVALUATION
    uint64_t latency_start = 0, latency_end = 0;
    uint64_t flush_sum = 0, wear_byte_sum = 0, flush_start = 0, wear_byte_start = 0;
    uint64_t ln_latency = 0, ln_flush = 0, ln_wear = 0;
#endif
//#define STATICS_EVALUATION 
#ifdef STATICS_EVALUATION
    uint64_t latency_start, latency_end;
    uint64_t flush_sum, wear_byte_sum, flush_start, wear_byte_start;
    uint64_t sort_latency, unsort_latency, balance_latency, others_latency, insert_latency;
    uint64_t sort_flush, unsort_flush, balance_flush, others_flush, insert_flush;
    uint64_t sort_wear, unsort_wear, balance_wear, others_wear, insert_wear;
#endif
//#define STATICS_FREE
//#define STATICS_MALLOC
#ifdef STATICS_MALLOC
    uint64_t pmalloc_sum, pfree_sum, vmalloc_sum, vfree_sum; 
    uint64_t scm_size, dram_size;
    uint64_t value_size = 64;
#endif

/* Persist functions */
#define FLUSH_ALIGN ((uintptr_t)64)
#define HAS_SCM_LATENCY
#define M_PCM_CPUFREQ 3900
#define EXTRA_SCM_LATENCY 500
#define NS2CYCLE(__ns) ((__ns) * M_PCM_CPUFREQ / 1000)
#define CYCLE2NS(__cycles) ((__cycles) * 1000 / M_PCM_CPUFREQ)

const uint64_t SPACE_PER_THREAD = 16ULL * 1024ULL * 1024ULL * 1024ULL;
const uint64_t SPACE_OF_MAIN_THREAD = 16ULL * 1024ULL * 1024ULL * 1024ULL;
extern char *start_addr;
extern char *curr_addr;

static inline void asm_movnti(volatile uint64_t *addr, uint64_t val)
{
    __asm__ __volatile__ ("movnti %1, %0" : "=m"(*addr): "r" (val));
}
static inline void asm_clflush(volatile uint64_t *addr)
{
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)));
}
static inline void asm_mfence(void)
{
    __asm__ __volatile__ ("mfence");
}
static inline unsigned long long asm_rdtsc(void)
{
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    return ( (unsigned long long)lo)|( ((unsigned long long)hi)<<32 );
}
static inline void emulate_latency_ns(uint64_t ns)
{
    /*
    uint64_t cycles, start, stop;

    start = asm_rdtsc();
    cycles = NS2CYCLE(ns);

    do { 
    	stop = asm_rdtsc();
    } while (stop - start < cycles);
    */
   return ;
} 
void pmem_drain(void)
{
	_mm_sfence();	
}
void pmem_flush(const void *addr, size_t len, int is_balance)
{
	uintptr_t uptr;
	for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1); uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN) {
#ifdef TREE_LIST_EVALUATION
        flush_sum += 1;
#endif
#ifdef STATICS_EVALUATION
        flush_sum += 1;
#endif
        asm_clflush((uint64_t *)uptr);
#ifdef HAS_SCM_LATENCY
        emulate_latency_ns(EXTRA_SCM_LATENCY);
#endif
	}
}
void pmem_persist(const void *addr, size_t flush_len, size_t _modified_bytes, int is_balance)
{
#ifdef TREE_LIST_EVALUATION
    wear_byte_sum += _modified_bytes;
#endif
#ifdef STATICS_EVALUATION
    wear_byte_sum += _modified_bytes;
#endif
    pmem_drain();
	pmem_flush(addr, flush_len, is_balance);
	pmem_drain();
}
void persist_region(void *beg_addr, void *end_addr, int is_balance)
{
    size_t len = (uintptr_t)end_addr - (uintptr_t)beg_addr + 1;
    pmem_persist(beg_addr, len, len, is_balance);
}

/****************************************************************************************************************************/
const int ProbabilityBits = 1;
const int BitsInRandom = 31 / ProbabilityBits;
const int ProbabilityMask =  (1 << ProbabilityBits) - 1;   
const int MaxInnerLevel = 25;
typedef struct leafNodeStructure *leafNode;
typedef struct innerNodeStructure *innerNode;

/* Need to be persisted into the PM */
typedef struct leafNodeStructure 
{
    keyType key;	
    valueType value;
    leafNode leaf_forward;
}leafNodeStructure;

POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_TOID(btree, leafNodeStructure);
POBJ_LAYOUT_END(btree);
PMEMobjpool *pop;

typedef struct innerNodeStructure
{
#ifdef STATICS_FREE
    uint64_t level;
#endif
    leafNode leaf_downward;
    innerNode inner_forward[1];
}innerNodeStructure;

typedef struct listStructure{
   int level; 	  
   innerNode header; 
} *list;

inline leafNode newLeafNode() 
{
  TOID(leafNodeStructure) p;
  POBJ_ZNEW(pop, &p, leafNodeStructure);
  return (leafNode)pmemobj_direct(p.oid);
}

inline innerNode newInnerNode(int l) 
{
#ifdef STATICS_MALLOC 
    vmalloc_sum += 1;
    dram_size += sizeof(innerNodeStructure) + l * sizeof(innerNodeStructure*);
#endif
    innerNode tmp = (innerNode)malloc(sizeof(struct innerNodeStructure) + l * sizeof(innerNodeStructure*));
#ifdef STATICS_FREE 
    tmp->level = l;
#endif
    return tmp;
}

inline void freeLeafNode(leafNode p)
{
    return ;
}

inline void freeInnerNode(innerNode p)
{
#ifdef STATICS_FREE 
    vfree_sum += 1;
    dram_size -= sizeof(innerNodeStructure) + p->level * sizeof(innerNodeStructure*);
    pfree_sum += 1;
    scm_size -= sizeof(leafNodeStructure);
#endif    
    free(p);
}

innerNode NIL;
int randomsLeft;
int randomBits;

int randomLevel()
{
    int level = 0, b;
    do {
        b = randomBits & ProbabilityMask;
        if (!b) level++; 
        randomBits >>= ProbabilityBits;
        if (--randomsLeft == 0) {
            randomBits = random();
            randomsLeft = BitsInRandom;
        }
    } while (!b);
    return (level > MaxInnerLevel ? MaxInnerLevel : level);
};

/****************************************************************************************************************************/
int file_exists(const char *filename) {
  struct stat buffer;
  return stat(filename, &buffer);
}

void openPmemobjPool() {
  printf("use pmdk!\n");
  char pathname[100] = "/home/fkd/CPTree-202006/mount/pool";
  int sds_write_value = 0;
  pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
  if (file_exists(pathname) != 0) {
    printf("create new one.\n");
    if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(nvtree),
                              (uint64_t)200 * 1024 * 1024 * 1024, 0666)) ==
        NULL) {
      perror("failed to create pool.\n");
      return;
    }
  } else {
    printf("open existing one.\n");
    if ((pop = pmemobj_open(pathname, POBJ_LAYOUT_NAME(nvtree))) == NULL) {
      perror("failed to open pool.\n");
      return;
    }
  }
}

void init() 
{
  openPmemobjPool();
  NIL = newInnerNode(MaxInnerLevel);
  NIL->leaf_downward = newLeafNode();
  NIL->leaf_downward->key = 0x7fffffff;

  srandom((int)time(0));
  randomBits = random();
  randomsLeft = BitsInRandom;
};

list newList()
{
    init();
    list l;
#ifdef STATICS_MALLOC 
    vmalloc_sum += 1;
    dram_size += sizeof(struct listStructure);
#endif
    l = (list)malloc(sizeof(struct listStructure));
    l->level = 0;
    l->header = newInnerNode(MaxInnerLevel);
    l->header->leaf_downward = newLeafNode();
    for(int i = 0; i < MaxInnerLevel; i++) {
        l->header->inner_forward[i] = NIL;
    }
    l->header->leaf_downward->leaf_forward = NIL->leaf_downward;
    return l;
}; 

void freeList(list l) 
{
#ifdef STATICS_FREE 
    vfree_sum += 1;
    dram_size -= sizeof(struct listStructure);
#endif
    free(l);
};

bool insert(list l, keyType key, valueType value) 
{
    int k = l->level;
    innerNode p = l->header, q;
    innerNode update[MaxInnerLevel];

#ifdef STATICS_EVALUATION
    latency_start = asm_rdtsc();
    flush_start = flush_sum;
    wear_byte_start = wear_byte_sum;
#endif
    while (k >= 0) {
	    while (q = p->inner_forward[k], q->leaf_downward->key < key) p = q;
		update[k] = p;
        k--;
    }
    
    leafNode p_leaf = p->leaf_downward, q_leaf; 
    leafNode update_leaf;
    while (q_leaf = p_leaf->leaf_forward, q_leaf->key < key) p_leaf = q_leaf;
    update_leaf = p_leaf;
    
#ifdef STATICS_EVALUATION
    latency_end = asm_rdtsc();
    others_latency += CYCLE2NS(latency_end - latency_start);
    others_flush += flush_sum - flush_start;
    others_wear += wear_byte_sum - wear_byte_start;
#endif

    if (q_leaf != l->header->leaf_downward && q_leaf->key == key) {
        q_leaf->value = value;
        pmem_persist(&q_leaf->value, 8, 8, 8);
	    return false;
	}

#ifdef STATICS_EVALUATION
    latency_start = asm_rdtsc();
    flush_start = flush_sum;
    wear_byte_start = wear_byte_sum;
#endif

    k = randomLevel() - 1;  
    if (k > l->level) {
		k = ++l->level;
		update[k] = l->header;
	}

    if (k >= 0) {
        q = newInnerNode(k);
        q->leaf_downward = newLeafNode();
        q->leaf_downward->key = key;
        q->leaf_downward->value = value;

        while (k >= 0) {
    	    p = update[k];
    		q->inner_forward[k] = p->inner_forward[k];
    		p->inner_forward[k] = q;
            k--;
    	} 

#ifdef TREE_LIST_EVALUATION
        latency_start = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif
        q->leaf_downward->leaf_forward = update_leaf->leaf_forward;
        pmem_persist(&q->leaf_downward, sizeof(leafNodeStructure), sizeof(leafNodeStructure), 8);
        update_leaf->leaf_forward = q->leaf_downward;
        pmem_persist(&update_leaf->leaf_forward, 8, 8, 8);
#ifdef TREE_LIST_EVALUATION
        latency_end = asm_rdtsc();
        ln_latency += CYCLE2NS(latency_end - latency_start);
        ln_flush += flush_sum - flush_start;
        ln_wear += wear_byte_sum - wear_byte_start;
#endif
    }
    else {
#ifdef TREE_LIST_EVALUATION
        latency_start = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif
        q_leaf = newLeafNode();
        q_leaf->key = key;
        q_leaf->value = value;
        
        q_leaf->leaf_forward = update_leaf->leaf_forward;
        pmem_persist(&q, sizeof(leafNodeStructure), sizeof(leafNodeStructure), 8);
        update_leaf->leaf_forward = q_leaf;
        pmem_persist(&update_leaf->leaf_forward, 8, 8, 8);
#ifdef TREE_LIST_EVALUATION
        latency_end = asm_rdtsc();
        ln_latency += CYCLE2NS(latency_end - latency_start);
        ln_flush += flush_sum - flush_start;
        ln_wear += wear_byte_sum - wear_byte_start;
#endif
    }

#ifdef STATICS_EVALUATION
    latency_end = asm_rdtsc();
    insert_latency += CYCLE2NS(latency_end - latency_start);
    insert_flush += flush_sum - flush_start;
    insert_wear += wear_byte_sum - wear_byte_start;
#endif
    
    return true;
}

bool del(list l, keyType key) 
{
    int k = l->level, m = l->level;
    innerNode p = l->header, q;
    innerNode update[MaxInnerLevel];

    while (k >= 0) {
	    while (q = p->inner_forward[k], q->leaf_downward->key < key) p = q;
		update[k] = p;
        k--;
    }

    leafNode p_leaf = p->leaf_downward, q_leaf;
    leafNode update_leaf;
    while (q_leaf = p_leaf->leaf_forward, q_leaf->key < key) p_leaf = q_leaf;
    update_leaf = p_leaf;

    if (q_leaf->key == key) {
        bool f = false;
        if (q->leaf_downward == q_leaf) {
            for (k = 0; k <= m && (p=update[k])->inner_forward[k] == q; k++) {
                p->inner_forward[k] = q->inner_forward[k];
                f = true;
            }
        }
        
        update_leaf->leaf_forward = q_leaf->leaf_forward;
        pmem_persist(&update_leaf->leaf_forward, 8, 8, 8);

        if (f) freeInnerNode(q);
        else freeLeafNode(q_leaf);

        while (l->header->inner_forward[m] == NIL && m > 0) m--;  
		l->level = m;
        
	    return true;
	}
    else return false;
}

bool search(list l, keyType key, valueType *valuePointer)
{
    int k = l->level;
    innerNode p = l->header, q;

    while (k >= 0) {
	    while (q = p->inner_forward[k], q->leaf_downward->key < key) p = q;
        if (q->leaf_downward->key == key) {
            *valuePointer = q->leaf_downward->value;
            return true;
        }
        k--;
    }

    leafNode p_leaf = p->leaf_downward, q_leaf;
    while (q_leaf = p_leaf->leaf_forward, q_leaf->key < key) p_leaf = q_leaf;

    if (q_leaf->key == key) {
        *valuePointer = p_leaf->value;
        return true;
    }

    return false;
}
