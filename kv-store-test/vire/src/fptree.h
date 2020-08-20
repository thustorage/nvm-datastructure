#ifndef __FACE_H__
#define __FACE_H__ 


#if defined(_KERNEL) || defined(_STANDALONE)
#include <sys/cdefs.h>
#include <sys/param.h>
#include <sys/types.h>
#else
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <inttypes.h>
#include <string.h>
#include <limits.h>
#include <assert.h>
#include <errno.h>
#include <err.h>
#include <algorithm>
#endif

#include "utils.h"
#include <libpmemobj.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

extern "C" {
#include "gc/portable_defns.h"
#include "gc/ptst.h"
#include "gc/set.h"
#include "atomic_ops/atomic_ops.h"
}


#define FP_KEY 8
#define FP_VALUE 8

typedef int64_t setkey_t;
typedef void *setval_t;
// typedef struct{
    // char v[FP_VALUE];
// }setval_t;


static int gc_id[2];
static int gc_size[2];

pthread_key_t ptst_key;
ptst_t *ptst_list;

static unsigned int next_id;

const uint64_t SPACE_PER_THREAD = 120ULL * 1024ULL * 1024ULL * 1024ULL;
extern __thread char *start_addr;
extern __thread char *curr_addr;

struct fptree;
typedef struct fptree fptree_t;
typedef struct fptree_inode fptree_inode_t;
typedef struct fptree_leaf fptree_leaf_t;

// Version number layout: flags and two counters.
#define NODE_LOCKED (1U << 0)     // lock (for the writers)
#define NODE_INSERTING (1U << 1)  // "dirty": for inserting
#define NODE_SPLITTING (1U << 2)  // "dirty": for splitting
#define NODE_DELETED (1U << 3)    // indicate node deletion
#define NODE_ISROOT (1U << 4)     // indicate root of B+ tree
#define NODE_ISBORDER (1U << 5)   // indicate border node

// Note: insert and split counter bit fields are adjacent such that
// the inserts may overflow into the split.  That is, 7 + 18 bits in
// total, thus making 2^25 the real overflow.
#define NODE_VINSERT 0x00001fc0  // insert counter (bits 6-13)
#define NODE_VINSERT_SHIFT 6
#define NODE_VSPLIT 0x7fffe000  // split counter (bits 13-31)
#define NODE_VSPLIT_SHIFT 13

#define M_PCM_CPUFREQ 3900
#define EXTRA_SCM_LATENCY 500

#define NODE_MAX 64
#define NODE_PIVOT 32
// #define USE_GC

// Poor man's "polymorphism": a structure to access the version field.
// NODE_ISBORDER determines whether it is interior or border (leaf) node.
typedef struct {
    uint32_t version;
    unsigned _pad;
} fptree_node_t;

typedef struct in_kv {
    setkey_t key;
    fptree_node_t *child;
} in_kv;

typedef struct ln_kv {
    setkey_t key;
    setval_t val;
} ln_kv;

struct fptree_inode {
    uint32_t version;
    uint8_t nkeys;
    in_kv slot_kv[NODE_MAX + 1];
    fptree_inode_t *parent;
};

struct fptree_leaf {
    uint32_t version;
    uint8_t nkeys;
    ln_kv slot_kv[NODE_MAX];

    uint64_t bitmap;
    uint8_t fingerprints[NODE_MAX];

    fptree_leaf_t *next;
    fptree_leaf_t *prev;
    fptree_inode_t *parent;
};

POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_TOID(btree, fptree_leaf_t);
POBJ_LAYOUT_END(btree);
extern __thread PMEMobjpool *pop;

struct fptree {
    fptree_node_t *root;
    fptree_leaf_t initleaf;
};

/* Recycled nodes are filled with this value if WEAK_MEM_ORDER. */
#define INVALID_BYTE 0
#define INITIALISE_NODES(_p,_c) memset((_p), INVALID_BYTE, (_c));

/* Number of unique block sizes we can deal with. */
#define MAX_SIZES 20
#define MAX_HOOKS 4

/* The initial number of allocation chunks for each per-blocksize list.
 * Popular allocation lists will steadily increase the allocation unit
 * in line with demand. */
#define ALLOC_CHUNKS_PER_LIST 10

/* How many times should a thread call gc_enter(), seeing the same epoch
 * each time, before it makes a reclaim attempt? */
#define ENTRIES_PER_RECLAIM_ATTEMPT 100

/*  0: current epoch -- threads are moving to this;
 * -1: some threads may still throw garbage into this epoch;
 * -2: no threads can see this epoch => we can zero garbage lists;
 * -3: all threads see zeros in these garbage lists => move to alloc lists. */
#ifdef WEAK_MEM_ORDER
#define NR_EPOCHS 4
#else
#define NR_EPOCHS 3
#endif

/* A chunk amortises the cost of allocation from shared lists. It also
 * helps when zeroing nodes, as it increases per-cacheline pointer density
 * and means that node locations don't need to be brought into the cache 
 * (most architectures have a non-temporal store instruction). */
#define BLKS_PER_CHUNK 100
typedef struct chunk_st chunk_t;
struct chunk_st
{
    chunk_t *next;             /* chunk chaining                 */
    unsigned int i;            /* the next entry in blk[] to use */
    void *blk[BLKS_PER_CHUNK];
};

static struct gc_global_st
{
    CACHE_PAD(0);

    /* The current epoch. */
    VOLATILE unsigned int current;
    CACHE_PAD(1);

    /* Exclusive access to gc_reclaim(). */
    VOLATILE unsigned int inreclaim;
    CACHE_PAD(2);

    /* RUN-TIME CONSTANTS (to first approximation) */

    /* Memory page size, in bytes. */
    unsigned int page_size;

    /* Node sizes (run-time constants). */
    int nr_sizes;
    int blk_sizes[MAX_SIZES];

    /* Registered epoch hooks. */
    int nr_hooks;
    hook_fn_t hook_fns[MAX_HOOKS];
    CACHE_PAD(3);

    /* DATA WE MAY HIT HARD */

    /* Chain of free, empty chunks. */
    chunk_t * VOLATILE free_chunks;

    /* Main allocation lists. */
    chunk_t * VOLATILE alloc[MAX_SIZES];
    VOLATILE unsigned int alloc_size[MAX_SIZES];
#ifdef PROFILE_GC
    VOLATILE unsigned int total_size;
    VOLATILE unsigned int allocations;
    VOLATILE unsigned int num_reclaims;
#endif
} gc_global;


/*
 * The helper functions - some primitives for persist operations.
 */
#define FLUSH_ALIGN ((uintptr_t)64)
#define HAS_SCM_LATENCY
#define NS2CYCLE(__ns) ((__ns) * M_PCM_CPUFREQ / 2000)
#define CYCLE2NS(__cycles) ((__cycles) * 1000 / M_PCM_CPUFREQ)




/* Used for avoiding memory leak when split or merge operations */
/*
extern __thread fptree_leaf_t *PCurrentLeaf;
extern __thread fptree_leaf_t *PNewLeaf;
extern __thread fptree_leaf_t *PPrevLeaf;
*/

void _init_set_subsystem(void);

bool fptree_get(fptree_t *tree, setkey_t key);

bool fptree_put(fptree_t *tree, setkey_t key, setval_t val);
fptree_t * fptree_create();

bool fptree_del(fptree_t *tree, setkey_t key);

static inline fptree_leaf_t *cast_to_leaf(fptree_node_t *node) {
    ASSERT((node->version & NODE_ISBORDER) != 0);
    return (fptree_leaf_t *)node;
}

static inline fptree_inode_t *cast_to_inode(fptree_node_t *node) {
    ASSERT((node->version & NODE_ISBORDER) == 0);
    return (fptree_inode_t *)node;
}

/*
 * Diagnostic checks to ease the debugging; they are valid only for
 * the single-threaded testing.
 */
static inline bool validate_leaf(const fptree_leaf_t *leaf) {
    NOSMP_ASSERT(!leaf->prev || leaf->prev->next == leaf);
    NOSMP_ASSERT(!leaf->next || leaf->next->prev == leaf);
    return true;
}

static inline bool validate_inode(const fptree_inode_t *inode) {
    unsigned nkeys = inode->nkeys;

    for (unsigned i = 1; i < nkeys; i++) {
        NOSMP_ASSERT(inode->slot_kv[i - 1].key < inode->slot_kv[i].key);
    }
    for (unsigned i = 0; i < nkeys + 1; i++) {
        uint32_t v = (inode->slot_kv[i].child)->version;

        if ((v & NODE_DELETED) == 0 && (v & NODE_ISBORDER) != 0) {
            fptree_leaf_t *leaf = cast_to_leaf(inode->slot_kv[i].child);
            NOSMP_ASSERT(validate_leaf(leaf));
        }
    }
    return true;
}

/*
 * The helper functions - some primitives for persist operations.
 */
// #define FLUSH_ALIGN ((uintptr_t)64)
// #define HAS_SCM_LATENCY
// #define NS2CYCLE(__ns) ((__ns) * M_PCM_CPUFREQ / 2000)
// #define CYCLE2NS(__cycles) ((__cycles) * 1000 / M_PCM_CPUFREQ)

/* Used for avoiding memory leak when split or merge operations */
__thread fptree_leaf_t *PCurrentLeaf;
__thread fptree_leaf_t *PNewLeaf;
__thread fptree_leaf_t *PPrevLeaf;

static inline void asm_movnti(volatile uint64_t *addr, uint64_t val) {
    __asm__ __volatile__("movnti %1, %0" : "=m"(*addr) : "r"(val));
}
static inline void asm_clflush(volatile uint64_t *addr) {
    //__asm__ __volatile__("clflush %0" : : "m"(*addr));
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)));
}
static inline void asm_mfence(void) { __asm__ __volatile__("mfence"); }
static inline unsigned long long asm_rdtsc(void) {
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc" : "=a"(lo), "=d"(hi));
    return ((unsigned long long)lo) | (((unsigned long long)hi) << 32);
}
static inline void emulate_latency_ns(uint64_t ns) {
    /*
    struct timespec T0, T1;
    uint64_t temp = 0;
    clock_gettime(CLOCK_MONOTONIC, &T0);
    while (temp < ns) {
        clock_gettime(CLOCK_MONOTONIC, &T1);
        temp = (T1.tv_sec - T0.tv_sec) * 1000000000 + T1.tv_nsec - T0.tv_nsec;
    }
    */
    // uint64_t cycles, start, stop;

    // start = asm_rdtsc();
    // cycles = NS2CYCLE(ns);

    // do {
    // stop = asm_rdtsc();
    // } while (stop - start < cycles);
    return ;
}
void pmem_drain(void) { _mm_sfence(); }
void pmem_flush(const void *addr, size_t len) {
    uintptr_t uptr;

    for (uptr = (uintptr_t)addr & ~(FLUSH_ALIGN - 1);
         uptr < (uintptr_t)addr + len; uptr += FLUSH_ALIGN) {
        //_mm_clflush((char *)uptr);
        asm_clflush((uint64_t *)uptr);
    }
    unsigned time = 0;
    if (len < 4 * 64) {
        time = 500;
    } else {
        time = len * 2;  // 0.5byte/ns
    }
    //emulate_latency_ns(time);
}
void pmem_persist(const void *addr, size_t flush_len, size_t _modified_bytes) {
    // pmem_drain();
    pmem_flush(addr, flush_len);
    pmem_drain();
}
void persist_region(void *beg_addr, void *end_addr) {
    size_t len = (uintptr_t)end_addr - (uintptr_t)beg_addr + 1;
    pmem_persist(beg_addr, len, len);
}

/*
 * The helper functions - some primitives for fingerprints and bitmap.
 */

// The special FPTree functions
void set_hash_fp(uint8_t &fp, const setkey_t &key) { fp = key % 256; }

bool check_hash_fp(uint8_t fp, const setkey_t &key) {
    return (fp == key % 256);
}

void set_bitmap(uint64_t &bitmap, const int slot) {
    bitmap = bitmap | (1ll << slot);
}

void reset_bitmap(uint64_t &bitmap, const int slot) {
    bitmap = bitmap & (~(1ll << slot));
}

bool check_bitmap(uint64_t bitmap, const int slot) {
    return ((bitmap >> slot) & 1);
}

/*
 * The helper functions - some primitives for locking operations.
 */

/*
 * stable_version: capture a snapshot of the node version when neither
 * insertion nor split is happening (i.e. the node is not "dirty").
 * This will be used be used to check the sequence (and retry on change).
 */
static uint32_t stable_version(fptree_node_t *node) {
    unsigned bcount = SPINLOCK_BACKOFF_MIN;
    uint32_t v;

    v = node->version;
    while (__predict_false(v & (NODE_INSERTING | NODE_SPLITTING))) {
        SPINLOCK_BACKOFF(bcount);
        v = node->version;
    }
    atomic_thread_fence(memory_order_acquire);
    return v;
}

static inline bool node_locked_p(const fptree_node_t *node) {
    return (node->version & NODE_LOCKED) != 0;
}

static void lock_node(fptree_node_t *node) {
    unsigned bcount = SPINLOCK_BACKOFF_MIN;
    uint32_t v;
again:
    v = node->version;
    if (v & NODE_LOCKED) {
        SPINLOCK_BACKOFF(bcount);
        goto again;
    }
    if (!atomic_compare_exchange_weak(&node->version, v, v | NODE_LOCKED))
        goto again;

    /* XXX: Use atomic_compare_exchange_weak_explicit() instead. */
    atomic_thread_fence(memory_order_acquire);
}

static void unlock_node(fptree_node_t *node) {
    uint32_t v = node->version;

    ASSERT(node_locked_p(node));

    /*
     * Increment the counter (either for insert or split).
     * - Inserts can overflow into splits (since the range is small).
     * - Clear NODE_ISROOT if split occured, it has a parent now.
     */
    if (v & NODE_INSERTING) {
        uint32_t c = (v & NODE_VINSERT) + (1 << NODE_VINSERT_SHIFT);
        v = (v & ~NODE_VINSERT) | c;
    }
    if (v & NODE_SPLITTING) {
        uint32_t c = (v & NODE_VSPLIT) + (1 << NODE_VSPLIT_SHIFT);
        v = ((v & ~NODE_ISROOT) & ~NODE_VSPLIT) | (c & NODE_VSPLIT);
    }

    /* Release the lock and clear the operation flags. */
    v &= ~(NODE_LOCKED | NODE_INSERTING | NODE_SPLITTING);

    /* Note: store on an integer is atomic. */
    atomic_thread_fence(memory_order_release);
    node->version = v;
}

static inline fptree_node_t *node_get_parent(fptree_node_t *node) {
    if (node->version & NODE_ISBORDER) {
        fptree_leaf_t *leaf = cast_to_leaf(node);
        return (fptree_node_t *)leaf->parent;
    } else {
        fptree_inode_t *inode = cast_to_inode(node);
        return (fptree_node_t *)inode->parent;
    }
}

static inline fptree_node_t *walk_to_root(fptree_node_t *node) {
    fptree_node_t *parent;

    while ((parent = node_get_parent(node)) != NULL) {
        node = parent;
    }
    return node;
}

static inline void node_set_parent(fptree_node_t *node,
                                   fptree_inode_t *parent) {
    /* Note: the parent member is locked by the parent lock!. */
    ASSERT(parent == NULL || (node->version & NODE_DELETED) != 0 ||
           node_locked_p((fptree_node_t *)parent));

    if ((node->version & NODE_ISBORDER) == 0) {
        fptree_inode_t *inode = cast_to_inode(node);
        inode->parent = parent;
    } else {
        fptree_leaf_t *leaf = cast_to_leaf(node);
        leaf->parent = parent;
    }
}

static fptree_node_t *lock_parent_node(fptree_node_t *node) {
    fptree_node_t *parent;
retry:
    if ((parent = node_get_parent(node)) == NULL) {
        return NULL;
    }
    lock_node(parent);
    if (__predict_false(node_get_parent(node) != parent)) {
        /* Race: unlock and try again. */
        unlock_node(parent);
        goto retry;
    }
    return parent;
}

inline void *smart_alloc(ptst_t *ptst, int id) {
    if (id == 0){
      TOID(fptree_leaf_t) p;
      POBJ_ZNEW(pop, &p, fptree_leaf_t);
      // printf("allocate %d bytes successfully\n", size);
      return (void *)pmemobj_direct(p.oid);
      /*
      void *ret = curr_addr;
      curr_addr += sizeof(fptree_leaf_t);
      if (curr_addr >= start_addr + SPACE_PER_THREAD) {
        printf("start_addr is %p, curr_addr is %p, SPACE_PER_THREAD is %lu, no "
               "free space to alloc\n",
               start_addr, curr_addr, SPACE_PER_THREAD);
        exit(0);
        }
        return ret;
        */
    }else{
#ifdef USE_GC
    return gc_alloc(ptst, gc_id[id]);
#else
    return malloc(sizeof(fptree_inode_t));
#endif
    }
}

inline void smart_free(ptst_t *ptst, void *n, int id) {
    if (id == 0){
        return ;
    }else{
#ifdef USE_GC
    gc_free(ptst, n, gc_id[id]);
#else
    free(n);
#endif
    }
}

/*
 * Primitives to manage leaf nodes of the B+ tree.
 */
static fptree_leaf_t *leaf_create(ptst_t *ptst) {
    fptree_leaf_t *leaf = (fptree_leaf_t *)smart_alloc(ptst, 0);
    memset(leaf, 0, sizeof(fptree_leaf_t));
    leaf->version = NODE_ISBORDER;
    return leaf;
}

static inline unsigned leaf_find_lv(const fptree_leaf_t *leaf, setkey_t key,
                                    bool *found) {
    unsigned i, nkeys = leaf->nkeys;
    NOSMP_ASSERT(validate_leaf(leaf));

    for (i = 0; i < NODE_MAX; i++)
        if (check_bitmap(leaf->bitmap, i) &&
            check_hash_fp(leaf->fingerprints[i], key) &&
            key == leaf->slot_kv[i].key) {
            *found = true;
            return i;
        }

    ASSERT(i < (NODE_MAX + 1));
    *found = false;
    return 0;
}

// Find the first zereo in the leaf node bitmap
static inline int find_first_zero(fptree_leaf_t *leaf) {
    unsigned i;
    for (i = 0; i < NODE_MAX; i++)
        if (!check_bitmap(leaf->bitmap, i)) return i;
    return i;
}

static bool key_geq(const fptree_leaf_t *leaf, setkey_t key) {
    int slot = -1;
    for (unsigned i = 0; i < leaf->nkeys; i++)
        if (check_bitmap(leaf->bitmap, i) &&
            (slot == -1 || leaf->slot_kv[i].key < leaf->slot_kv[slot].key)) {
            slot = i;
        }
    if (slot == -1) return false;

    ASSERT((leaf->version & NODE_ISBORDER) != 0);
    // return !empty && (key > slice || (key == slice && len >= slen));
    return leaf->nkeys != 0 && key >= leaf->slot_kv[slot].key;
}

static bool leaf_insert_key(fptree_node_t *node, setkey_t key, setval_t val) {
    fptree_leaf_t *leaf = cast_to_leaf(node);
    unsigned i, nkeys = leaf->nkeys;
    bool found = true;

    ASSERT(node_locked_p(node));

    /* If full - need a split. */
    if (nkeys == NODE_MAX) {
        return false;
    }

    /* To avoid that unpersisted kv will be seen by other read operations. */
    node->version |= NODE_INSERTING;
    atomic_thread_fence(memory_order_release);

    i = leaf_find_lv(leaf, key, &found);

    // simulate the 64B-value-persist latency.
    emulate_latency_ns(EXTRA_SCM_LATENCY);

    if (found) {
        int slot = find_first_zero(leaf);
        leaf->slot_kv[slot].key = key;
        leaf->slot_kv[slot].val = val;
        //yfad
    // fprintf(stderr,"found \n");
        // (*ret) = leaf->slot_kv[slot].val;

        pmem_persist(&leaf->slot_kv[slot], sizeof(ln_kv), sizeof(ln_kv));

        set_hash_fp(leaf->fingerprints[slot], key);
        pmem_persist(&leaf->fingerprints[slot], 8, 1);

        uint64_t tmp_bitmap = leaf->bitmap;
        reset_bitmap(tmp_bitmap, i);
        set_bitmap(tmp_bitmap, slot);
        leaf->bitmap = tmp_bitmap;
        pmem_persist(&leaf->bitmap, 8, 8);
        return true;
    }
    // fprintf(stderr,"notfound \n");
    //yfad
    // (*ret).size=-1;

    int slot = find_first_zero(leaf);
    leaf->slot_kv[slot].key = key;
    leaf->slot_kv[slot].val = val;
    pmem_persist(&leaf->slot_kv[slot], sizeof(ln_kv), sizeof(ln_kv));

    set_hash_fp(leaf->fingerprints[slot], key);
    pmem_persist(&leaf->fingerprints[slot], 8, 1);
    set_bitmap(leaf->bitmap, slot);
    pmem_persist(&leaf->bitmap, 8, 1);

    leaf->nkeys++;
    return true;
}

static bool leaf_remove_key(fptree_node_t *node, setkey_t key) {
    fptree_leaf_t *leaf = cast_to_leaf(node);
    unsigned i, nkeys = leaf->nkeys;
    bool found = true;

    ASSERT(nkeys > 0);
    ASSERT(node_locked_p(node));
    NOSMP_ASSERT(validate_inode(inode));

    /* Find the position and move the right-hand side. */
    int slot = leaf_find_lv(leaf, key, &found);

    node->version |= NODE_INSERTING;
    atomic_thread_fence(memory_order_release);

    reset_bitmap(leaf->bitmap, slot);
    pmem_persist(&leaf->bitmap, 8, 1);
    leaf->nkeys--;

    NOSMP_ASSERT(validate_inode(leaf));
    return (nkeys - 1) == 0;
}

/*
 * Primitives to manage the interior nodes of the B+ tree.
 */
static fptree_inode_t *internode_create(ptst_t *ptst) {
    fptree_inode_t *node = (fptree_inode_t *)smart_alloc(ptst, 1);
    memset(node, 0, sizeof(fptree_inode_t));
    return node;
}

static inline fptree_node_t *internode_lookup(fptree_node_t *node,
                                              setkey_t key) {
    fptree_inode_t *inode = cast_to_inode(node);
    unsigned i, nkeys = inode->nkeys;

    NOSMP_ASSERT(validate_inode(inode));

    for (i = 0; i < nkeys; i++)
        if (key < inode->slot_kv[i].key) break;

    ASSERT(i < (NODE_MAX + 1));
    return inode->slot_kv[i].child;
}

static void internode_insert(ptst_t *ptst, fptree_node_t *node, setkey_t key,
                             fptree_node_t *child) {
    fptree_inode_t *inode = cast_to_inode(node);
    unsigned i, j, nkeys = inode->nkeys;

    ASSERT(nkeys < NODE_MAX);
    ASSERT(node_locked_p(node));
    ASSERT(node_locked_p(child));
    ASSERT(node->version & (NODE_INSERTING | NODE_SPLITTING));
    NOSMP_ASSERT(validate_inode(inode));

    /* Find the position and move the right-hand side. */
    for (i = 0; i < nkeys; i++)
        if (key < inode->slot_kv[i].key) break;

    if (i != nkeys) {
        const unsigned klen = (nkeys - i);
        const unsigned clen = (nkeys - i + 1);
        for (j = klen; j > 0; j--)
            inode->slot_kv[i + j].key = inode->slot_kv[i + j - 1].key;
        for (j = clen; j > 0; j--)
            inode->slot_kv[i + j].child = inode->slot_kv[i + j - 1].child;
    }

    /* Insert the new key and the child. */
    inode->slot_kv[i].key = key;
    inode->slot_kv[i + 1].child = child;
    node_set_parent(child, inode);
    atomic_thread_fence(memory_order_release);

    inode->nkeys++;
    NOSMP_ASSERT(validate_inode(inode));
}

static fptree_node_t *internode_remove(fptree_node_t *node, setkey_t key) {
    fptree_inode_t *inode = cast_to_inode(node);
    unsigned i, j, nkeys = inode->nkeys;

    ASSERT(nkeys > 0);
    ASSERT(node_locked_p(node));
    ASSERT(node->version & NODE_INSERTING);
    NOSMP_ASSERT(validate_inode(inode));

    /*
     * Removing the last key - determine the stray leaf and
     * return its pointer for the rotation.
     */
    if (inode->nkeys == 1) {
        i = (key < inode->slot_kv[0].key);
        return inode->slot_kv[i].child;
    }

    /* Find the position and move the right-hand side. */
    for (i = 0; i < nkeys; i++)
        if (key < inode->slot_kv[i].key) break;

    if (i != nkeys) {
        const unsigned klen = (nkeys - i - 1);
        const unsigned clen = (nkeys - i);
        for (j = 0; j < klen; j++)
            inode->slot_kv[i + j].key = inode->slot_kv[i + j + 1].key;
        for (j = 0; j < clen; j++)
            inode->slot_kv[i + j].child = inode->slot_kv[i + j + 1].child;
    }
    inode->nkeys--;

    NOSMP_ASSERT(validate_inode(inode));
    return NULL;
}

/*
 * Split of the interior node.
 *
 * => Inserts the child into the correct node.
 * => Returns the right (new) node; the parent node is left.
 * => Returns the "middle key" for the creation of a new parent.
 */
static fptree_node_t *split_inter_node(ptst_t *ptst, fptree_node_t *parent,
                                       setkey_t ckey, fptree_node_t *nchild,
                                       setkey_t *midkey) {
    fptree_inode_t *lnode = cast_to_inode(parent);
    fptree_inode_t *rnode = internode_create(ptst);
    const unsigned s = NODE_PIVOT + 1, c = NODE_MAX - s;

    ASSERT(node_locked_p(parent));
    ASSERT(node_locked_p(nchild));
    ASSERT(lnode->nkeys == NODE_MAX);

    *midkey = lnode->slot_kv[NODE_PIVOT].key;
    rnode->version = NODE_LOCKED | NODE_SPLITTING;
    rnode->parent = lnode->parent;

    /*
     * Copy all keys after the pivot to the right-node.  The pivot
     * will be removed and passed the upper level as a middle key.
     */
    for (unsigned i = 0; i < c; i++)
        rnode->slot_kv[i].key = lnode->slot_kv[s + i].key;
    for (unsigned i = 0; i <= c; i++) {
        rnode->slot_kv[i].child = lnode->slot_kv[s + i].child;
        node_set_parent(rnode->slot_kv[i].child, rnode);
    }
    rnode->nkeys = c;

    /*
     * Mark the left node as "dirty" and actually move the keys.
     * Note the extra decrement in order to remove the pivot.
     */
    lnode->version |= NODE_SPLITTING;
    atomic_thread_fence(memory_order_release);
    lnode->nkeys = s - 1;

    /* Insert the child into the correct parent. */
    const bool toleft = ckey < *midkey;
    fptree_node_t *pnode = (fptree_node_t *)(toleft ? lnode : rnode);

    internode_insert(ptst, pnode, ckey, nchild);

    NOSMP_ASSERT(validate_inode(lnode));
    NOSMP_ASSERT(validate_inode(rnode));

    return (fptree_node_t *)rnode;
}

/*
 * split_leaf_node: split the leaf node and insert the given key slice.
 *
 * => If necessary, performs the splits up-tree.
 * => If the root node is reached, sets a new root for the tree.
 */
static void split_leaf_node(ptst_t *ptst, fptree_t *tree, fptree_node_t *node,
                            setkey_t key, setval_t val) {
    fptree_leaf_t *leaf = cast_to_leaf(node), *nleaf;
    fptree_node_t *nnode, *parent;
    setkey_t nkey;

    ASSERT(node_locked_p(node));
    ASSERT(node->nkeys == NODE_MAX);
    const unsigned s = NODE_PIVOT + 1, c = NODE_MAX - s;

    PCurrentLeaf = leaf;
    pmem_persist(&leaf, 8, 8);

    /*
     * Create a new leaf and split the keys amongst the nodes.
     * Attention: we split *only* to-the-right in order to ease the concurrent
     * lookups.
     */
    nleaf = leaf_create(ptst);
    nnode = (fptree_node_t *)nleaf;
    nleaf->version |= NODE_LOCKED;
    PNewLeaf = nleaf;
    pmem_persist(&PNewLeaf, 8, 8);

    /* Copy all keys after the pivot to the right-node. */
    bool flag[NODE_MAX];
    memset(flag, 0, sizeof(flag));
    for (unsigned i = 0; i < c; i++) {
        int slot = -1;
        for (unsigned j = 0; j < NODE_MAX; j++)
            if (!flag[j] && check_bitmap(leaf->bitmap, j) &&
                (slot == -1 ||
                 leaf->slot_kv[j].key > leaf->slot_kv[slot].key)) {
                slot = j;
            }
        if (i == c - 1) nkey = leaf->slot_kv[slot].key;
        std::copy(leaf->slot_kv + slot, leaf->slot_kv + slot + 1,
                  nleaf->slot_kv + slot);
        nleaf->fingerprints[slot] = leaf->fingerprints[slot];
        set_bitmap(nleaf->bitmap, slot);
        flag[slot] = true;
    }

    nleaf->nkeys = c;
    nleaf->version |= NODE_SPLITTING;

    /*
         * Notes on updating the list pointers:
         *
         * - Right-leaf (the new one) gets 'prev' and 'next' pointers set
         *   since both of the nodes are locked.
         *
         * - The 'parent' of the right-leaf will be set upon its insertion
         *   to the internode; only the splits use this pointer.
         *
         * - The left-leaf is locked and its 'next' pointer can be set
         *   once the right-leaf is ready to be visible.
         *
         * - The 'prev' pointer of the leaf which is right to the
         *   right-leaf can also be updated since the original previous
         *   leaf is locked.
         */
    if ((nleaf->next = leaf->next) != NULL) {
        fptree_leaf_t *next = nleaf->next;
        next->prev = nleaf;
    }
    nleaf->prev = leaf;
    nleaf->parent = leaf->parent;
    pmem_persist(&nleaf, sizeof(fptree_leaf_t), sizeof(fptree_leaf_t));

    /*
     * Mark the left node as "dirty" and actually move the keys.
     * Note the extra decrement in order to remove the pivot.
     */
    leaf->version |= NODE_SPLITTING;
    atomic_thread_fence(memory_order_release);
    leaf->nkeys = s;

    /* Only persist the bitmap and next arr[]. */
    leaf->bitmap = ~(nleaf->bitmap);
    pmem_persist(&leaf->bitmap, 8, 8);

    bool toright = key_geq(nleaf, key);
    //yfad
    // setval_t tmp;
    leaf_insert_key(toright ? nnode : node, key, val);

    leaf->next = nleaf;
    pmem_persist(&leaf->next, 8, 8);

    // Reset uLog
    PNewLeaf = NULL;
    pmem_persist(&PNewLeaf, 8, 8);
    PCurrentLeaf = NULL;
    pmem_persist(&PCurrentLeaf, 8, 8);

    /*
         * Done with the leaves - any further ascending would be on the
         * internodes (invalidate the pointers merely for diagnostics).
         *
         * Both nodes locked; acquire the lock on parent node.
         */
    leaf = nleaf = (fptree_leaf_t *)0xdeadbeef;
ascend:
    if ((parent = lock_parent_node(node)) == NULL) {
        /*
         * We have reached the root.  Create a new interior
         * node which will be a new root.
         */
        fptree_inode_t *pnode = internode_create(ptst);

        /* Initialise, set two children and the middle key. */
        pnode->version = NODE_LOCKED | NODE_INSERTING | NODE_ISROOT;
        pnode->slot_kv[0].key = nkey;
        pnode->slot_kv[0].child = node;
        pnode->slot_kv[1].child = nnode;
        pnode->nkeys = 1;
        atomic_thread_fence(memory_order_release);

        ASSERT(node->version & (NODE_SPLITTING | NODE_INSERTING));
        // XXX ASSERT(node->version & NODE_ISROOT);
        ASSERT(node_get_parent(node) == NULL);
        ASSERT(node_get_parent(nnode) == NULL);

        /*
         * Long live new root!  Note: the top-root pointer is
         * protected by the node lock.
         */
        node_set_parent(nnode, pnode);
        node_set_parent(node, pnode);
        parent = (fptree_node_t *)pnode;

        if (tree->root == node) {
            tree->root = parent;
        }
        NOSMP_ASSERT(validate_inode(pnode));

        /* Release the locks.  Unlock will clear NODE_ISROOT. */
        unlock_node(parent);
        unlock_node(nnode);
        unlock_node(node);
        return;
    }
    ASSERT(node_locked_p(parent));
    NOSMP_ASSERT(validate_inode(cast_to_inode(parent)));

    if (__predict_false(((fptree_inode_t *)parent)->nkeys == NODE_MAX)) {
        fptree_node_t *inode;

        /*
         * The parent node is full - split and ascend.  We can
         * release the lock of the already existing child.
         */
        unlock_node(node);
        inode = split_inter_node(ptst, parent, nkey, nnode, &nkey);
        unlock_node(nnode);

        ASSERT(node_locked_p(parent));
        ASSERT(node_locked_p(inode));

        node = parent;
        nnode = inode;

        fptree_inode_t *temp = ((fptree_inode_t *)node)->parent;
        fptree_inode_t *temp2 = ((fptree_inode_t *)inode)->parent;
        goto ascend;
    }

    /*
     * The parent node is not full: mark the parent as "dirty"
     * and then insert the new node into our parent.
     */
    parent->version |= NODE_INSERTING;
    unlock_node(node);  // memory_order_release
    internode_insert(ptst, parent, nkey, nnode);

    ASSERT(node_get_parent(nnode) == parent);
    ASSERT(node_get_parent(node) == parent);
    unlock_node(nnode);
    unlock_node(parent);
}

/*
 * collapse_nodes: collapse the intermediate nodes and indicate whether
 * the the upper layer needs cleanup/fixup (true) or not (false).
 */
static bool collapse_nodes(ptst_t *ptst, fptree_t *tree, fptree_node_t *node,
                           setkey_t key) {
    fptree_node_t *parent, *child = NULL;
    bool toproot;

    ASSERT(node->version & NODE_DELETED);
    ASSERT(tree->root != node);

    /*
     * Lock the parent.  If there is no parent, then the leaf is the
     * root of a layer (but not the top layer).  Set the layer deletion
     * flag and indicate that the upper layer needs a cleanup.
     */
    if ((parent = lock_parent_node(node)) == NULL) {
        // ASSERT(node->version & NODE_ISROOT);
        node->version = (node->version & ~NODE_DELETED);
        // smart_free(ptst, node, 0);
        return true;
    }
    smart_free(ptst, node, 0);

    /* Fail the readers by pretending the insertion. */
    ASSERT((parent->version & NODE_DELETED) == 0);
    parent->version |= NODE_INSERTING;
    atomic_thread_fence(memory_order_release);

    /* Remove the key from the parent node. */
    if ((child = internode_remove(parent, key)) == NULL) {
        /* Done (no further collapsing). */
        unlock_node(parent);
        return false;
    }
    ASSERT(child != node);

    /*
     * It was the last key, therefore rotate the tree: delete the
     * internode and assign its child to the new parent.
     */
    parent->version |= NODE_DELETED;
    node = parent;

    if ((parent = lock_parent_node(node)) != NULL) {
        fptree_inode_t *pnode = cast_to_inode(parent);
        unsigned i;

        NOSMP_ASSERT(validate_inode(pnode));

        /* Assign the child, set its parent pointer. */
        for (i = 0; i < pnode->nkeys; i++)
            if (key < pnode->slot_kv[i].key) break;
        ASSERT(pnode->slot_kv[i].child == node);
        pnode->slot_kv[i].child = child;
        node_set_parent(child, pnode);
        smart_free(ptst, node, 1);

        NOSMP_ASSERT(validate_inode(pnode));
        unlock_node(parent);
        return false;
    }

    /*
     * No parent: the child must become the new root.
     *
     * - The deleted internode, however, is still being the root of
     *   the layer; clear the NODE_ISROOT pointer and set the parent
     *   pointer to child, so the readers would retry from there.
     *
     * - Set the child's parent pointer to NULL as its parent has
     *   just been marked as deleted.  At this point, concurrent
     *   split or deletion of the child itself may happen.
     */
    // ASSERT(node->version & NODE_ISROOT);
    node->version = (node->version & ~NODE_ISROOT) | NODE_DELETED;
    node_set_parent(node, (fptree_inode_t *)child);
    toproot = (tree->root == node);
    if (toproot) {
        tree->root = child;
    }
    atomic_thread_fence(memory_order_release);
    node_set_parent(child, NULL);
    smart_free(ptst, node, 1);

    /* Indicate that the upper layer needs a clean up. */
    return !toproot;
}

/*
 * delete_leaf_node: remove the leaf and add it for G/C, if necessary
 * triggering the layer collapse.
 *
 * => Return true if the upper layer needs a cleanup.
 */
static inline void delete_leaf_node(ptst_t *ptst, fptree_t *tree,
                                    fptree_node_t *node, setkey_t key) {
    fptree_leaf_t *leaf = cast_to_leaf(node);
    fptree_node_t *prev, *next;

    ASSERT(node_locked_p(node));
    ASSERT((node->version & (NODE_INSERTING | NODE_SPLITTING)) == 0);

    NOSMP_ASSERT(validate_leaf(leaf));
    NOSMP_ASSERT(!leaf->prev || validate_leaf(leaf->prev));
    NOSMP_ASSERT(!leaf->next || validate_leaf(leaf->next));
    NOSMP_ASSERT(!leaf->parent || validate_inode(leaf->parent));

    /*
     * If this is the top level leaf, then we merely keep it empty.
     */
    if (tree->root == node) {
        ASSERT(node_get_parent(node) == NULL);
        // ASSERT(node->version & NODE_ISROOT);
        unlock_node(node);
        return;
    }

    /*
     * Unlink the leaf from the doubly-linked list.
     *
     * First, we must lock the next leaf.  Then, since the node is
     * empty, mark as deleted.  Any readers will fail and retry from
     * the top at this point.
     */
    while ((next = (fptree_node_t *)leaf->next) != NULL) {
        lock_node(next);
        if ((next->version & NODE_DELETED) == 0) {
            break;
        }
        /* Race: our 'next' prointer should be updated. */
        unlock_node(next);
    }
    node->version |= NODE_DELETED;
    atomic_thread_fence(memory_order_release);

    /*
     * Get a stable version of the previous node and attempt to
     * perform CAS on its 'next' pointer.  If the CAS failed or
     * the version has changed - retry.
     */
    while ((prev = (fptree_node_t *)leaf->prev) != NULL) {
        fptree_leaf_t *prevl = cast_to_leaf(prev);
        uint32_t v;
        bool ok;

        v = stable_version(prev);
        ok = prevl->next == (fptree_leaf_t *)next ||
             atomic_compare_exchange_weak(&prevl->next, node, next);
        if (ok && (prev->version ^ v) <= NODE_LOCKED) {
            /* Only needs to persist the next pointer. */
            pmem_persist(&prevl->next, 8, 8);
            break;
        }
    }
    if (next) {
        fptree_leaf_t *nextl = cast_to_leaf(next);
        nextl->prev = leaf->prev;
        unlock_node(next);
    }

    NOSMP_ASSERT(!leaf->prev || validate_leaf(leaf->prev));
    NOSMP_ASSERT(!leaf->next || validate_leaf(leaf->next));

    /*
     * Collapse the intermediate nodes (note: releases the leaf lock).
     */
    collapse_nodes(ptst, tree, node, key);
}

/*
 * find_leaf: given the partial key, traverse the tree to find a leaf.
 *
 * => The traversing is done without acquiring any locks (READER).
 * => The closest matching leaf and its stable version are returned.
 * => If the root is changed, the func will walk to the current root.
 */
static fptree_leaf_t *find_leaf(fptree_node_t *root, setkey_t key,
                                uint32_t *rv) {
    fptree_node_t *node;
    uint32_t v;
retry:
    node = root;
    v = stable_version(node);

    /* Handle stale roots which can occur due to splits. */
    if (__predict_false((v & NODE_ISROOT) == 0)) {
        root = node = walk_to_root(node);
        v = stable_version(node);
    }

    /*
     * Traverse the tree validating the captured child pointers on
     * every step ("hand-over-hand validation", see p. 7 of the paper).
     */
    while ((v & NODE_ISBORDER) == 0) {
        fptree_node_t *cnode;
        uint32_t cv, nv;

        /* Fetch the child node and get its state. */
        cnode = internode_lookup(node, key);
        cv = stable_version(cnode);  // memory_order_acquire

        /*
         * Check that the version has not changed.  Somebody may
         * hold a lock, but we can proceed as long as the node is
         * not marked as "dirty".
         */
        if (__predict_true((node->version ^ v) <= NODE_LOCKED)) {
            /* Good - keep descending. */
            node = cnode;
            v = cv;
            continue;
        }

        /*
         * If the split was performed - the hierarchy might have
         * been disrupted and we have to retry from the root.
         */
        nv = stable_version(node);
        if (__predict_false((nv & NODE_VSPLIT) != (v & NODE_VSPLIT)))
            goto retry;

        /* The node was modified - retry. */
        v = nv;
    }
    NOSMP_ASSERT(validate_leaf(cast_to_leaf(node)));

    *rv = v;
    return cast_to_leaf(node);
}

static fptree_leaf_t *walk_leaves(fptree_leaf_t *leaf, setkey_t key,
                                  uint32_t *vp) {
    fptree_leaf_t *next;
    uint32_t v = *vp;

    /*
     * Walk the leaves (i.e. iterate from left to right until we
     * find the matching one) because of a version change.  This
     * logic relies on a key invariant of the FPTree that the
     * nodes split *only* to-the-right, therefore such iteration
     * is reliable.
     *
     * Note: we check the current leaf first.
     */
    v = stable_version((fptree_node_t *)leaf);
    next = leaf->next;

    /* Compare with the lowest key of the next leaf. */
    while ((v & NODE_DELETED) == 0 && next && key_geq(next, key)) {
        v = stable_version((fptree_node_t *)next);
        leaf = next, next = leaf->next;
    }
    *vp = v;

    /*
     * At this point we either found our border leaf and have its
     * stable version or hit a deleted leaf.
     */
    return leaf;
}

static fptree_leaf_t *find_leaf_locked(fptree_node_t *root, setkey_t key) {
    fptree_leaf_t *leaf;
    uint32_t v, nvc;

/*
 * Perform the same lookup logic as in fptree_get(), but lock
 * the leaf once found and just re-lock if walking the leaves.
 */
retry:
    leaf = find_leaf(root, key, &v);
forward:
    if (__predict_false(v & NODE_DELETED)) {
        goto retry;
    }

    /*
     * Lock!  Check the split counter and re-check the delete flag.
     * Note that lock_node() issues a read memory barrier for us.
     */
    lock_node((fptree_node_t *)leaf);  // memory_order_release
    nvc = leaf->version & (NODE_VSPLIT | NODE_DELETED);
    if (__predict_false(nvc != (v & NODE_VSPLIT))) {
        unlock_node((fptree_node_t *)leaf);
        leaf = walk_leaves(leaf, key, &v);
        goto forward;
    }
    return leaf;
}

/*
 * fptree_get: fetch a value given the key.
 */
// void *fptree_get(fptree_t *tree, setkey_t key) {
bool fptree_get(fptree_t *tree, setkey_t key) {
    fptree_node_t *root = tree->root;
    unsigned idx;
    fptree_leaf_t *leaf;
    uint32_t v;
    // void *lv;
    setval_t lv;
    bool found = true;
    ptst_t *ptst = NULL;

    key = CALLER_TO_INTERNAL_KEY(key);
    //ptst = critical_enter();


retry:
    /* Find the leaf given the slice-key. */
    leaf = find_leaf(root, key, &v);
forward:
    if (__predict_false(v & NODE_DELETED)) {
        /* Collided with deletion - try again from the root. */
        goto retry;
    }

    /* Fetch the value (or pointer to the next layer). */
    idx = leaf_find_lv(leaf, key, &found);
    lv = leaf->slot_kv[idx].val;
    atomic_thread_fence(memory_order_acquire);

    /* Check that the version has not changed. */
    if (__predict_false((leaf->version ^ v) > NODE_LOCKED)) {
        leaf = walk_leaves(leaf, key, &v);
        goto forward;
    }

    //critical_exit(ptst);

    return found;
    // if (found) return lv;
    // return NULL;
}

/*
 * fptree_put: store a value given the key.
 *
 * => Returns true if the new entry was created or the existing entry was
 *modified.
 */
bool fptree_put(fptree_t *tree, setkey_t key, setval_t val) {
    fptree_node_t *root = tree->root, *node;
    fptree_leaf_t *leaf;
    ptst_t *ptst = NULL;
    //printf("pass line 1330\n");
    key = CALLER_TO_INTERNAL_KEY(key);

    //ptst = critical_enter();

retry:
    /* Lookup the leaf and lock it (returns stable version). */
    leaf = find_leaf_locked(root, key);
    if (__predict_false(leaf == NULL)) {
        root = tree->root;
        goto retry;
    }
    //printf("pass line 1342\n");
    node = (fptree_node_t *)leaf;
    NOSMP_ASSERT(!leaf->parent || validate_inode(leaf->parent));
    ASSERT(node_locked_p(node));
    //printf("pass line 1346\n");
    /* The key was not found: insert it. */
    if (!leaf_insert_key(node, key, val)) {
        /* The node is full: perform the split processing. */
        split_leaf_node(ptst, tree, node, key, val);
    } else {
        unlock_node(node);
    }

    //critical_exit(ptst);

    return true;
}

/*
 * fptree_del: remove they entry given the key.
 *
 * => Returns true if the key was removed; false if not found.
 */
bool fptree_del(fptree_t *tree, setkey_t key) {
    fptree_node_t *root = tree->root, *node;
    unsigned idx;
    fptree_leaf_t *leaf;
    bool found = true;
    ptst_t *ptst = NULL;

    key = CALLER_TO_INTERNAL_KEY(key);

    //ptst = critical_enter();

retry:
    /* Lookup the leaf and lock it (returns stable version). */
    leaf = find_leaf_locked(root, key);
    if (__predict_false(leaf == NULL)) {
        root = tree->root;
        goto retry;
    }
    idx = leaf_find_lv(leaf, key, &found);
    node = (fptree_node_t *)leaf;
    NOSMP_ASSERT(!leaf->parent || validate_inode(leaf->parent));

    if (found) {
        /* The key was found: delete it. */
        if (!leaf_remove_key(node, key)) {
            unlock_node(node);

            //critical_exit(ptst);

        } else {
            NOSMP_ASSERT(!leaf->parent || validate_inode(leaf->parent));
            /* It was the last key: deleting the whole leaf. */
            delete_leaf_node(ptst, tree, node, key);
        }
        return true;
    } else {
        /*
         * Not found: nothing to do, just unlock and return.
         * Note: cannot be MTREE_UNSTABLE as we acquired the lock.
         */
        unlock_node(node);
    }

    //critical_exit(ptst);

    return false;
}

fptree_t *fptree_create() {
    fptree_t *tree;
    fptree_node_t *root;
    ptst_t *ptst = NULL;

    //ptst = critical_enter();


    tree = (fptree_t *)malloc(sizeof(fptree_t));
    memset(tree, 0, sizeof(fptree_t));

    root = (fptree_node_t *)&tree->initleaf;
    root->version = NODE_ISROOT | NODE_ISBORDER;
    tree->root = root;
    atomic_thread_fence(memory_order_release);

    //critical_exit(ptst);

    return tree;
}

void fptree_destroy(fptree_t *tree) {
    fptree_leaf_t *root = cast_to_leaf(tree->root);
    ptst_t *ptst = NULL;

    //ptst = critical_enter();

    /* Finally, free fptree. */
    if (&tree->initleaf != root) {
        smart_free(ptst, root, 0);
    }
    free(tree);

    //critical_exit(ptst);

}

#define MEM_FAIL(_s)                                                         \
do {                                                                         \
    fprintf(stderr, "OUT OF MEMORY: %d bytes at line %d\n", (_s), __LINE__); \
    exit(1);                                                                 \
} while ( 0 )

/* Allocate more empty chunks from the heap. */
#define CHUNKS_PER_ALLOC 1000
static chunk_t *alloc_more_chunks(void)
{
    int i;
    chunk_t *h, *p;

    h = p = (chunk_t*)ALIGNED_ALLOC(CHUNKS_PER_ALLOC * sizeof(*h));
    if ( h == NULL ) MEM_FAIL(CHUNKS_PER_ALLOC * sizeof(*h));

    for ( i = 1; i < CHUNKS_PER_ALLOC; i++ )
    {
        p->next = p + 1;
        p++;
    }

    p->next = h;
    return(h);
}

/* Put a chain of chunks onto a list. */
static void add_chunks_to_list(chunk_t *ch, chunk_t *head)
{
    chunk_t *h_next, *new_h_next, *ch_next;
    ch_next    = ch->next;
    new_h_next = head->next;
    do { ch->next = h_next = new_h_next; WMB_NEAR_CAS(); }
    while ( (new_h_next = CASPO(&head->next, h_next, ch_next)) != h_next );
}

/* Allocate a chain of @n empty chunks. Pointers may be garbage. */
static chunk_t *get_empty_chunks(int n)
{
    int i;
    chunk_t *new_rh, *rh, *rt, *head;
    printf("pass line 1494\n");
 retry:
    head = gc_global.free_chunks;
    new_rh = head->next;
    do {
        rh = new_rh;
        rt = head;
        WEAK_DEP_ORDER_RMB();
        for ( i = 0; i < n; i++ )
        {
            if ( (rt = rt->next) == head )
            {
                /* Allocate some more chunks. */
                printf("pass line 1507\n");
                add_chunks_to_list(alloc_more_chunks(), head);
                printf("pass line 1509\n");
                goto retry;
            }
        }
    }
    while ( (new_rh = CASPO(&head->next, rh, rt->next)) != rh );

    rt->next = rh;
    return(rh);
}

/* Get @n filled chunks, pointing at blocks of @sz bytes each. */
static chunk_t *get_filled_chunks(int n, int sz)
{
    chunk_t *h, *p;
    char *node;
    int i;

#ifdef PROFILE_GC
    ADD_TO(gc_global.total_size, n * BLKS_PER_CHUNK * sz);
    ADD_TO(gc_global.allocations, 1);
#endif
    printf("pass line 1528\n");
    node = (char*)ALIGNED_ALLOC(n * BLKS_PER_CHUNK * sz);
    if ( node == NULL ) MEM_FAIL(n * BLKS_PER_CHUNK * sz);
#ifdef WEAK_MEM_ORDER
    INITIALISE_NODES(node, n * BLKS_PER_CHUNK * sz);
#endif
    printf("pass line 1534\n");
    h = p = get_empty_chunks(n);
    printf("pass line 1536\n");
    do {
        p->i = BLKS_PER_CHUNK;
        for ( i = 0; i < BLKS_PER_CHUNK; i++ )
        {
            p->blk[i] = node;
            node += sz;
        }
    }
    while ( (p = p->next) != h );
    return(h);
}

int gc_add_allocator(int alloc_size)
{
    int ni, i = gc_global.nr_sizes;
    printf("pass line 1552\n");
    while ( (ni = CASIO(&gc_global.nr_sizes, i, i+1)) != i ) i = ni;
    printf("pass line 1554\n");
    gc_global.blk_sizes[i]  = alloc_size;
    gc_global.alloc_size[i] = ALLOC_CHUNKS_PER_LIST;
    printf("pass line 1557\n");
    gc_global.alloc[i] = get_filled_chunks(ALLOC_CHUNKS_PER_LIST, alloc_size);
    return i;
}

void _init_set_subsystem(void) {
    printf("pass line 1563\n");
    gc_id[0] = gc_add_allocator(sizeof(fptree_leaf_t));
    gc_size[0] = sizeof(fptree_leaf_t);
    printf("pass line 1566\n");
    gc_id[1] = gc_add_allocator(sizeof(fptree_inode_t));
    gc_size[1] = sizeof(fptree_inode_t);
}


#endif /* __FACE_H__ */
