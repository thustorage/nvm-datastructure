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

#include <libpmemobj.h>
#include <sys/stat.h>
#include "utils.h"

extern "C" {
#include "gc/portable_defns.h"
#include "gc/ptst.h"
#include "gc/set.h"
#include "atomic_ops/atomic_ops.h"
}


#define FP_KEY 8
#define FP_VALUE 8

typedef uint64_t setkey_t;
typedef void *setval_t;

static int gc_id[2];
static int gc_size[2];

const uint64_t SPACE_PER_THREAD = 6ULL * 1024ULL * 1024ULL * 1024ULL;
const uint64_t SPACE_OF_MAIN_THREAD = 6ULL * 1024ULL * 1024ULL * 1024ULL;
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


/*
 * The helper functions - some primitives for persist operations.
 */
#define FLUSH_ALIGN ((uintptr_t)64)
#define HAS_SCM_LATENCY
#define NS2CYCLE(__ns) ((__ns) * M_PCM_CPUFREQ / 2000)
#define CYCLE2NS(__cycles) ((__cycles) * 1000 / M_PCM_CPUFREQ)




/* Used for avoiding memory leak when split or merge operations */
extern __thread fptree_leaf_t *PCurrentLeaf;
extern __thread fptree_leaf_t *PNewLeaf;
extern __thread fptree_leaf_t *PPrevLeaf;


void _init_set_subsystem(void);

bool fptree_get(fptree_t *tree, setkey_t key);

bool fptree_put(fptree_t *tree, setkey_t key, setval_t val);
fptree_t * fptree_create();

bool fptree_del(fptree_t *tree, setkey_t key);

int file_exists(const char *filename);

void openPmemobjPool(char *pathname);

#endif /* __FACE_H__ */
