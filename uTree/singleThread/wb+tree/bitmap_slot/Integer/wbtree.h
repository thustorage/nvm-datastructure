/*
 * Configuration: M_PCM_CPUFREQ/EXTRA_SCM_LATENCY/NODE_SIZE/  LOG_DATA_SIZE+LOG_AREA_SIZE ? / 
 * Note: 1. space management; 2. 
 */

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

#define mfence() asm volatile("mfence":::"memory")
#define BITOP_WORD(nr)	((nr) / BITS_PER_LONG)

unsigned long node_count = 0;
unsigned long mfence_count = 0;

/****************************************************************************************************************************/
#define NODE_SIZE 			63

#define SLOT_SIZE 			NODE_SIZE + 1
#define MIN_LIVE_ENTRIES 	NODE_SIZE / 2
#define CACHE_LINE_SIZE 	64

#define LOG_DATA_SIZE		48
#define LOG_AREA_SIZE		4194304
#define LE_DATA			0
#define LE_COMMIT		1

#define BITS_PER_LONG	64
#define BITMAP_SIZE		NODE_SIZE + 1

const uint64_t SPACE_PER_THREAD = 16ULL * 1024ULL * 1024ULL * 1024ULL;
extern char *start_addr;
extern char *curr_addr;

typedef struct entry entry;
typedef struct node node;
typedef struct tree tree;

extern unsigned long node_count;
extern unsigned long clflush_count;
extern unsigned long mfence_count;

typedef struct {
	unsigned int size;
	unsigned char type;
	void *addr;
	char data[LOG_DATA_SIZE];
} log_entry;

typedef struct {
	log_entry *next_offset;
	char log_data[LOG_AREA_SIZE];
} log_area;

struct entry{
	unsigned long key;
	void *ptr;
};

struct node{
	char slot[SLOT_SIZE];
	unsigned long bitmap;
	struct entry entries[NODE_SIZE];
	struct node *leftmostPtr;
	struct node *parent;
	int isleaf;
//	char dummy[32];		//15
//	char dummy[16];		//31
	char dummy[48];		//63
};

POBJ_LAYOUT_BEGIN(btree);
POBJ_LAYOUT_TOID(btree, node);
POBJ_LAYOUT_END(btree);
PMEMobjpool *pop;

struct tree{
	node *root;
	log_area *start_log;
};

void flush_buffer_nocount(void *buf, unsigned long len, bool fence);
tree *initTree();
void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[]);
void *Lookup(tree *t, unsigned long key);
int Append(node *n, unsigned long key, void *value);
int Append_in_inner(node *n, unsigned long key, void *value);
int Search(node *curr, char *temp, unsigned long key);
node *find_leaf_node(node *curr, unsigned long key);
void Insert(tree *t, unsigned long key, void *value);
void *Update(tree *t, unsigned long key, void *value);
int insert_in_leaf_noflush(node *curr, unsigned long key, void *value);
void insert_in_leaf(node *curr, unsigned long key, void *value);
void insert_in_inner(node *curr, unsigned long key, void *value);
void insert_in_parent(tree *t, node *curr, unsigned long key, node *splitNode);
void printNode(node *n);
int Delete(tree *t, unsigned long key);

static inline unsigned long ffz(unsigned long word)
{
	asm("rep; bsf %1,%0"
		: "=r" (word)
		: "r" (~word));
	return word;
}

/****************************************************************************************************************************/
#define STATICS_EVALUATION 
#ifdef STATICS_EVALUATION
    uint64_t latency_start, latency_end;
    uint64_t flush_sum, wear_byte_sum, flush_start, wear_byte_start;
    uint64_t sort_latency, unsort_latency, balance_latency, others_latency, insert_latency;
    uint64_t sort_flush, unsort_flush, balance_flush, others_flush, insert_flush;
    uint64_t sort_wear, unsort_wear, balance_wear, others_wear, insert_wear;
#endif
//#define STATICS_MALLOC
#ifdef STATICS_MALLOC
    uint64_t pmalloc_sum, pfree_sum, vmalloc_sum, vfree_sum; 
    uint64_t scm_size, dram_size;
    uint64_t value_size = 64;
#endif

#define HAS_SCM_LATENCY
#define M_PCM_CPUFREQ 2500
#define EXTRA_SCM_LATENCY 500
#define NS2CYCLE(__ns) ((__ns) * M_PCM_CPUFREQ / 1000)
#define CYCLE2NS(__cycles) ((__cycles) * 1000 / M_PCM_CPUFREQ)
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

static inline void asm_clflush(volatile uint64_t *addr)
{
    asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(addr)));
}

static inline void cpu_pause()
{
	__asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void)
{
	unsigned long var;
	unsigned int hi, lo;

	asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
	var = ((unsigned long long int) hi << 32) | lo;

	return var;
}

void flush_buffer(void *buf, unsigned long len, bool fence)
{
	unsigned long i, etsc;
#ifdef STATICS_EVALUATION
    wear_byte_sum += len;
#endif
	len = len + ((unsigned long)(buf) & (CACHE_LINE_SIZE - 1));
    fence = true; //hqd add
	if (fence) {
		mfence();
		for (i = 0; i < len; i += CACHE_LINE_SIZE) {
#ifdef STATICS_EVALUATION
            flush_sum += 1;
#endif
			asm_clflush((uint64_t *)(buf+i));
#ifdef HAS_SCM_LATENCY
            emulate_latency_ns(EXTRA_SCM_LATENCY);
#endif
		}
		mfence();
		mfence_count = mfence_count + 2;
	} else {
		for (i = 0; i < len; i += CACHE_LINE_SIZE) {
#ifdef STATICS_EVALUATION
            flush_sum += 1;
#endif
			asm_clflush((uint64_t *)(buf+i));
#ifdef HAS_SCM_LATENCY
            emulate_latency_ns(EXTRA_SCM_LATENCY);
#endif
		}
	}
}

void add_log_entry(tree *t, void *addr, unsigned int size, unsigned char type)
{
	log_entry *log;
	int i, remain_size;

	remain_size = size - ((size / LOG_DATA_SIZE) * LOG_DATA_SIZE);

	if ((char *)t->start_log->next_offset == 
			(t->start_log->log_data + LOG_AREA_SIZE))
		t->start_log->next_offset = (log_entry *)t->start_log->log_data;

	if (size <= LOG_DATA_SIZE) {
		log = t->start_log->next_offset;
		log->size = size;
		log->type = type;
		log->addr = addr;
		memcpy(log->data, addr, size);

		if (type == LE_DATA)
			flush_buffer(log, sizeof(log_entry), false);
		else
			flush_buffer(log, sizeof(log_entry), true);

		t->start_log->next_offset = t->start_log->next_offset + 1;
	} else {
		void *next_addr = addr;

		for (i = 0; i < size / LOG_DATA_SIZE; i++) {
			log = t->start_log->next_offset;
			log->size = LOG_DATA_SIZE;
			log->type = type;
			log->addr = next_addr;
			memcpy(log->data, next_addr, LOG_DATA_SIZE);

			flush_buffer(log, sizeof(log_entry), false);

			t->start_log->next_offset = t->start_log->next_offset + 1;
			if ((char *)t->start_log->next_offset == 
					(t->start_log->log_data + LOG_AREA_SIZE))
				t->start_log->next_offset = (log_entry *)t->start_log->log_data;

			next_addr = (char *)next_addr + LOG_DATA_SIZE;
		}

		if (remain_size > 0) {
			log = t->start_log->next_offset;
			log->size = LOG_DATA_SIZE;
			log->type = type;
			log->addr = next_addr;
			memcpy(log->data, next_addr, remain_size);

			flush_buffer(log, sizeof(log_entry), false);
			
			t->start_log->next_offset = t->start_log->next_offset + 1;
		}
	}
}

node *allocNode()
{
#ifdef STATICS_MALLOC 
    pmalloc_sum += 1;
    scm_size += sizeof(node);
#endif
    TOID(node) p;
    POBJ_ZNEW(pop, &p, node);
    node* n = (node *)pmemobj_direct(p.oid);
    memset(n->slot, 0, sizeof(n->slot));
    n->bitmap = 1;
    n->isleaf = 1;
    node_count++;
    return n;
}

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
                              (uint64_t)600ULL * 1024ULL * 1024ULL * 1024ULL, 0666)) == NULL) {
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

tree *initTree()
{
#ifdef STATICS_MALLOC 
    pmalloc_sum += 1;
    scm_size += sizeof(tree);
#endif
	openPmemobjPool();
	tree *t =(tree *)malloc(sizeof(tree)); 
	t->root = allocNode(); 
#ifdef STATICS_MALLOC 
    pmalloc_sum += 1;
    scm_size += sizeof(log_area);
#endif
	t->start_log = (log_area *)malloc(sizeof(log_area));
	t->start_log->next_offset = (log_entry *)t->start_log->log_data;
	return t;
}

void *Lookup(tree *t, unsigned long key)
{
	node *curr = t->root;
	curr = find_leaf_node(curr, key);
	int loc = Search(curr, curr->slot, key);

	if (loc > curr->slot[0]) 
		loc = curr->slot[0];

	if (curr->entries[curr->slot[loc]].key != key || loc > curr->slot[0])
		return NULL;

	return curr->entries[curr->slot[loc]].ptr;
}

void Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
		unsigned long buf[])
{
	int loc, i;
	unsigned long search_count = 0;
	struct timespec t1, t2;
	unsigned long elapsed_time;
	node *curr = t->root;

	curr = find_leaf_node(curr, start_key);
	loc = Search(curr, curr->slot, start_key);

	while (search_count < num) {
		for (i = loc; i <= curr->slot[0]; i++) {
			buf[search_count] = *(unsigned long *)curr->entries[curr->slot[i]].ptr;
			search_count++;
			if(search_count == num) {
				return ;
			}
		}

		curr = curr->leftmostPtr;
		if (curr == NULL) {
			printf("error\n");
			return ;
		}
		loc = 1;
	}
}

unsigned long find_next_zero_bit(const unsigned long *addr, unsigned long size,
		unsigned long offset)
{
	const unsigned long *p = addr + BITOP_WORD(offset);
	unsigned long result = offset & ~(BITS_PER_LONG - 1);
	unsigned long tmp;

	if (offset >= size)
		return size;
	size -= result;
	offset %= BITS_PER_LONG;
	if (offset) {
		tmp = *(p++);
		tmp |= ~0UL >> (BITS_PER_LONG - offset);
		if (size < BITS_PER_LONG)
			goto found_first;
		if (~tmp)
			goto found_middle;
		size -= BITS_PER_LONG;
		result += BITS_PER_LONG;
	}
	while (size & ~(BITS_PER_LONG - 1)) {
		if (~(tmp = *(p++)))
			goto found_middle;
		result += BITS_PER_LONG;
		size -= BITS_PER_LONG;
	}
	if (!size)
		return result;
	tmp = *p;

found_first:
	tmp |= ~0UL << size;
	if (tmp == ~0UL)	/* Are any bits zero? */
		return result + size;	/* Nope */
found_middle:
	return result + ffz(tmp);
}

int Append(node *n, unsigned long key, void *value)
{
	int errval = -1;
	unsigned long index;

	index = find_next_zero_bit(&n->bitmap, BITMAP_SIZE, 1) - 1;
	if (index == BITMAP_SIZE - 1)
		return errval;

	n->entries[index].key = key;
	n->entries[index].ptr = value;
	return index;
}

int Append_in_inner(node *n, unsigned long key, void *value)
{
	int errval = -1;
	unsigned long index;

	index = find_next_zero_bit(&n->bitmap, BITMAP_SIZE, 1) - 1;
	if (index == BITMAP_SIZE - 1)
		return errval;

	n->entries[index].key = key;
	n->entries[index].ptr = value;
	return index;
}

int Search(node *curr, char *temp, unsigned long key)
{
	int low = 1, mid = 1;
	int high = temp[0];

	while (low <= high){
		mid = (low + high) / 2;
		if (curr->entries[temp[mid]].key > key)
			high = mid - 1;
		else if (curr->entries[temp[mid]].key < key)
			low = mid + 1;
		else
			break;
	}

	if (low > mid) 
		mid = low;

	return mid;
}

node *find_leaf_node(node *curr, unsigned long key) 
{
	int loc;

	if (curr->isleaf) 
		return curr;
	loc = Search(curr, curr->slot, key);

	if (loc > curr->slot[0]) 
		return find_leaf_node((node *)(curr->entries[curr->slot[loc - 1]].ptr), key);
	else if (curr->entries[curr->slot[loc]].key <= key) 
		return find_leaf_node((node *)(curr->entries[curr->slot[loc]].ptr), key);
	else if (loc == 1) 
		return find_leaf_node(curr->leftmostPtr, key);
	else 
		return find_leaf_node((node *)(curr->entries[curr->slot[loc - 1]].ptr), key);
}


void Insert(tree *t, unsigned long key, void *value)
{
#ifdef STATICS_MALLOC 
    scm_size += value_size;
#endif
	int numEntries;
	node *curr = t->root;
	/* Find proper leaf */
	curr = find_leaf_node(curr, key);

	/* Check overflow & split */
	numEntries = curr->slot[0];
	if(numEntries == NODE_SIZE) {
#ifdef STATICS_EVALUATION
        latency_start = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif
		add_log_entry(t, curr, sizeof(node), LE_DATA);
		node *splitNode = allocNode();
		int j, loc, cp = curr->slot[0];
		splitNode->leftmostPtr = curr->leftmostPtr;

		//overflown node
		for (j = MIN_LIVE_ENTRIES; j > 0; j--) {
			loc = Append(splitNode, curr->entries[curr->slot[cp]].key, 
					curr->entries[curr->slot[cp]].ptr);
			splitNode->slot[j] = loc;
			splitNode->slot[0]++;
			splitNode->bitmap = splitNode->bitmap + (0x1UL << (loc + 1));
			curr->bitmap = curr->bitmap & (~(0x1UL << (curr->slot[cp] + 1)));
			cp--;
		}

		curr->slot[0] -= MIN_LIVE_ENTRIES;

		if (splitNode->entries[splitNode->slot[1]].key > key) {
			loc = insert_in_leaf_noflush(curr, key, value);
			flush_buffer(&(curr->entries[loc]), sizeof(entry), false);
		}
		else
			insert_in_leaf_noflush(splitNode, key, value);

		insert_in_parent(t, curr, splitNode->entries[splitNode->slot[1]].key, splitNode);

		curr->leftmostPtr = splitNode;
		
		flush_buffer(curr->slot, (char *)curr->entries - (char *)curr->slot, false);
		flush_buffer(&curr->leftmostPtr, 8, false);

		add_log_entry(t, NULL, 0, LE_COMMIT);
#ifdef STATICS_EVALUATION
        latency_end = asm_rdtsc();
        balance_latency += CYCLE2NS(latency_end - latency_start);
        balance_flush += flush_sum - flush_start;
        balance_wear += wear_byte_sum - wear_byte_start;
#endif
	}
	else{
		insert_in_leaf(curr, key, value);
	}
}

int insert_in_leaf_noflush(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	loc = Append(curr, key, value);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;

	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	return loc;
}

void insert_in_leaf(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

#ifdef STATICS_EVALUATION
    latency_start = asm_rdtsc();
    flush_start = flush_sum;
    wear_byte_start = wear_byte_sum;
#endif
	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
#ifdef STATICS_EVALUATION
    latency_end = asm_rdtsc();
    unsort_latency += CYCLE2NS(latency_end - latency_start);
    unsort_flush += flush_sum - flush_start;
    unsort_wear += wear_byte_sum - wear_byte_start;
#endif

#ifdef STATICS_EVALUATION
    latency_start = asm_rdtsc();
    flush_start = flush_sum;
    wear_byte_start = wear_byte_sum;
#endif
	loc = Append(curr, key, value);
	flush_buffer(&(curr->entries[loc]), sizeof(entry), false);
#ifdef STATICS_EVALUATION
    latency_end = asm_rdtsc();
    insert_latency += CYCLE2NS(latency_end - latency_start);
    insert_flush += flush_sum - flush_start;
    insert_wear += wear_byte_sum - wear_byte_start;
#endif

#ifdef STATICS_EVALUATION
    latency_start = asm_rdtsc();
    flush_start = flush_sum;
    wear_byte_start = wear_byte_sum;
#endif
	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;
	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
#ifdef STATICS_EVALUATION
    latency_end = asm_rdtsc();
    unsort_latency += CYCLE2NS(latency_end - latency_start);
    unsort_flush += flush_sum - flush_start;
    unsort_wear += wear_byte_sum - wear_byte_start;
#endif
}

void insert_in_inner(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	loc = Append_in_inner(curr, key, value);
	flush_buffer(&(curr->entries[loc]), sizeof(entry), false);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;
	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
}

int insert_in_inner_noflush(node *curr, unsigned long key, void *value)
{
	int loc, mid, j;

	curr->bitmap = curr->bitmap - 1;
	loc = Append_in_inner(curr, key, value);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j >= mid; j--)
		curr->slot[j + 1] = curr->slot[j];

	curr->slot[mid] = loc;

	curr->slot[0] = curr->slot[0] + 1;

	curr->bitmap = curr->bitmap + 1 + (0x1UL << (loc + 1));
	return loc;
}

void insert_in_parent(tree *t, node *curr, unsigned long key, node *splitNode) {
	if (curr == t->root) {
		node *root = allocNode();
		root->isleaf = 0;
		root->leftmostPtr = curr;
		root->bitmap = root->bitmap + (0x1UL << 1);
		root->entries[0].ptr = splitNode;
		root->entries[0].key = key;
		splitNode->parent = root;

		root->slot[1] = 0;
		root->slot[0] = 1;
		flush_buffer(root, sizeof(node), false);
		flush_buffer(splitNode, sizeof(node), false);

		curr->parent = root;
		flush_buffer(&curr->parent, 8, false);

		t->root = root;
		return ;
	}

	node *parent = curr->parent;

	if (parent->slot[0] < NODE_SIZE) {
		int mid, j, loc;

		add_log_entry(t, parent->slot, (char *)parent->entries - (char *)parent->slot, LE_DATA);
		
		parent->bitmap = parent->bitmap - 1;

		loc = Append_in_inner(parent, key, splitNode);
		flush_buffer(&(parent->entries[loc]), sizeof(entry), false);

		splitNode->parent = parent;
		flush_buffer(splitNode, sizeof(node), false);

		mid = Search(parent, parent->slot, key);

		for (j = parent->slot[0]; j >= mid; j--)
			parent->slot[j + 1] = parent->slot[j];

		parent->slot[mid] = loc;
		parent->slot[0] = parent->slot[0] + 1;

		parent->bitmap = parent->bitmap + 1 + (0x1UL << (loc + 1));
		flush_buffer(parent->slot, (char *)parent->entries - (char *)parent->slot, false);
	} else {
		int j, loc, cp = parent->slot[0];
		node *splitParent = allocNode();
		splitParent->isleaf = 0;

		add_log_entry(t, parent, sizeof(node), LE_DATA);

		for (j = MIN_LIVE_ENTRIES; j > 0; j--) {
			loc = Append_in_inner(splitParent,parent->entries[parent->slot[cp]].key, parent->entries[parent->slot[cp]].ptr);
			node *child = (node *)(parent->entries[parent->slot[cp]].ptr);
			add_log_entry(t, &child->parent, 8, LE_DATA);
			child->parent = splitParent;
			flush_buffer(&child->parent, 8, false);
			splitParent->slot[j] = loc;
			splitParent->slot[0]++;
			splitParent->bitmap = splitParent->bitmap + (0x1UL << (loc + 1));
			parent->bitmap = parent->bitmap & (~(0x1UL << (parent->slot[cp] + 1)));
			cp--;
		}

		parent->slot[0] -= MIN_LIVE_ENTRIES;

		if (splitParent->entries[splitParent->slot[1]].key > key) {
			loc = insert_in_inner_noflush(parent, key, splitNode);
			flush_buffer(&(parent->entries[loc]), sizeof(entry), false);
			splitNode->parent = parent;
			flush_buffer(splitNode, sizeof(node), false);
		}
		else {
			splitNode->parent = splitParent;
			flush_buffer(splitNode, sizeof(node), false);
			insert_in_inner_noflush(splitParent, key, splitNode);
		}

		flush_buffer(parent->slot, (char *)parent->entries - (char *)parent->slot, false);

		insert_in_parent(t, parent, 
				splitParent->entries[splitParent->slot[1]].key, splitParent);
	}
}

void *Update(tree *t, unsigned long key, void *value)
{
	node *curr = t->root;
	curr = find_leaf_node(curr, key);
	int loc = Search(curr, curr->slot, key);

	if (loc > curr->slot[0]) 
		loc = curr->slot[0];

	if (curr->entries[curr->slot[loc]].key != key || loc > curr->slot[0])
		return NULL;

    flush_buffer(value, 8, true);
	curr->entries[curr->slot[loc]].ptr = value;
	flush_buffer(&curr->entries[curr->slot[loc]].ptr, 8, true);

	return curr->entries[curr->slot[loc]].ptr;
}

int delete_in_leaf(node *curr, unsigned long key)
{
	int mid, j;

	curr->bitmap = curr->bitmap - 1;
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);

	mid = Search(curr, curr->slot, key);

	for (j = curr->slot[0]; j > mid; j--)
		curr->slot[j - 1] = curr->slot[j];

	curr->slot[0] = curr->slot[0] - 1;

	flush_buffer(curr->slot, sizeof(curr->slot), false);
	
	curr->bitmap = curr->bitmap + 1 - (0x1UL << (mid + 1));
	flush_buffer(&curr->bitmap, sizeof(unsigned long), true);
	return 0;
}

int Delete(tree *t, unsigned long key)
{
	int numEntries, errval = 0;
	node *curr = t->root;
	
	curr = find_leaf_node(curr, key);
	errval = delete_in_leaf(curr, key);

	return errval;
}
