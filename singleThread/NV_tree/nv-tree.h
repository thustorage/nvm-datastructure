/* Implemented by Se Kwon Lee in UNIST NECSST Lab
 * E-mail: sekwonlee90@gmail.com */

 // Configuration: M_PCM_CPUFREQ/EXTRA_SCM_LATENCY/MAX_NUM_ENTRY_IN/MAX_NUM_ENTRY_PLN/MAX_NUM_ENTRY_LN
 // bug (change MAX_NUM_ENTRY_LN) need fix!

#include <assert.h>
#include <emmintrin.h>
#include <libpmemobj.h>
#include <limits.h>
#include <malloc.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <x86intrin.h>
#include <unistd.h>

const int debug = 0;

#define nvtree_msg(fmt, args...) \
  do {                 \
    if (debug > 0){\
      printf("NVTree[%s]{%u}: " fmt "\n", __func__, __LINE__, ##args);\
      fflush(stdout);\
      usleep(1000); }\
}while (0)

/***************************************************************************************************************************/
/* Motivation Evaluation */
//#define TREE_LIST_EVALUATION 
#ifdef TREE_LIST_EVALUATION
    uint64_t latency_start = 0, latency_end = 0;
    uint64_t flush_sum = 0, wear_byte_sum = 0, flush_start = 0, wear_byte_start = 0;
    uint64_t ln_latency = 0, ln_flush = 0, ln_wear = 0;
#endif
#define STATICS_EVALUATION 
#ifdef STATICS_EVALUATION
    uint64_t latency_start = 0, latency_end = 0;
    uint64_t flush_sum = 0, wear_byte_sum = 0, flush_start = 0, wear_byte_start = 0;
    uint64_t sort_latency = 0, unsort_latency = 0, balance_latency = 0, others_latency = 0, insert_latency = 0;
    uint64_t sort_flush = 0, unsort_flush = 0, balance_flush = 0, others_flush = 0, insert_flush = 0;
    uint64_t sort_wear = 0, unsort_wear = 0, balance_wear = 0, others_wear = 0, insert_wear = 0;
#endif
#define STATICS_MALLOC
#ifdef STATICS_MALLOC
    uint64_t pmalloc_sum = 0, pfree_sum = 0, vmalloc_sum = 0, vfree_sum = 0; 
    uint64_t scm_size = 0, dram_size = 0;
    // NOTE
    
#endif
    

#define CACHE_LINE_SIZE		64
#define MAX_NUM_ENTRY_IN	509
#define MAX_NUM_ENTRY_PLN	255
//#define MAX_NUM_ENTRY_LN	169
#define MAX_NUM_ENTRY_LN 300
#define MAX_KEY 			ULONG_MAX
const uint64_t value_size = MAX_NUM_ENTRY_LN+1;
    /*
    const uint64_t SPACE_PER_THREAD = 16ULL * 1024ULL * 1024ULL * 1024ULL;
    extern char *start_addr;
    extern char *curr_addr;
    */
    unsigned long IN_count = 0;
    unsigned long LN_count = 0;
    unsigned long clflush_count = 0;
    unsigned long mfence_count = 0;

    typedef struct entry entry;
    typedef struct Internal_Node IN;
    typedef struct Parent_Leaf_Node PLN;
    typedef struct Leaf_Node LN;
    typedef struct tree tree;

    struct PLN_entry {
      unsigned long key;
      LN *ptr;
      char str[value_size - 8];
};

struct LN_entry {
  bool flag;
  unsigned long key;
  void *value;
};

struct Internal_Node {
  unsigned int nKeys;
  unsigned long key[MAX_NUM_ENTRY_IN];
  char dummy[16];
};

struct Parent_Leaf_Node {
  unsigned int nKeys;
  struct PLN_entry entries[MAX_NUM_ENTRY_PLN];
  char dummy[8];
};

struct Leaf_Node {
  unsigned char nElements;
  LN *sibling;
  unsigned long parent_id;
  struct LN_entry LN_Element[MAX_NUM_ENTRY_LN];
  char dummy[16];
};

void print(LN* node) {
  int i;
  nvtree_msg("Leaf Node: nElements=%d, parent_id=%d", node->nElements,
         node->parent_id);
  for (i = 0; i < node->nElements; i++) {
    nvtree_msg("\tEntry %d: flag=%d, key=%d, value=%p", i,
               node->LN_Element[i].flag, node->LN_Element[i].key,
               node->LN_Element[i].value);
  }
}

POBJ_LAYOUT_BEGIN(nvtree);
POBJ_LAYOUT_TOID(nvtree, Leaf_Node);
POBJ_LAYOUT_END(nvtree);
PMEMobjpool *pop;

struct tree{
  unsigned char height;
  unsigned char is_leaf;	// 0.LN 1.PLN 2.IN
  unsigned long first_PLN_id;
  unsigned long last_PLN_id;
  void *root;
  LN *first_leaf;
};

tree *initTree();
void destroyTree();
void flush_buffer_nocount(void *buf, unsigned long len, bool fence);
int Insert(tree *t, unsigned long key, void *value);
int Update(tree *t, unsigned long key, void *value);
int Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
    unsigned long buf[]);
void *Lookup(tree *t, unsigned long key);
int Delete(tree *t, unsigned long key);
int delete_leaf_node(tree *t, unsigned long parent_id, LN *node, LN *prev_leaf);

/***************************************************************************************************************************/
#define HAS_SCM_LATENCY
//注意下面两个都需要修改
#define M_PCM_CPUFREQ 3900
#define EXTRA_SCM_LATENCY 500
#define NS2CYCLE(__ns) ((__ns) * M_PCM_CPUFREQ / 1000)
#define CYCLE2NS(__cycles) ((__cycles) * 1000 / M_PCM_CPUFREQ)
#define mfence() asm volatile("mfence":::"memory")
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
    //__asm__ __volatile__ ("clflush %0" : : "m"(*addr));
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
#ifdef TREE_LIST_EVALUATION
    wear_byte_sum += len;
#endif
#ifdef STATICS_EVALUATION
    wear_byte_sum += len;
#endif
  len = len + ((unsigned long)(buf) & (CACHE_LINE_SIZE - 1));
    fence = true; //hqd add
  if (fence) {
    mfence();
    for (i = 0; i < len; i += CACHE_LINE_SIZE) {
      clflush_count++;
#ifdef TREE_LIST_EVALUATION
            flush_sum += 1;
#endif
#ifdef STATICS_EVALUATION
            flush_sum += 1;
#endif
      //asm volatile ("clflush %0\n" : "+m" (*(char *)(buf+i)));
            //_mm_clflush((char *)(buf+i));
            asm_clflush((uint64_t *)(buf+i));
#ifdef HAS_SCM_LATENCY
            emulate_latency_ns(EXTRA_SCM_LATENCY);
#endif
    }
    mfence();
    mfence_count = mfence_count + 2;
  } else {
    for (i = 0; i < len; i += CACHE_LINE_SIZE) {
      clflush_count++;
#ifdef TREE_LIST_EVALUATION
            flush_sum += 1;
#endif
#ifdef STATICS_EVALUATION
            flush_sum += 1;
#endif
      //asm volatile ("clflush %0\n" : "+m" (*(char *)(buf+i)));
            //_mm_clflush((char *)(buf+i));
            asm_clflush((uint64_t *)(buf+i));
#ifdef HAS_SCM_LATENCY
            emulate_latency_ns(EXTRA_SCM_LATENCY);
#endif
    }
  }
}

/* Alloc volatile node */
void *allocINode(unsigned long num)
{
#ifdef STATICS_MALLOC 
    vmalloc_sum += 1;
    dram_size += num * sizeof(IN);
#endif
  IN *new_INs = (IN *)calloc(num, sizeof(IN));
  IN_count = num;
  memset(new_INs, 0, num*sizeof(IN));
  return (void *)new_INs;
}

/* Alloc non-volatile leaf node */
LN *allocLNode()
{
#ifdef STATICS_MALLOC
  pmalloc_sum += 1;
    scm_size += sizeof(LN);
#endif 
  TOID(Leaf_Node) p;
  POBJ_ZNEW(pop, &p, Leaf_Node);
  //printf("allocate %d bytes successfully\n", sizeof(Leaf_Node));
  LN *new_LN = (LN *)pmemobj_direct(p.oid);
  flush_buffer(new_LN, sizeof(LN), false);
  LN_count++;
  return new_LN;
  /*
  LN *new_LN = (LN *)curr_addr;
  curr_addr += sizeof(LN);
  flush_buffer(new_LN, sizeof(LN), false);
  LN_count++;
  if (curr_addr >= start_addr + SPACE_PER_THREAD){
          printf("start_addr is %p, curr_addr is %p, SPACE_PER_THREAD is %lu, no free space to alloc\n", start_addr, curr_addr, SPACE_PER_THREAD); exit(0);
  }
  return new_LN;
  */
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
    printf("create new one.");
    if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(nvtree),
                              (uint64_t)200 * 1024 * 1024 * 1024, 0666)) ==
        NULL) {
      perror("failed to create pool.\n");
      return;
    }
  } else {
    printf("open existing one.");
    if ((pop = pmemobj_open(pathname, POBJ_LAYOUT_NAME(nvtree))) == NULL) {
      perror("failed to open pool.\n");
      return;
    }
  }
}

tree *initTree()
{
#ifdef STATICS_MALLOC 
    vmalloc_sum += 1;
    dram_size += sizeof(tree);
#endif
    openPmemobjPool();
    tree *t = (tree *)calloc(1, sizeof(tree));
    return t;
}

void destroyTree() { 
  pmemobj_close(pop); 
}

int binary_search_IN(unsigned long key, IN *node)
{
  int low = 0, mid = 0, high = (node->nKeys - 1);

  while (low <= high) {
    mid = (low + high) / 2;
    if (node->key[mid] > key)
      high = mid - 1;
    else if (node->key[mid] < key)
      low = mid + 1;
    else
      break;
  }

  if (low > mid)
    mid = low;

  return mid;
}

int binary_search_PLN(unsigned long key, PLN *node)
{
  int low = 0, mid = 0, high = (node->nKeys - 1);

  while (low <= high) {
    mid = (low + high) / 2;
    if (node->entries[mid].key > key)
      high = mid - 1;
    else if (node->entries[mid].key < key)
      low = mid + 1;
    else
      break;
  }

  if (low > mid)
    mid = low;

  return mid;
}

LN *find_leaf(tree *t, unsigned long key)
{
  unsigned int pos, id, i;
  IN *current_IN;
  PLN *current_PLN;
  LN *current_LN;

  if (t->is_leaf == 2) {
    id = 0;

    current_IN = (IN *)t->root;

    while (id < t->first_PLN_id) {
      pos = binary_search_IN(key, &current_IN[id]);
      /* MAX_NUM_ENTRY_IN = 2m + 1 */
      id = id * (MAX_NUM_ENTRY_IN) + 1 + pos;
    }

    current_PLN = (PLN *)t->root;

    pos = binary_search_PLN(key, &current_PLN[id]);

    if (pos < current_PLN[id].nKeys){
      nvtree_msg("return");
      return current_PLN[id].entries[pos].ptr;
    }else{
      nvtree_msg("return");
      return NULL;
    }
  } else if (t->is_leaf == 1) {
    current_PLN = (PLN *)t->root;

    pos = binary_search_PLN(key, &current_PLN[0]);

    if (pos < current_PLN[0].nKeys){
      nvtree_msg("return");
      return current_PLN[0].entries[pos].ptr; //PLN的ptr维护存在bug，也可能是nKeys维护存在bug，明天仔细看一下。
    }else{
      nvtree_msg("return");
      return NULL;
    }
  } else {
    current_LN = (LN *)t->root;
    nvtree_msg("return");
    return current_LN;
  }
}

// key可能被删除
int search_leaf_node(LN *node, unsigned long key)
{
  int i, pos = -1;
  for (i = 0; i < node->nElements; i++) {
    if (node->LN_Element[i].key == key &&
        node->LN_Element[i].flag == true) {
      pos = i;
    }

    if (node->LN_Element[i].key == key &&
        node->LN_Element[i].flag == false) {
      pos = -1;
    }
  }
  return pos;
}

void *Lookup(tree *t, unsigned long key)
{
  int pos, i;
  LN *current_LN;
  IN *current_IN = (IN *)t->root;

  current_LN = find_leaf(t, key);
  if (current_LN == NULL){
    //printf("fail to find leaf\n");
    goto fail;
  }
    

  pos = search_leaf_node(current_LN, key);
  if (pos < 0){
    //printf("fail to search in leaf node\n");
    //print(current_LN);
    //exit(1);
    goto fail;
  }
    

  return current_LN->LN_Element[pos].value;
fail:
  return NULL;
}

void insertion_sort(struct LN_entry *base, int num)
{
  int i, j;
  struct LN_entry temp;

  for (i = 1; i < num; i++) {
    for (j = i; j > 0; j--) {
      if (base[j - 1].key > base[j].key) {
        temp = base[j - 1];
        base[j - 1] = base[j];
        base[j] = temp;
      } else
        break;
    }
  }
}

int Range_Lookup(tree *t, unsigned long start_key, unsigned int num, 
    unsigned long buf[])
{
  int i, j = 0, count = 0, invalid_count, valid_num;
  LN *current_LN;
  unsigned long *invalid_key =
    (unsigned long *)malloc(((MAX_NUM_ENTRY_LN)/2 + 1) * sizeof(unsigned long));
  struct LN_entry *valid_Element =
    (struct LN_entry *)malloc(MAX_NUM_ENTRY_LN * sizeof(struct LN_entry));

  current_LN = find_leaf(t, start_key);
  if (current_LN == NULL)
    goto fail;

  while (current_LN != NULL && j < num) {
    valid_num = 0;
    invalid_count = 0;
    for (i = 0; i < current_LN->nElements; i++) {
      if (current_LN->LN_Element[i].flag == false) {
        invalid_key[invalid_count] = current_LN->LN_Element[i].key;
        invalid_count++;
      }
    }

    for (i = 0; i < current_LN->nElements; i++) {
      if (current_LN->LN_Element[i].flag == false)
        continue;
  
      if (invalid_count > 0) {
        if (current_LN->LN_Element[i].key == invalid_key[count]) {
          count++;
          invalid_count--;
          continue;
        }
      }
      valid_Element[valid_num] = current_LN->LN_Element[i];
      valid_num++;
    }

    insertion_sort(valid_Element, valid_num);

    for (i = 0; i < current_LN->nElements; i++) {
      buf[j] = *(unsigned long *)valid_Element[i].value;
      j++;
      if (j == num)
        break;
    }
    current_LN = current_LN->sibling;
  }

  return 0;
fail:
  free(invalid_key); // hqd add
  free(valid_Element);
  return -1;
}

int create_new_tree(tree *t, unsigned long key, void *value)
{
  int errval = -1;
  LN *current_LN;
  t->root = (LN *)allocLNode();
  if (t->root == NULL)
    return errval;

  current_LN = (LN *)t->root;

  current_LN->parent_id = -1;
  current_LN->sibling = NULL;
  current_LN->LN_Element[0].flag = true;
  current_LN->LN_Element[0].key = key;
  current_LN->LN_Element[0].value = value;
  current_LN->nElements++;
  flush_buffer(current_LN, sizeof(LN), true);

  t->height = 0;
  t->first_leaf = (LN *)t->root;
  t->is_leaf = 0;
  t->first_PLN_id = 0;
  t->last_PLN_id = 0;

  return 0;
}

void quick_sort(struct LN_entry *base, int left, int right)
{
  unsigned long i, j, pivot = base[left].key;
  struct LN_entry temp;

  if (left < right)
  {
    i = left;
    j = right + 1;
    while (i <= j) {
      do
        i++;
      while (base[i].key < pivot);

      do
        j--;
      while (base[j].key > pivot);

      if (i < j) {
        temp = base[i];
        base[i] = base[j];
        base[j] = temp;
      }
      else
        break;
    }
    temp = base[j];
    base[j] = base[left];
    base[left] = temp;

    quick_sort(base, left, j - 1);
    quick_sort(base, j + 1, right);
  }
}

//一个leaf里面可能有若干对相同的key被删除和加入
int Find_Valid_Key_Num(LN *leaf)
{
    int valid_count = 0, i, j;
    for (i = 0; i < leaf->nElements; i++) {
        if (leaf->LN_Element[i].flag == false) continue;

        //if (leaf->LN_Element[i].flag == false) invalid_count++;
        for (j = i + 1; j < leaf->nElements; j++) 
            if (leaf->LN_Element[i].key == leaf->LN_Element[j].key && leaf->LN_Element[j].flag == false) break;
        if (j >= leaf->nElements) valid_count++;
    }

    return valid_count;
}

int leaf_scan_replace(tree *t, LN *new_node, LN *leaf)
{
  int i, j = 0, invalid_count = 0, errval = -1, k;
  struct LN_entry *valid_Element =
    (struct LN_entry *)malloc(MAX_NUM_ENTRY_LN * sizeof(struct LN_entry));

  for (i = 0; i < leaf->nElements; i++) {
    if (leaf->LN_Element[i].flag == false) continue;

        for (k = i + 1; k < leaf->nElements; k++) 
            if (leaf->LN_Element[i].key == leaf->LN_Element[k].key && leaf->LN_Element[k].flag == false) {
                break;
            }
        if (k < leaf->nElements) continue;  // deleted
        
    valid_Element[j] = leaf->LN_Element[i];
    j++;
  }

  insertion_sort(valid_Element, j);

    memcpy(new_node->LN_Element, valid_Element, sizeof(struct LN_entry) * j);
    new_node->nElements = j;
    
    free(valid_Element);
  return 0;
}

int leaf_scan_merge(tree *t, LN *new_node, LN *leaf, LN *next_leaf)
{
  int i, j = 0, invalid_count = 0, errval = -1, k;
  struct LN_entry *valid_Element =
    (struct LN_entry *)malloc(MAX_NUM_ENTRY_LN * sizeof(struct LN_entry));

  for (i = 0; i < leaf->nElements; i++) {
    if (leaf->LN_Element[i].flag == false) continue;

        for (k = i + 1; k < leaf->nElements; k++) 
            if (leaf->LN_Element[i].key == leaf->LN_Element[k].key && leaf->LN_Element[k].flag == false) 
                break;
        if (k < leaf->nElements) continue;  // deleted
        
    valid_Element[j] = leaf->LN_Element[i];
    j++;
  }

  insertion_sort(valid_Element, j);

    memcpy(new_node->LN_Element, valid_Element, sizeof(struct LN_entry) * j);
    new_node->nElements = j;

    // copy the next_leaf
    invalid_count = 0;
    j = 0;

  for (i = 0; i < next_leaf->nElements; i++) {
    if (next_leaf->LN_Element[i].flag == false) continue;

        for (k = i + 1; k < next_leaf->nElements; k++) 
            if (next_leaf->LN_Element[i].key == next_leaf->LN_Element[k].key && next_leaf->LN_Element[k].flag == false) 
                break;
        if (k < next_leaf->nElements) continue;  // deleted
        
    valid_Element[j] = next_leaf->LN_Element[i];
    j++;
  }

  insertion_sort(valid_Element, j);

    //for (i = 0; i < j; i++) new_node->LN_Element[i + new_node->nElements] = valid_Element[i];
    memcpy(new_node->LN_Element + new_node->nElements, valid_Element, sizeof(struct LN_entry) * j);
    new_node->nElements += j;
    
    free(valid_Element);
  return 0;
}

int leaf_scan_divide(tree *t, LN *leaf, LN *split_node1, LN *split_node2)
{
  int i, j = 0, count = 0, invalid_count = 0, errval = -1, k;
  struct LN_entry *valid_Element =
    (struct LN_entry *)malloc(MAX_NUM_ENTRY_LN * sizeof(struct LN_entry));

  for (i = 0; i < leaf->nElements; i++) {
    if (leaf->LN_Element[i].flag == false) continue;

        for (k = i + 1; k < leaf->nElements; k++) 
            if (leaf->LN_Element[i].key == leaf->LN_Element[k].key && leaf->LN_Element[k].flag == false) {
                break;
            }
        if (k < leaf->nElements) continue;  // deleted
        
    valid_Element[j] = leaf->LN_Element[i];
    j++;
  }

    //printf("j=%d\n", j);
//	quick_sort(valid_Element, 0, j - 1);
  insertion_sort(valid_Element, j);

  memcpy(split_node1->LN_Element, valid_Element, 
      sizeof(struct LN_entry) * (j / 2));
  split_node1->nElements = (j / 2);

  memcpy(split_node2->LN_Element, &valid_Element[j / 2],
      sizeof(struct LN_entry) * (j - (j / 2)));
  split_node2->nElements = (j - (j / 2));

    assert(split_node1->nElements > 0);
    assert(split_node2->nElements > 0);

    free(valid_Element);
  return 0;
}

int recursive_insert_IN(void *root_addr, unsigned long first_IN_id,
    unsigned long num_IN)
{
  int errval = -1;
  unsigned long id, last_id, pos, prev_id, IN_count = 0;
  IN *current_IN, *prev_IN;

  id = first_IN_id;
  last_id = first_IN_id + num_IN - 1;

  if (id > 0) {
    while (id <= last_id) {
      for (pos = 0; pos < MAX_NUM_ENTRY_IN; pos++) {
        current_IN = (IN *)root_addr;
        prev_id = (id - pos - 1) / MAX_NUM_ENTRY_IN;
        prev_IN = (IN *)root_addr;
        prev_IN[prev_id].key[prev_IN[prev_id].nKeys] =
          current_IN[id].key[current_IN[id].nKeys - 1];
        prev_IN[prev_id].nKeys++;
        id++;
        if (id > last_id)
          break;
      }
      IN_count++;
    }

    first_IN_id = (first_IN_id - 1) / MAX_NUM_ENTRY_IN;
    errval = recursive_insert_IN(root_addr, first_IN_id, IN_count);
    if (errval < 0)
      goto fail;
  }

  errval = 0;
fail:
  return errval;
}

int reconstruct_from_PLN(void *root_addr, unsigned long first_PLN_id, 
    unsigned long num_PLN)
{
  unsigned long prev_id, id, pos, last_id, IN_count = 0, first_IN_id;
  int i, errval;
  IN *prev_IN;
  PLN *current_PLN;

  id = first_PLN_id;
  last_id = first_PLN_id + num_PLN - 1;

  while (id <= last_id) {
    for (pos = 0; pos < MAX_NUM_ENTRY_IN; pos++) {
      current_PLN = (PLN *)root_addr;
      prev_id = (id - pos - 1) / MAX_NUM_ENTRY_IN;

      prev_IN = (IN *)root_addr;
      prev_IN[prev_id].key[prev_IN[prev_id].nKeys] =
        current_PLN[id].entries[current_PLN[id].nKeys - 1].key;
      prev_IN[prev_id].nKeys++;

      for (i = 0; i < current_PLN[id].nKeys; i++) {
        current_PLN[id].entries[i].ptr->parent_id = id;
  //			flush_buffer(&current_PLN[id].entries[i].ptr->parent_id,
  //					sizeof(unsigned long), false);
      }
  //		sfence();

      id++;
      if (id > last_id)
        break;
    }
    IN_count++;	//num_IN
  }

  first_IN_id = (first_PLN_id - 1) / MAX_NUM_ENTRY_IN;
  if (first_IN_id > 0) {
    errval = recursive_insert_IN(root_addr, first_IN_id, IN_count);
    if (errval < 0)
      return errval;
  }

  return 0;
}

int insert_node_to_PLN(PLN *new_PLNs, unsigned long parent_id, unsigned long insert_key,
    unsigned long split_max_key, LN *split_node1, LN *split_node2)
{
  int entry_index, i;
  unsigned long inserted_PLN_id;

  if (split_max_key >
      new_PLNs[parent_id].entries[new_PLNs[parent_id].nKeys - 1].key)
    inserted_PLN_id = parent_id + 1;
  else
    inserted_PLN_id = parent_id;

  for (i = 0; i < new_PLNs[inserted_PLN_id].nKeys; i++) {
    if (split_max_key <= new_PLNs[inserted_PLN_id].entries[i].key) {
      struct PLN_entry temp[new_PLNs[inserted_PLN_id].nKeys - i];
      memcpy(temp, &new_PLNs[inserted_PLN_id].entries[i],
          sizeof(struct PLN_entry) * (new_PLNs[inserted_PLN_id].nKeys - i));
      memcpy(&new_PLNs[inserted_PLN_id].entries[i + 1], temp,
          sizeof(struct PLN_entry) * (new_PLNs[inserted_PLN_id].nKeys - i));
      new_PLNs[inserted_PLN_id].entries[i].key = insert_key;
      new_PLNs[inserted_PLN_id].entries[i].ptr = split_node1;
      new_PLNs[inserted_PLN_id].entries[i + 1].ptr = split_node2;
      new_PLNs[inserted_PLN_id].nKeys++;
      entry_index = i;
      break;
    }
  }

  return entry_index;
}

int reconstruct_PLN(tree *t, unsigned long parent_id, unsigned long insert_key,
    unsigned long split_max_key, LN *split_node1, LN *split_node2)
{
  unsigned long height, max_PLN, total_PLN, total_IN = 1;
  unsigned long new_parent_id;
  unsigned int i, entry_index;
  int errval;
  IN *new_INs;
  PLN *old_PLNs, *new_PLNs;

  height = t->height;

  max_PLN = 1;

  while (height) {
    max_PLN = max_PLN * MAX_NUM_ENTRY_IN;
    height--;
  }

  total_PLN = (t->last_PLN_id - t->first_PLN_id) + 2;

  if (t->is_leaf != 1) {
    if (total_PLN > max_PLN) {
      height = t->height;
      height++;
      for (i = 1; i < height; i++)
        total_IN += total_IN * MAX_NUM_ENTRY_IN;

      new_parent_id = total_IN + parent_id - t->first_PLN_id;

      new_PLNs = (PLN *)allocINode(total_IN + total_PLN);
      old_PLNs = (PLN *)t->root;
      /* total IN == new_first_PLN_id */
      memcpy(&new_PLNs[total_IN], &old_PLNs[t->first_PLN_id],
          sizeof(PLN) * (parent_id - t->first_PLN_id + 1));
      memcpy(&new_PLNs[new_parent_id + 2], &old_PLNs[parent_id + 1], 
          sizeof(PLN) * (t->last_PLN_id - parent_id));
      memcpy(&new_PLNs[new_parent_id + 1].entries,
          &new_PLNs[new_parent_id].entries[new_PLNs[new_parent_id].nKeys / 2],
          sizeof(struct PLN_entry) * (new_PLNs[new_parent_id].nKeys -
            (new_PLNs[new_parent_id].nKeys / 2)));

      new_PLNs[new_parent_id + 1].nKeys = (new_PLNs[new_parent_id].nKeys -
         (new_PLNs[new_parent_id].nKeys / 2));
      new_PLNs[new_parent_id].nKeys = (new_PLNs[new_parent_id].nKeys / 2);

      entry_index = insert_node_to_PLN(new_PLNs, new_parent_id, 
          insert_key, split_max_key, split_node1, split_node2);

      errval = reconstruct_from_PLN(new_PLNs, total_IN, total_PLN);
      if (errval < 0)
        goto fail;

      nvtree_msg("free() in line 874");
      free(t->root);
      t->height = height;
      t->root = (IN *)new_PLNs;
      t->first_PLN_id = total_IN;
      t->last_PLN_id = total_IN + total_PLN - 1;
      t->is_leaf = 2;
    } else {
      old_PLNs = (PLN *)t->root;
      new_PLNs = (PLN *)allocINode(t->first_PLN_id + total_PLN);

      /* copy from first PLN of old PLNs to parent_id's PLN of new PLNs */
      memcpy(&new_PLNs[t->first_PLN_id], &old_PLNs[t->first_PLN_id],
          sizeof(PLN) * (parent_id - t->first_PLN_id + 1));
      /* copy from (parent_id + 1) ~ last of old PLNs to parent_id + 2
       * of newPLNsnew */
      memcpy(&new_PLNs[parent_id + 2], &old_PLNs[parent_id + 1],
          sizeof(PLN) * (t->last_PLN_id - parent_id));
      /* copy the half of PLN's entries to (parent_id + 1)'s PLN */
      memcpy(&new_PLNs[parent_id + 1].entries, 
          &new_PLNs[parent_id].entries[new_PLNs[parent_id].nKeys / 2],
          sizeof(struct PLN_entry) * (new_PLNs[parent_id].nKeys -
            (new_PLNs[parent_id].nKeys / 2)));

      new_PLNs[parent_id + 1].nKeys = (new_PLNs[parent_id].nKeys -
          (new_PLNs[parent_id].nKeys / 2));
      new_PLNs[parent_id].nKeys = (new_PLNs[parent_id].nKeys / 2);

      entry_index = insert_node_to_PLN(new_PLNs, parent_id, insert_key,
          split_max_key, split_node1, split_node2);

      errval = reconstruct_from_PLN(new_PLNs, t->first_PLN_id, total_PLN);
      if (errval < 0)
        goto fail;
      nvtree_msg("free() in line 908");
      free(t->root);
      t->root = (IN *)new_PLNs;
      t->last_PLN_id++;
      t->is_leaf = 2;
    }
  } else {
    total_IN = 1;
    old_PLNs = (PLN *)t->root;
    new_PLNs = (PLN *)allocINode(total_IN + total_PLN);
    new_parent_id = total_IN + parent_id - t->first_PLN_id;

    memcpy(&new_PLNs[total_IN], &old_PLNs[t->first_PLN_id], sizeof(PLN) * 1);
    memcpy(&new_PLNs[2].entries, &new_PLNs[1].entries[new_PLNs[1].nKeys / 2],
        sizeof(struct PLN_entry) * (new_PLNs[1].nKeys - (new_PLNs[1].nKeys / 2)));

    new_PLNs[new_parent_id + 1].nKeys = (new_PLNs[new_parent_id].nKeys -
       (new_PLNs[new_parent_id].nKeys / 2));
    new_PLNs[new_parent_id].nKeys = (new_PLNs[new_parent_id].nKeys / 2);

    entry_index = insert_node_to_PLN(new_PLNs, new_parent_id, 
        insert_key, split_max_key, split_node1, split_node2);

    errval = reconstruct_from_PLN(new_PLNs, total_IN, total_PLN);
    if (errval < 0)
      goto fail;
    nvtree_msg("free() in line 934");
    free(t->root);
    t->height = 1;
    t->root = (IN *)new_PLNs;
    t->first_PLN_id = total_IN;
    t->last_PLN_id = total_IN + total_PLN - 1;
    t->is_leaf = 2;
  }
  return entry_index;
fail:
  return errval;
}

int swap_to_PLN(tree *t, unsigned long parent_id, LN *new_node, LN *leaf)
{
  int entry_index;
  /* Newly inserted key to PLN */
  unsigned long split_max_key = leaf->LN_Element[leaf->nElements - 1].key;
    unsigned long insert_key = new_node->LN_Element[new_node->nElements - 1].key;

  PLN *parent = (PLN *)t->root;

  if (split_max_key <= parent[parent_id].entries[parent[parent_id].nKeys - 1].key) {
    for (entry_index = 0; entry_index < parent[parent_id].nKeys; entry_index++) {
      if (split_max_key <= parent[parent_id].entries[entry_index].key) {
                parent[parent_id].entries[entry_index].key = insert_key;
        parent[parent_id].entries[entry_index].ptr = new_node;
        new_node->parent_id = parent_id;
        break;
      }
    }
  }

fail:
  return entry_index;	//插入新密钥的PLN的条目索引号
}

int delete_to_PLN(tree *t, unsigned long parent_id, LN *node)
{
  int entry_index;
  /* Newly inserted key to PLN */
  unsigned long split_key = node->LN_Element[node->nElements - 1].key; //这两个key不一定是最大的key

  PLN *parent = (PLN *)t->root;

  if (split_key <= parent[parent_id].entries[parent[parent_id].nKeys - 1].key) {
    /* Not PLN split */
    for (entry_index = 0; entry_index < parent[parent_id].nKeys; entry_index++) {
      if (split_key <= parent[parent_id].entries[entry_index].key) {

                if (entry_index != parent[parent_id].nKeys - 1) {
            struct PLN_entry temp[parent[parent_id].nKeys - entry_index];
            memcpy(temp, &parent[parent_id].entries[entry_index + 1],
                sizeof(struct PLN_entry) * (parent[parent_id].nKeys - entry_index - 1));
            memcpy(&parent[parent_id].entries[entry_index], temp,
                sizeof(struct PLN_entry) * (parent[parent_id].nKeys - entry_index - 1));
                }

        parent[parent_id].nKeys--;
        break;
      }
    }
  }

fail:
  return entry_index;	//插入新密钥的PLN的条目索引号
}

int insert_to_PLN(tree *t, unsigned long parent_id, 
    LN *split_node1, LN *split_node2)
{
  int entry_index;
  /* Newly inserted key to PLN */
  unsigned long insert_key = split_node1->LN_Element[split_node1->nElements - 1].key;
  unsigned long split_max_key = split_node2->LN_Element[split_node2->nElements - 1].key;

  PLN *parent = (PLN *)t->root;
    //printf("parent[parent_id].nKeys =%u\n", parent[parent_id].nKeys); fflush(stdout);  //bug!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

  if (parent[parent_id].nKeys == MAX_NUM_ENTRY_PLN) {
    /* PLN split */
    entry_index = reconstruct_PLN(t, parent_id, insert_key, 
        split_max_key, split_node1, split_node2);
    if (entry_index < 0)
      goto fail;
  } else if (split_max_key <= parent[parent_id].entries[parent[parent_id].nKeys - 1].key) {
    /* Not PLN split */
    for (entry_index = 0; entry_index < parent[parent_id].nKeys; entry_index++) {
      if (split_max_key <= parent[parent_id].entries[entry_index].key) {

        struct PLN_entry temp[parent[parent_id].nKeys - entry_index];

        memcpy(temp, &parent[parent_id].entries[entry_index],
            sizeof(struct PLN_entry) * (parent[parent_id].nKeys - entry_index));
        memcpy(&parent[parent_id].entries[entry_index + 1], temp,
            sizeof(struct PLN_entry) * (parent[parent_id].nKeys - entry_index));

        parent[parent_id].entries[entry_index].key = insert_key;
        parent[parent_id].entries[entry_index].ptr = split_node1;
        parent[parent_id].entries[entry_index + 1].ptr = split_node2;

        split_node1->parent_id = parent_id;
        split_node2->parent_id = parent_id;
        parent[parent_id].nKeys++;
        break;
      }
    }
  }

fail:
  return entry_index;	//插入新密钥的PLN的条目索引号
}

void insert_entry_to_leaf(LN *leaf, unsigned long key, void *value, bool flush)
{
  if (flush == true) {
#ifdef STATICS_EVALUATION
        latency_start  = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif
    leaf->LN_Element[leaf->nElements].flag = true;
    leaf->LN_Element[leaf->nElements].key = key;
    leaf->LN_Element[leaf->nElements].value = value;
    flush_buffer(&leaf->LN_Element[leaf->nElements], sizeof(struct LN_entry), false);
#ifdef STATICS_EVALUATION
        latency_end = asm_rdtsc();
        insert_latency += CYCLE2NS(latency_end - latency_start );
        insert_flush += flush_sum - flush_start;
        insert_wear += wear_byte_sum - wear_byte_start;
#endif
#ifdef STATICS_EVALUATION
        latency_start  = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif
    leaf->nElements++;
    flush_buffer(&leaf->nElements, sizeof(unsigned char), true);
#ifdef STATICS_EVALUATION
        latency_end = asm_rdtsc();
        unsort_latency += CYCLE2NS(latency_end - latency_start );
        unsort_flush += flush_sum - flush_start;
        unsort_wear += wear_byte_sum - wear_byte_start;
#endif
  } else {
    leaf->LN_Element[leaf->nElements].flag = true;
    leaf->LN_Element[leaf->nElements].key = key;
    leaf->LN_Element[leaf->nElements].value = value;
    leaf->nElements++;
  }
}

int leaf_split_and_insert(tree *t, LN *leaf, unsigned long key, void *value)
{
  int errval = -1, current_idx;
  LN *split_node1, *split_node2, *prev_leaf, *new_leaf;
  PLN *prev_PLN;
    
#ifdef TREE_LIST_EVALUATION
    latency_start = asm_rdtsc();
    flush_start = flush_sum;
    wear_byte_start = wear_byte_sum;
#endif     

  split_node1 = allocLNode();
  split_node2 = allocLNode();
  split_node1->sibling = split_node2;
  split_node2->sibling = leaf->sibling;
  //nvtree_msg("finish allocate two new leaf nodes");

  if (split_node1 == NULL || split_node2 == NULL)
    return errval;
  //nvtree_msg("before leaf_scan_divide()");
  leaf_scan_divide(t, leaf, split_node1, split_node2);
  //nvtree_msg("after leaf_scan_divide()");

#ifdef TREE_LIST_EVALUATION
    latency_end = asm_rdtsc();
    ln_latency += CYCLE2NS(latency_end - latency_start);
    ln_flush += flush_sum - flush_start;
    ln_wear += wear_byte_sum - wear_byte_start;
#endif  

  if (t->is_leaf != 0) {
    //nvtree_msg("before insert_to_PLN(): leaf->parent_id=%d", leaf->parent_id);
    current_idx = insert_to_PLN(t, leaf->parent_id, split_node1, split_node2);
    //nvtree_msg("after insert_to_PLN()");
    if (current_idx < 0)
      goto fail;

    if (current_idx != 0) {
      prev_PLN = (PLN *)t->root;
      prev_leaf = prev_PLN[split_node1->parent_id].entries[current_idx - 1].ptr;
    } else {
      if (split_node1->parent_id > t->first_PLN_id) {
        prev_PLN = (PLN *)t->root;
        prev_leaf = prev_PLN[split_node1->parent_id - 1].entries[prev_PLN[split_node1->parent_id - 1].nKeys - 1].ptr;
      } else {
        t->first_leaf = split_node1;
        goto end;
      }
    }
    //nvtree_msg("before find_leaf()");
    new_leaf = find_leaf(t, key);
    //nvtree_msg("after find_leaf(), before insert_entry_to_leaf()");
#ifdef TREE_LIST_EVALUATION
        latency_start = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif 

    insert_entry_to_leaf(new_leaf, key, value, false);
    //nvtree_msg("after insert_entry_to_leaf");
    flush_buffer(split_node1, sizeof(LN), false);
    flush_buffer(split_node2, sizeof(LN), false);

    prev_leaf->sibling = split_node1;
    flush_buffer(&prev_leaf->sibling, sizeof(&prev_leaf->sibling), true);
#ifdef TREE_LIST_EVALUATION
        latency_end = asm_rdtsc();
        ln_latency += CYCLE2NS(latency_end - latency_start);
        ln_flush += flush_sum - flush_start;
        ln_wear += wear_byte_sum - wear_byte_start;
#endif 
  } else {
    //nvtree_msg("before allocINode()");
    PLN *new_PLN = (PLN *)allocINode(1);
    //nvtree_msg("after allocINode()");
    split_node1->parent_id = 0;
    split_node2->parent_id = 0;
    split_node2->sibling = NULL;

    new_PLN->entries[new_PLN->nKeys].key = 
      split_node1->LN_Element[split_node1->nElements - 1].key;
    new_PLN->entries[new_PLN->nKeys].ptr = split_node1;
    new_PLN->nKeys++;
    new_PLN->entries[new_PLN->nKeys].key = MAX_KEY;
    new_PLN->entries[new_PLN->nKeys].ptr = split_node2;
    new_PLN->nKeys++;

    t->height = 0;
    t->is_leaf = 1;
    t->first_PLN_id = 0;
    t->last_PLN_id = 0;
    t->root = new_PLN;

    new_leaf = find_leaf(t, key);

#ifdef TREE_LIST_EVALUATION
        latency_start = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif
    insert_entry_to_leaf(new_leaf, key, value, false);

    flush_buffer(split_node1, sizeof(LN), false);
    flush_buffer(split_node2, sizeof(LN), true);
#ifdef TREE_LIST_EVALUATION
        latency_end = asm_rdtsc();
        ln_latency += CYCLE2NS(latency_end - latency_start);
        ln_flush += flush_sum - flush_start;
        ln_wear += wear_byte_sum - wear_byte_start;
#endif
  }

end:
#ifdef STATICS_MALLOC 
    pfree_sum += 1;
    scm_size -= sizeof(LN);
#endif
    //printf("free() in line 1200\n");
    //free(leaf);
    return 0;
fail:
  return current_idx;
}

int Insert(tree *t, unsigned long key, void *value)
{
  int errval = -1;
  LN *leaf;

  if (t->root == NULL) {
    errval = create_new_tree(t, key, value);
    if (errval < 0)
      goto fail;
    return errval;
  }
  //nvtree_msg("before find_leaf()");
  leaf = find_leaf(t, key);
  //nvtree_msg("after find_leaf()");
  if (leaf == NULL) {
    nvtree_msg("key = %lu", key);
    goto fail;
  }
  nvtree_msg("before if(): leaf=%p", leaf);
  if (leaf->nElements < MAX_NUM_ENTRY_LN) { // leaf指向非法地址，应该是find_leaf函数出了问题
#ifdef TREE_LIST_EVALUATION
        latency_start = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif
        nvtree_msg("before insert_entry_to_leaf()");
        //usleep(1);
        insert_entry_to_leaf(leaf, key, value, true);
        nvtree_msg("after insert_entry_to_leaf()");
#ifdef TREE_LIST_EVALUATION
        latency_end = asm_rdtsc();
        ln_latency += CYCLE2NS(latency_end - latency_start);
        ln_flush += flush_sum - flush_start;
        ln_wear += wear_byte_sum - wear_byte_start;
#endif        
  }
  else {
    /* Insert after split */
#ifdef STATICS_EVALUATION
        latency_start  = asm_rdtsc();
        flush_start = flush_sum;
        wear_byte_start = wear_byte_sum;
#endif
        nvtree_msg("before leaf_split_and_insert()");
        //fflush(stdout);
        //usleep(1000);
        errval = leaf_split_and_insert(t, leaf, key, value);
        nvtree_msg("after leaf_split_and_insert()");
#ifdef STATICS_EVALUATION
        latency_end = asm_rdtsc();
        balance_latency += CYCLE2NS(latency_end - latency_start );
        balance_flush += flush_sum - flush_start;
        balance_wear += wear_byte_sum - wear_byte_start;
#endif
    if (errval < 0)
      goto fail;
  }

  return 0;
fail:
  return errval;
}

void update_entry_to_leaf(LN *leaf, unsigned long old_key, void *old_value,
    unsigned long new_key, void *new_value, bool flush)
{
  if (flush == true) {
    leaf->LN_Element[leaf->nElements].flag = false;
    leaf->LN_Element[leaf->nElements].key = old_key;
    leaf->LN_Element[leaf->nElements].value = old_value;
    leaf->LN_Element[leaf->nElements + 1].flag = true;
    leaf->LN_Element[leaf->nElements + 1].key = new_key;
    leaf->LN_Element[leaf->nElements + 1].value = new_value;
    flush_buffer(&leaf->LN_Element[leaf->nElements], 
        sizeof(struct LN_entry) * 2, false);
    leaf->nElements = leaf->nElements + 2;
    flush_buffer(&leaf->nElements, sizeof(unsigned char), true);
  } else {
    leaf->LN_Element[leaf->nElements].flag = false;
    leaf->LN_Element[leaf->nElements].key = old_key;
    leaf->LN_Element[leaf->nElements].value = old_value;
    leaf->LN_Element[leaf->nElements + 1].flag = true;
    leaf->LN_Element[leaf->nElements + 1].key = new_key;
    leaf->LN_Element[leaf->nElements + 1].value = new_value;
    leaf->nElements = leaf->nElements + 2;
  }
}

int leaf_split_and_update(tree *t, LN *leaf, unsigned long key, void *value,
    unsigned long new_key, void *new_value)
{
  int errval = -1, current_idx;
  LN *split_node1, *split_node2, *prev_leaf, *new_leaf;
  PLN *prev_PLN;

  split_node1 = allocLNode();
  split_node2 = allocLNode();
  split_node1->sibling = split_node2;
  split_node2->sibling = leaf->sibling;

  if (split_node1 == NULL || split_node2 == NULL)
    return errval;

  leaf_scan_divide(t, leaf, split_node1, split_node2);

  if (t->is_leaf != 0) {
    current_idx = insert_to_PLN(t, leaf->parent_id, split_node1, split_node2);
    if (current_idx < 0)
      goto fail;

    if (current_idx != 0) {
      prev_PLN = (PLN *)t->root;
      prev_leaf = prev_PLN[split_node1->parent_id].entries[current_idx - 1].ptr;
    } else {
      if (split_node1->parent_id > t->first_PLN_id) {
        prev_PLN = (PLN *)t->root;
        prev_leaf = prev_PLN[split_node1->parent_id - 1].entries[prev_PLN[split_node1->parent_id - 1].nKeys - 1].ptr;
      } else {
        t->first_leaf = split_node1;
        goto end;
      }
    }

    new_leaf = find_leaf(t, key);

    update_entry_to_leaf(new_leaf, key, value, new_key, new_value, false);

    flush_buffer(split_node1, sizeof(LN), false);
    flush_buffer(split_node2, sizeof(LN), false);

    prev_leaf->sibling = split_node1;
    flush_buffer(&prev_leaf->sibling, sizeof(&prev_leaf->sibling), true);
  } else {
    PLN *new_PLN = (PLN *)allocINode(1);

    split_node1->parent_id = 0;
    split_node2->parent_id = 0;
    split_node2->sibling = NULL;

    new_PLN->entries[new_PLN->nKeys].key = 
      split_node1->LN_Element[split_node1->nElements - 1].key;
    new_PLN->entries[new_PLN->nKeys].ptr = split_node1;
    new_PLN->nKeys++;
    new_PLN->entries[new_PLN->nKeys].key = MAX_KEY;
    new_PLN->entries[new_PLN->nKeys].ptr = split_node2;
    new_PLN->nKeys++;

    t->height = 0;
    t->is_leaf = 1;
    t->first_PLN_id = 0;
    t->last_PLN_id = 0;
    t->root = new_PLN;

    new_leaf = find_leaf(t, key);
    
    update_entry_to_leaf(new_leaf, key, value, new_key, new_value, false);

    flush_buffer(split_node1, sizeof(LN), false);
    flush_buffer(split_node2, sizeof(LN), true);
  }

end:
#ifdef STATICS_MALLOC 
    pfree_sum += 1;
    scm_size -= sizeof(LN);
#endif
    //printf("free() in line 1366\n");
    //free(leaf);
    return 0;
fail:
  return current_idx;
}

int Update(tree *t, unsigned long key, void *value)
{
  int errval = -1, pos, i;
  unsigned long old_key, new_key;
  void *old_value, *new_value;
  LN *current_LN;
  IN *current_IN = (IN *)t->root;

  current_LN = find_leaf(t, key);
  if (current_LN == NULL)
    goto fail;

  if (current_LN->nElements < MAX_NUM_ENTRY_LN - 1)
    update_entry_to_leaf(current_LN, key, NULL, key, value, true);
  else {
    /* Insert after split */
    errval = leaf_split_and_update(t, current_LN, key, NULL, key, value);
    if (errval < 0)
      goto fail;
  }

  return 0;
fail:
  return errval;
}

void delete_entry_to_leaf(LN *leaf, unsigned long key, void *value, bool flush)
{
    /*int i;
    for (i = 0; i < leaf->nElements; i++) 
        if(leaf->LN_Element[i].key == key) break;
    if (i == leaf->nElements) assert(0);*/
    
  if (flush == true) {
    leaf->LN_Element[leaf->nElements].flag = false;
    leaf->LN_Element[leaf->nElements].key = key;
    leaf->LN_Element[leaf->nElements].value = value;
    flush_buffer(&leaf->LN_Element[leaf->nElements], 
        sizeof(struct LN_entry), false);
    leaf->nElements++;
    flush_buffer(&leaf->nElements, sizeof(unsigned char), true);
  } else {
    leaf->LN_Element[leaf->nElements].flag = false;
    leaf->LN_Element[leaf->nElements].key = key;
    leaf->LN_Element[leaf->nElements].value = value;
    leaf->nElements++;
  }
}

int leaf_split_and_delete(tree *t, LN *leaf, unsigned long key, void *value)
{
  int errval = -1, current_idx;
  LN *split_node1, *split_node2, *prev_leaf, *new_leaf;
  PLN *prev_PLN;

  split_node1 = allocLNode();
  split_node2 = allocLNode();
  split_node1->sibling = split_node2;
  split_node2->sibling = leaf->sibling;

  if (split_node1 == NULL || split_node2 == NULL)
    return errval;

  leaf_scan_divide(t, leaf, split_node1, split_node2);

  if (t->is_leaf != 0) {
    current_idx = insert_to_PLN(t, leaf->parent_id, split_node1, split_node2);
    if (current_idx < 0)
      goto fail;

    if (current_idx != 0) {
      prev_PLN = (PLN *)t->root;
      prev_leaf = prev_PLN[split_node1->parent_id].entries[current_idx - 1].ptr;
    } else {
      if (split_node1->parent_id > t->first_PLN_id) {
        prev_PLN = (PLN *)t->root;
        prev_leaf = prev_PLN[split_node1->parent_id - 1].entries[prev_PLN[split_node1->parent_id - 1].nKeys - 1].ptr;
      } else {
        t->first_leaf = split_node1;
        goto end;
      }
    }

    new_leaf = find_leaf(t, key);

    delete_entry_to_leaf(new_leaf, key, value, false);

    flush_buffer(split_node1, sizeof(LN), false);
    flush_buffer(split_node2, sizeof(LN), false);

    prev_leaf->sibling = split_node1;
    flush_buffer(&prev_leaf->sibling, sizeof(&prev_leaf->sibling), true);
  } else {
    PLN *new_PLN = (PLN *)allocINode(1);

    split_node1->parent_id = 0;
    split_node2->parent_id = 0;
    split_node2->sibling = NULL;

    new_PLN->entries[new_PLN->nKeys].key = 
      split_node1->LN_Element[split_node1->nElements - 1].key;
    new_PLN->entries[new_PLN->nKeys].ptr = split_node1;
    new_PLN->nKeys++;
    new_PLN->entries[new_PLN->nKeys].key = MAX_KEY;
    new_PLN->entries[new_PLN->nKeys].ptr = split_node2;
    new_PLN->nKeys++;

    t->height = 0;
    t->is_leaf = 1;
    t->first_PLN_id = 0;
    t->last_PLN_id = 0;
    t->root = new_PLN;

    new_leaf = find_leaf(t, key);
    
    delete_entry_to_leaf(new_leaf, key, value, false);

    flush_buffer(split_node1, sizeof(LN), false);
    flush_buffer(split_node2, sizeof(LN), true);
  }

end:
#ifdef STATICS_MALLOC 
    pfree_sum += 1;
    scm_size -= sizeof(LN);
#endif
    //printf("free() in line 1499\n");
    //free(leaf);
    return 0;
fail:
  return current_idx;
}

int leaf_merge_or_replace_and_delete(tree *t, LN *leaf, unsigned long key, void *value)
{
  int errval = -1, current_idx;
  LN *new_node, *prev_leaf, *next_leaf, *new_leaf;
  PLN *prev_PLN;

  new_node = allocLNode();
    if (new_node == NULL) return errval;

    //寻找一个合理的next_leaf
    do {
        next_leaf = leaf->sibling;
        if (next_leaf == NULL) break;
        int valid_num = Find_Valid_Key_Num(next_leaf);
        if (valid_num == 0) delete_leaf_node(t, next_leaf->parent_id, next_leaf, leaf);
        else break;
    } while(1);

    if (next_leaf != NULL && next_leaf->nElements < MAX_NUM_ENTRY_LN / 2) {
        leaf_scan_merge(t, new_node, leaf, next_leaf);  //leaf和next_leaf合并成一个new_node
        delete_leaf_node(t, next_leaf->parent_id, next_leaf, leaf);
        new_node->sibling = leaf->sibling;         //后续只需要prev_leaf和父节点修改就可以了
    }
    else {
        leaf_scan_replace(t, new_node, leaf); //将leaf中的合法entries集中到new_node上
        new_node->sibling = leaf->sibling;
    }

    if (t->is_leaf != 0) {
    current_idx = swap_to_PLN(t, leaf->parent_id, new_node, leaf); //在父节点上把leaf用new_node替换掉
    if (current_idx < 0)
      goto fail;

        if (current_idx != 0) {  //直接可得到前置指针
        prev_PLN = (PLN *)t->root;
        prev_leaf = prev_PLN[new_node->parent_id].entries[current_idx - 1].ptr;
      } else {
        if (new_node->parent_id > t->first_PLN_id) { //如果parent不是第一个
          prev_PLN = (PLN *)t->root;
          prev_leaf = prev_PLN[new_node->parent_id - 1].entries[prev_PLN[new_node->parent_id - 1].nKeys - 1].ptr;
        } else { //否则就是最前面那个
          t->first_leaf = new_node;
          goto end;
        }
      }

      new_leaf = find_leaf(t, key);
      delete_entry_to_leaf(new_leaf, key, value, false);
      flush_buffer(new_node, sizeof(LN), false);

      prev_leaf->sibling = new_node;
      flush_buffer(&prev_leaf->sibling, sizeof(&prev_leaf->sibling), true);
    } else {
        assert(0);
    }
        
end:
#ifdef STATICS_MALLOC 
    pfree_sum += 1;
    scm_size -= sizeof(LN);
#endif
    //printf("free() in line 1567\n");
    //free(leaf);
    return 0;
    
fail:
    return current_idx;
}

int delete_leaf_node(tree *t, unsigned long parent_id, LN *node, LN *prev_leaf) 
{
    delete_to_PLN(t, node->parent_id, node);
    prev_leaf->sibling = node->sibling;
    flush_buffer(&prev_leaf->sibling, sizeof(&prev_leaf->sibling), true);
    
#ifdef STATICS_MALLOC 
    pfree_sum += 1;
    scm_size -= sizeof(LN);
#endif 
  //free(node);
  return 0;
}

int Delete(tree *t, unsigned long key)
{
  int errval = -1;

  LN *leaf = find_leaf(t, key);
  if (leaf == NULL) goto fail;

  if (leaf->nElements < MAX_NUM_ENTRY_LN) 
    delete_entry_to_leaf(leaf, key, NULL, true);
  else {
        /* Find the number of valid keys */ 
        int valid_num = Find_Valid_Key_Num(leaf);
        if (valid_num < MAX_NUM_ENTRY_LN / 2) 
            errval = leaf_merge_or_replace_and_delete(t, leaf, key, NULL); //这里面还有bug
        else 
        errval = leaf_split_and_delete(t, leaf, key, NULL);  /* Delete after split */
    if (errval < 0) goto fail;
  }

  return 0;
fail:
  return errval;
}
