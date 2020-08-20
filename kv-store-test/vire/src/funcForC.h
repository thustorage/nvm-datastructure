// wrapper 
#include <stdint.h>

#ifdef __cplusplus
extern "C"{
#endif

    void* cptree_init(void);
    void cptree_insert(void* p, int64_t key, char* val);
    char* cptree_search(void* p, int64_t key);
    void* cptree_getListHead(void* p);
    void print_btree(void* p);
    void initForFpTree(void);
    void openPmemobjPool(char *pathname, uint64_t allocate_size);

#ifdef __cplusplus
}//end extern "C"
#endif