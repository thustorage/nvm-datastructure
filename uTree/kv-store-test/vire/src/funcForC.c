#include "funcForC.h"

//#define USE_UTREE
#define USE_FAST_FAIR
//#define USE_FPTREE

#ifdef USE_UTREE
#include "utree.h"
#endif

#ifdef USE_FAST_FAIR
#include "fast_fair.h"
#endif

#ifdef USE_FPTREE
#include "fptree.h"
#endif

using namespace std;

extern "C"{
    void* cptree_init(void){
#ifdef USE_UTREE
      perror("use utree!\n");
      btree *bt = new btree();
      return (void *)bt;
#endif
#ifdef USE_FAST_FAIR
        perror("use fast_fair!\n");
        btree *bt = new btree();
        return (void*)bt;
#endif
#ifdef USE_FPTREE
        perror("use fptree!\n");
        fptree_t *fpt = fptree_create();
        return fpt;
#endif
    }

    void cptree_insert(void* p, int64_t key, char* val){
#ifdef USE_UTREE
        btree* bt = (btree*)p;
        bt->insert(key, val);
        return ;
#endif
#ifdef USE_FAST_FAIR
        btree* bt = (btree*)p;
        bt->btree_insert(key, val);
        return ;
#endif
#ifdef USE_FPTREE
        fptree_t *fpt = (fptree_t*)p;
        fptree_put(fpt, key, val);
        return ;
#endif
    }

    char* cptree_search(void* p, int64_t key){
#ifdef USE_UTREE
        btree* bt = (btree*)p;
        char* ret;
        ret = bt->search(key);
        return ret;
#endif
#ifdef USE_FAST_FAIR
        btree* bt = (btree*)p;
        char* ret;
        ret = bt->btree_search(key);
        return ret;
#endif
#ifdef USE_FPTREE
        fptree_t *fpt = (fptree_t*)p;
        fptree_get(fpt, key);
        return NULL;
#endif
    }
    
    void* cptree_getListHead(void* p){
#ifdef USE_UTREE
        btree* bt = (btree*)p;
        return bt->list_head;
#endif
#ifdef USE_FAST_FAIR
        btree* bt = (btree*)p;
        return bt->getRoot();
#endif
        return NULL;
    }

    void print_btree(void* p){
#ifdef USE_UTREE
        btree* bt = (btree*)p;
        bt->printAll();
#endif
#ifdef USE_FAST_FAIR
        btree* bt = (btree*)p;
        bt->printAll();
#endif
        
    }

    void initForFpTree(void){
#ifdef USE_FPTREE
        _init_set_subsystem();
#endif
    }


    int file_exists(const char *filename) {
      struct stat buffer;
      return stat(filename, &buffer);
    }

    void openPmemobjPool(char *pathname, uint64_t allocate_size) {
#ifndef USE_UTREE
      // printf("use pmdk!\n");
      int sds_write_value = 0;
      pmemobj_ctl_set(NULL, "sds.at_create", &sds_write_value);
      if (file_exists(pathname) != 0) {
        printf("create new one.\n");
        if ((pop = pmemobj_create(pathname, POBJ_LAYOUT_NAME(btree), allocate_size, 0666)) == NULL) {
          perror("failed to create pool.\n");
          return;
        }
      } else {
        printf("open existing one.\n");
        if ((pop = pmemobj_open(pathname, POBJ_LAYOUT_NAME(btree))) == NULL) {
          perror("failed to open pool.\n");
          return;
        }
      }
#endif
    }

}