#ifndef RN_TREE_4_H
#define RN_TREE_4_H

#include "../third-party-lib/tbb/spin_rw_mutex.h"
#include <iostream>

#include "nvm_mgr.h"
#include "index.h"
#include "threadinfo.h"
#include "util.h"

namespace nvindex{

namespace RN_tree
{

static const int LN_SIZE = 63;

template <typename K, typename V>
struct node
{
    K key;
    V data;
    node(K k, V d) : key(k), data(d) {}
    node(K k) : key(k) {}
    node() {}
};

template <typename K, typename V, int size>
class Node
{
  public:
    bool is_leaf = 0;
    bool isLeaf(){ return is_leaf; }
};

template <typename K, typename V, int size>
class LN : public Node<K, V, size>
{
    typedef tbb::speculative_spin_rw_mutex speculative_lock_t;

  public:
    LN()
    {
        memset(slot, 0, 64);
        entry = 0;
        persist_entry = 0;
        version = 0;
        next = 0;
        this->is_leaf = true;
    }

    alignas(64) unsigned char slot[64]; // first slot is the number of children
    //unsigned char dslot[64];
    node<K, V> data[64];

    int persist_entry;
    LN<K, V, size> *next;

    /*
     *  /-------- version ------- / perserved / splitting / updating /
     *  /-------- 48 ------------ /    14     /      1    /    1     /
     */
    volatile uint64_t entry;
    volatile uint64_t version;
    volatile uint64_t split_version;
    static const uint64_t LOCK_MASK = 1llu;
    static const uint64_t SPLIT_MASK = 2llu;
    static const int VERSION_SHIFT = 16;
    static const uint64_t META_MASK = (1 << VERSION_SHIFT) - 1;

    inline uint64_t get_version()
    {
        return version >> VERSION_SHIFT;
    }
    inline uint64_t set_version(uint64_t _v)
    {
        version = (_v << VERSION_SHIFT) | (_v & META_MASK);
    }

    inline uint64_t unlock_version(uint64_t _v)
    {
        return _v & (~LOCK_MASK);
    }
    inline uint64_t locked_version(uint64_t _v)
    {
        return _v | LOCK_MASK;
    }

    inline uint64_t stable_version()
    {
        uint64_t _v = version;
        while (_v & LOCK_MASK)
        {
         //   asm("pause");
            _v = version;
        }
        return _v;
    }

    inline int allocate_entry()
    {
        #ifdef NO_CONCURRENT
            return entry++;
        #else
        uint64_t e = entry;
        if ( e >= LN_SIZE ){
            do{
                asm("pause");
            }while(entry >= LN_SIZE);
            return -1;
        }

        while(!__sync_bool_compare_and_swap(&entry, e, e+1)){
            e = entry;
            if (e >= LN_SIZE){
                do{
                    asm("pause");
                }while(entry >= LN_SIZE);
                return -1;
            }
        }
        return e;
        #endif
    }

    void lock()
    {
        uint64_t _v = version;
        while (!__sync_bool_compare_and_swap(&version, unlock_version(_v), locked_version(_v)))
        {
            while(version & LOCK_MASK){
                asm("pause");
            }
            _v = version;
        }
    }
    void set_splitting()
    {
        //assert(version & LOCK_MASK);
        //assert((version & SPLIT_MASK) == 0);
        version |= SPLIT_MASK;
    }

    void unset_splitting(bool add_version=true)
    {
        //assert(version & SPLIT_MASK);
        if (add_version)
            set_version(get_version()+1);
        version &= ~SPLIT_MASK;
    }

    void unlock()
    {
        set_version(get_version()+1);
        version &= (~LOCK_MASK);
    }

    void change_slot(int k, int pos, K key)
    {
        assert(data[slot[k]].key == key);
        slot[k] = pos;
        assert(data[slot[k]].key == key);
    }

    bool remove_slot(K key)
    {
        int k = find_key(key);
        if (k < 0)
            return false;
        for (int i = k + 1; i <= slot[0]; i++)
            slot[i - 1] = slot[i];
        slot[0]--;
        return true;
    }

    inline bool update_slot(K key, int entry)
    {
        int k = find_key(key);
        if (entry < 0)
        {
            // remove
            if (k >= 0)
            {
                for (int i = k; i < slot[0]; i++)
                {
                    slot[i] = slot[i + 1];
                }
                slot[0]--;
                return true;
            }
            return false;
        }

        if (k >= 0)
        {
            // update
            slot[k] = entry;
            return false;
        }
        else
        {
            // insert
            k = -k - 1; // 比key大的第一个数
            slot[0]++;
            for (int i = slot[0]; i > k; i--)
            {
                slot[i] = slot[i - 1];
            }
            slot[k] = entry;
            return true;
        }
    }

    inline int binary_search(K key){
        int l = 1, r = slot[0] + 1;
        while (l < r)
        {
            int mid = (l + r) / 2;
            if (data[slot[mid]].key >= key)
                r = mid;
            else
                l = mid + 1;
        }
        if (r <= slot[0] && data[slot[r]].key == key)
            return r;
        else
            return -r - 1;
    }

    inline int scan_search(K key)
    {
        for (int i = 1; i <= slot[0]; i++)
        {
            if (data[slot[i]].key == key)
                return i;
            if (data[slot[i]].key > key)
                return -(i + 1);
        }
        return -(slot[0] + 1 + 1);
    }

    int find_key(K key)
    {
        if (slot[0] > 20)
            return binary_search(key);
        else
            return scan_search(key);
    }

    inline void _prefetch(){
        char * start_ptr =(char*)this;
        int length =  (sizeof(LN<K, V, size>))/64;
        while(length-- > 0){
            prefetch(start_ptr);
            start_ptr += 64;
        }
    }

    inline void flush()
    {
        flush_data(slot, 64);
        flush_data(data, sizeof(node<K, V>) * 64);
    }

} __attribute__((aligned(64)));

template <typename K, typename V, int size>
class IN : public Node<K, V, size>
{
  public:
    typedef Node<K, V, size> node_t;
    typedef IN<K, V, size> inner_node_t;

  public:
    alignas(64) K keys[size];
    K lower_bound;
    K upper_bound;
    bool infinite_lower_bound;
    bool infinite_upper_bound;

    node_t *children_ptrs[size];
    inner_node_t *parent;
    int children_number;

    IN()
    {
        infinite_lower_bound = infinite_upper_bound = true;
        parent = nullptr;
        this->is_leaf = false;
        children_number = 0;
    }

    inline int binary_search(K key)
    {
        int l = 0, r = children_number - 1;
        while (l < r)
        {
            int mid = (l + r) / 2;
            if (keys[mid] >= key)
            {
                r = mid;
            }
            else
            {
                l = mid + 1;
            }
        }
        return l;
    }

    bool isLeaf() { return false; }

    inline node_t *find(K key)
    {
        return children_ptrs[binary_search(key)];
    }

    inline bool contains(K key)
    {
        if (!infinite_lower_bound && key <= lower_bound)
        {
            return false;
        }
        if (!infinite_upper_bound && key > upper_bound)
        {
            return false;
        }
        return true;
    }

    bool insert(K key, node_t *child)
    {
        int d = binary_search(key);
        if (keys[d] == key)
        {
            if(d < children_number - 1){
                assert(0);
            }
        }
        for (int i = children_number - 1; i > d; i--)
        {
            keys[i] = keys[i - 1];
            children_ptrs[i + 1] = children_ptrs[i];
        }
        keys[d] = key;
        children_ptrs[d + 1] = child;
        assert(d + 1 < size);
        children_number++;

        if (children_number == size)
        {
            return true;
        }
        return false;
    }
} __attribute__((aligned(64)));

template <typename K, typename V, int size>
class Btree : public Index<K, V, size>
{
    typedef Node<K, V, size> node_t;
    typedef IN<K, V, size> inner_node_t;
    typedef LN<K, V, size> leaf_node_t;
    typedef node<K, V> item_t;

    node_t *root;

    typedef tbb::speculative_spin_rw_mutex speculative_lock_t;
    speculative_lock_t mtx;

    void htmTraverseLeaf(K key, inner_node_t *&parent, leaf_node_t *&leaf, uint64_t &split_version)
    {
#ifndef NO_CONCURRENT
        speculative_lock_t::scoped_lock _lock;
        _lock.acquire(mtx, false);
#endif

        parent = nullptr;
        node_t *child = root;

        while (!child->isLeaf())
        {
            parent = (inner_node_t *)child;
            child = parent->find(key);
        }
        leaf = (leaf_node_t *)child;
        split_version = leaf->split_version;

#ifndef NO_CONCURRENT
        _lock.release();
#endif
    }

    bool htmLeafUpdateSlot(leaf_node_t *leaf, K key, int entry)
    {
        speculative_lock_t::scoped_lock _lock;
        _lock.acquire(mtx);
        bool res = leaf->update_slot(key, entry);
        _lock.release();
        return res;
    }

    node_t *split_inner_node(inner_node_t *node)
    {
        inner_node_t *successor = new inner_node_t;
        successor->children_number = node->children_number = size / 2;
        for (int i = 0; i < size / 2; i++)
        {
            successor->children_ptrs[i] = node->children_ptrs[i + size / 2];
            if (successor->children_ptrs[i]->isLeaf() == false)
            {
                ((inner_node_t *)(successor->children_ptrs[i]))->parent = successor;
            }
            successor->keys[i] = node->keys[i + size / 2];
        }

        successor->parent = node->parent;
        successor->lower_bound = node->keys[size / 2 - 1];
        successor->infinite_lower_bound = false;
        successor->upper_bound = node->upper_bound;
        successor->infinite_upper_bound = node->infinite_upper_bound;

        node->upper_bound = successor->lower_bound;
        node->infinite_upper_bound = false;

        return successor;
    }

    void insertInnerNode(inner_node_t *parent, leaf_node_t *leaf, leaf_node_t *newleaf, K sep)
    {
        bool needsplit = true;
        node_t *successor = newleaf;
        node_t *child = leaf;

        while (needsplit)
        {
            if (parent == nullptr)
            {
                if (child != root){
                    assert(0);
                }
                inner_node_t *newroot = new inner_node_t;
                newroot->keys[0] = sep;
                newroot->children_ptrs[0] = child;
                newroot->children_ptrs[1] = successor;
                newroot->children_number = 2;

                if (child->isLeaf() == false)
                {
                    ((inner_node_t *)child)->parent = newroot;
                    ((inner_node_t *)successor)->parent = newroot;
                }
                root = newroot;
                return;
            }
            needsplit = parent->insert(sep, successor);
            if (needsplit)
            {
                successor = split_inner_node(parent);
                child = parent;
                sep = parent->keys[size / 2 - 1];
            }
            parent = parent->parent;
        }
    }

    void htmTreeUpdate(inner_node_t *parent, leaf_node_t *leaf, leaf_node_t *next, K sep)
    {
        speculative_lock_t::scoped_lock _lock;

#ifndef NO_CONCURRENT
        _lock.acquire(mtx);
#endif
        if (parent == nullptr || parent->contains(sep))
        {
            insertInnerNode(parent, leaf, next, sep);
        }

        else
        {

#ifndef NO_CONCURRENT
            _lock.release();
#endif
            leaf_node_t *nleaf;
            uint64_t sv;
            //TODO: 更精确的函数
            htmTraverseLeaf(sep, parent, nleaf, sv);
            if(nleaf != leaf){
                assert(0);
            }
            assert(parent->contains(sep));

#ifndef NO_CONCURRENT
            _lock.acquire(mtx);
#endif
            insertInnerNode(parent, leaf, next, sep);
        }

#ifndef NO_CONCURRENT
        _lock.release();
#endif
    }

    void generateNextLeaf(leaf_node_t *leaf, K &sep)
    {
        // 1. log leaf，创建新的leaf 和 nextleaf。
        leaf_node_t *next = new (alloc_leaf()) leaf_node_t;
        leaf_node_t *log = (leaf_node_t*)(static_leaf());
        memcpy(log, leaf, sizeof(leaf_node_t));
        log->flush();
        leaf_node_t** pointer = (leaf_node_t**) ((char*) log + sizeof(leaf_node_t));
        *pointer = leaf;
        flush_data(pointer, 64);
        //
        int split = log->slot[0] / 2;

        leaf->persist_entry = leaf->entry = leaf->slot[0] = split;
        next->persist_entry = next->entry = next->slot[0] = log->slot[0] - split;

        for (int i = 0; i < split; i++)
        {
            leaf->slot[i + 1] = i;
            leaf->data[i] = log->data[log->slot[i + 1]];
        }
        for (int i = 0; i < next->slot[0]; i++)
        {
            next->slot[i + 1] = i;
            next->data[i] = log->data[log->slot[i + 1 + split]];
        }
        next->next = leaf->next;
        leaf->next = next;
        sep = leaf->data[split - 1].key;
        //std::cout << "leaf sep: " << sep << std::endl;
        leaf->flush();
        next->flush();

        // delete log
        log->slot[0] = 0;
        flush_data(log->slot, 1);
    }

    void shrinkLeaf(leaf_node_t *leaf)
    {
        //leaf_node_t *log = new leaf_node_t;
        leaf_node_t* log = (leaf_node_t*) static_leaf();
        memcpy(log, leaf, sizeof(leaf_node_t));
        leaf_node_t** pointer = (leaf_node_t**) ((size_t) log + sizeof(leaf_node_t));
        *pointer = leaf;
        log->flush();
        flush_data(pointer, 64);

        leaf->persist_entry = leaf->entry = leaf->slot[0] = log->slot[0];
        for (int i = 0; i < log->slot[0]; i++)
        {
            leaf->slot[i + 1] = i;
            leaf->data[i] = log->data[log->slot[i + 1]];
        }
        leaf->flush();
        //delete log;
        log->slot[0] = 0;
        flush_data(log->slot, 1);
    }

    void splitLeafNode(leaf_node_t *leaf, inner_node_t *parent)
    {
        if (leaf->slot[0] > LN_SIZE / 2)
        {
            K sep;
            generateNextLeaf(leaf, sep);
            htmTreeUpdate(parent, leaf, leaf->next, sep);
        }
        else
        {
            shrinkLeaf(leaf);
        }
    }

    leaf_node_t* anchor;

    void htmLeafCopySlot(leaf_node_t* leaf){
    #ifndef NO_CONCURRENT
        speculative_lock_t::scoped_lock _lock;
        _lock.acquire(mtx);
    #endif
        //memcpy(leaf->dslot, leaf->slot, 64);

    #ifndef NO_CONCURRENT
        _lock.release();
    #endif
    }

    bool modify(K key, V value, double latency_breaks[3], bool remove = false)
    {
        speculative_lock_t::scoped_lock lock;
        inner_node_t *parent;
        leaf_node_t *leaf;

    #ifdef PERF_LATENCY
        cpuCycleTimer t1, t2, t3;
        t1.start();
        t3.start();
    #endif

        while (true)
        {
            uint64_t sv;
            htmTraverseLeaf(key, parent, leaf, sv);
            leaf->_prefetch();

            if (remove)
            {
                #ifndef NO_CONCURRENT
                leaf->lock();
                #endif
                if (sv != leaf->get_version()){
                    #ifndef NO_CONCURRENT
                        leaf->unlock();
                    #endif
                    continue;
                }
                bool res = htmLeafUpdateSlot(leaf, key, -1);
                flush_data(leaf->slot, 64);
                //memcpy(leaf->dslot, leaf->slot, 64);
                #ifndef NO_CONCURRENT
                leaf->unlock();
                #endif

                return res;
            }
            int entry = leaf->allocate_entry();
            if (entry < 0){
                continue;
            }
        #ifdef PERF_LATENCY
            t1.end();
            t2.start();
        #endif

            leaf->data[entry].key = key;
            leaf->data[entry].data = value;
            flush_data(&leaf->data[entry], sizeof(node<K, V>));

        #ifdef PERF_LATENCY
            t2.end();
            latency_breaks[2] = t2.duration();
        #endif

        #ifndef NO_CONCURRENT
            leaf->lock();
        #endif
            if (leaf->split_version != sv){
                #ifndef NO_CONCURRENT
                    leaf->unlock();
                #endif
                continue;
            }
            htmLeafUpdateSlot(leaf, key, entry);
            //assert(leaf->slot % 64 == 0);
            //assert(leaf->data % 64 == 0);
        #ifdef PERF_LATENCY
            t2.start();
        #endif
            flush_data(leaf->slot, 64);
        #ifdef PERF_LATENCY
            t2.end();
            latency_breaks[2] += t2.duration();
        #endif

            //htmLeafCopySlot(leaf);
        #ifdef NO_CONCURRENT
            int pentry = leaf->persist_entry+=1;
        #else
            int pentry = __sync_add_and_fetch(&leaf->persist_entry, 1);
        #endif
            assert(pentry <= LN_SIZE);


        #ifdef PERF_LATENCY
            t3.end();
        #endif

            if (pentry == LN_SIZE)
            {
                //leaf->set_splitting();
                splitLeafNode(leaf, parent);
                //leaf->unset_splitting();
            }

        #ifndef NO_CONCURRENT
            leaf->unlock();
        #endif

        #ifdef PERF_LATENCY
            latency_breaks[0] = t3.duration();
            latency_breaks[1] = t1.duration();
            //printf("%lf %lf %lf\n", latency_breaks[0], latency_breaks[1], latency_breaks[2]);
        #endif

            return true;
        }
    }

  public:
    Btree()
    {
        set_leaf_size(sizeof(leaf_node_t));
        printf("[RNTree]\tnew RNTree..\n");

        void* thread_info;
        int threads;
        bool safe;
        bool init = init_nvm_mgr(thread_info, threads, safe);
        register_threadinfo();
        NVMMgr* mgr = get_nvm_mgr();
        void* tree_meta =  mgr->alloc_tree_meta();

        if (init){
            anchor = new (tree_meta) leaf_node_t;
            printf("[RNTREE]\tcreate a new tree at %p\n", anchor);
            root = (leaf_node_t*) anchor;
        }else{
            NVMMgr* mgr = get_nvm_mgr();
            anchor = (leaf_node_t*) tree_meta;
            printf("[RNTREE]\treopen a tree, anchor is %p\n", anchor);
            if (!safe){
                repair(thread_info, threads);
            }
            rebuild();
        }
    }
    virtual ~Btree(){
        printf("[RNTree]\tBye\n");
        //check_leaf();
    }
    void check_leaf(){
        leaf_node_t* leaf = this->anchor;
        K key = leaf->data[leaf->slot[1]].key;
        leaf = leaf->next;
        while(leaf){
            K next_key = leaf->data[leaf->slot[1]].key;
            assert(next_key > key);
            key = next_key;
            leaf = leaf->next;
        }
    }

    bool insert(K key, V value)
    {
        double a[3];
        return modify(key, value, a);
    }

    void scan(K key, bool (*function)(K key, V value)) {
        inner_node_t* parent;
        leaf_node_t* leaf;

        uint64_t sv;
        htmTraverseLeaf(key, parent, leaf, sv);
        int pos = leaf->find_key(key);
        while(leaf){
            for(int i=pos; i<=leaf->slot[0]; i++){
                K key = leaf->data[leaf->slot[i]].key;
                V value = leaf->data[leaf->slot[i]].data;
                if ((*function)(key, value) == true){
                    return;
                }
            }
            leaf = leaf->next;
            pos = 1;
        }

    }

    int htmLeafFindSlot(leaf_node_t *leaf, K key)
    {
#ifndef NO_CONCURRENT
     //   speculative_lock_t::scoped_lock _lock;
     //   _lock.acquire(mtx);
#endif

        int d = leaf->find_key(key);
        int pos = leaf->slot[d];
        if (d < 0)
            pos = -1;
            //std::cout << "pos " << pos << " key " << key << " d " << d << std::endl;

#ifndef NO_CONCURRENT
     //   _lock.release();
#endif

        return pos;
    }

    int htmLeafFindDSlot(leaf_node_t *leaf, K key)
    {
#ifndef NO_CONCURRENT
        speculative_lock_t::scoped_lock _lock;
        _lock.acquire(mtx);
#endif
        int res;

        int l = 1, r = leaf->dslot[0] + 1;
        while (l < r)
        {
            int mid = (l + r) / 2;
            if (leaf->data[leaf->dslot[mid]].key >= key)
                r = mid;
            else
                l = mid + 1;
        }
        if (r <= leaf->dslot[0] && leaf->data[leaf->dslot[r]].key == key)
            res = leaf->dslot[r];
        else
            res = -1;

#ifndef NO_CONCURRENT
        _lock.release();
#endif

        return res;
    }


    V get(K key, double latency_breaks[3])
    {
        inner_node_t *parent;
        leaf_node_t *leaf;

    #ifdef PERF_LATENCY
        cpuCycleTimer t1, t2;
        t1.start();
        t2.start();
    #endif

        uint64_t sv;
        htmTraverseLeaf(key, parent, leaf, sv);
        leaf->_prefetch();

       //return V(-1);
    retry:
    #ifndef NO_CONCURRENT
         uint64_t _v = leaf->stable_version();
    #endif

        int pos = htmLeafFindSlot(leaf, key);
    #ifdef PERF_LATENCY
        t2.end();
    #endif

    #ifndef NO_CONCURRENT
        if (_v != leaf->stable_version())
        {
            goto retry;
        }
    #endif
        if (pos < 0)
        {
            return V(-1);
        }
        V result = leaf->data[pos].data;

    #ifndef NO_CONCURRENT
        if (_v != leaf->stable_version())
        {
            goto retry;
        }
    #endif

    #ifdef PERF_LATENCY
        t1.end();
        latency_breaks[0] = t1.duration();
        latency_breaks[1] = t2.duration();
        latency_breaks[2] = 0;
    #endif

        return result;
    }

    bool update(K key, V value, double latency_breaks[3])
    {
        return modify(key, value, latency_breaks);
    }

    bool remove(K key)
    {
        return modify(key, V(0), nullptr, true);
    }

    void repair(void* thread_info, int nthread){
        leaf_node_t* leaf = anchor;
        while(leaf){
            int _max = 0;
            for(int i=1; i<=leaf->slot[0]; i++){
                _max = std::max(_max, (int)leaf->slot[i]);
            }
            leaf->entry = leaf->persist_entry = _max;
            leaf->version = 0;

            leaf = leaf->next;
        }
        for(int i=0; i<nthread; i++){
            ThreadInfo* ti = (ThreadInfo*)((size_t)thread_info + i * 4096);
            leaf_node_t* leaf = (leaf_node_t*)ti->static_log;
            if (leaf->slot[0]){
                // Crash happens during the leaf splitted.
                leaf_node_t** origin = (leaf_node_t**)((char*)leaf + sizeof(leaf_node_t));
                memcpy(*origin, leaf, sizeof(leaf_node_t));
                K sep;
                if (leaf->slot[0] > size/2)
                    generateNextLeaf(*origin, sep);
                else
                    shrinkLeaf(*origin);
            }
        }
    }

    void rebuild()
    {
        printf("[REBUILD]\trebuild RNTree\n");

        leaf_node_t* leaf = anchor;
        if (leaf->next == nullptr){
            root = leaf;
            return;
        }

        inner_node_t* parent = nullptr;
        int total_leaves = 0;
        while(leaf){
            leaf = leaf->next;
            total_leaves++;
        }

        K* seps = new K[total_leaves];
        node_t** nodes = new node_t*[total_leaves];

        leaf = anchor;
        int i = 0;
        size_t largest_addr = 0;

        while(leaf){
            largest_addr = std::max(largest_addr, (size_t)leaf);

            seps[i] = leaf->data[leaf->slot[leaf->slot[0]]].key; // 最小值
            //printf("[REBUILD]\t %d sep %lld\n", i, seps[i]);
            nodes[i] = leaf;
            if (i > 0){
                assert(seps[i] > seps[i-1]);
            }
            i++;
            leaf = leaf->next;
        }
        printf("[REBUILD]\tlargest leaf addr %p\n", (void*)largest_addr);
        //printf("[REBUILD]\texists nodes: %d \n", i);

        int current_level_size = total_leaves;
        int ISIZE = size/2;

        while(current_level_size > 1){
            int parent_size = (current_level_size+ISIZE-1)/ISIZE;
            //printf("[REBUILD]\t current_level_size %d\n", current_level_size);
            inner_node_t* inner_nodes = new inner_node_t[parent_size];
            for(int i=0; i<parent_size; i++){
                inner_nodes[i].children_ptrs[0] = nodes[i*ISIZE];
                for (int j=0; j<ISIZE && i*ISIZE + j < current_level_size; j++){
                    inner_nodes[i].keys[j] = seps[i*ISIZE + j];
                    inner_nodes[i].children_ptrs[j] = nodes[i*ISIZE+j];
                    if (!nodes[i*ISIZE+j]->isLeaf()){
                        ((inner_node_t*)nodes[i*ISIZE+j])->parent = &inner_nodes[i];
                    }
                }
                inner_nodes[i].children_number = std::min(ISIZE, current_level_size-i*ISIZE);

                if (i != 0){
                    inner_nodes[i].infinite_lower_bound = false;
                    inner_nodes[i].lower_bound = seps[i*ISIZE-1];
                }
                if (i != parent_size-1){
                    inner_nodes[i].infinite_upper_bound = false;
                    inner_nodes[i].upper_bound = seps[(i+1)*ISIZE-1];
                }

                if (i < parent_size-1)
                    seps[i] = seps[(i+1)*ISIZE-1];

             //   printf("[REBUILD]\t %d sep %lld\n", i, seps[i]);

                nodes[i] = &inner_nodes[i];
            }
            current_level_size = parent_size;
            root = inner_nodes;
        }
        printf("[REBUILD]\trebuild over..\n");
    }
};

} // namespace RTM_tree4
} // nvindex

#endif
