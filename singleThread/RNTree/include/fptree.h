#ifndef fp_tree_h
#define fp_tree_h

#include "../third-party-lib/tbb/spin_rw_mutex.h"
#include <mutex>
#include <algorithm>
#include <utility>

#include "pmalloc_wrap.h"
#include "index.h"
#include "threadinfo.h"

namespace nvindex{
namespace FP_tree
{
#define bits_a_word (sizeof(unsigned long long) * 8)
#define words_a_bitmap ((size + bits_a_word - 1) / bits_a_word)

template <int size>
struct Bitmap
{
    unsigned long long data[words_a_bitmap];
    int find_first_zero()
    {
        for (int i = 0; i < words_a_bitmap; i++)
        {
            if (data[i] == ~0lu)
            {
                continue;
            }
            return i * bits_a_word + ffz(data[i]);
        }
        assert(0);
        return -1;
    }
    void set_slot(int i)
    {
        data[i / bits_a_word] |= 1llu << (i % bits_a_word);
    }
    void clear_slot(int i)
    {
        data[i / bits_a_word] &= ~0llu - (1llu << (i % bits_a_word));
    }
    unsigned long test(int i)
    {
        return data[i / bits_a_word] & (1llu << (i % bits_a_word));
    }
    void inverse(Bitmap<size> &bitmap)
    {
        for (int i = 0; i < words_a_bitmap; i++)
        {
            data[i] = ~bitmap.data[i];
        }
    }
    int non_zero()
    {
        int count = 0;
        for (int i = 0; i < words_a_bitmap; i++)
        {
            unsigned long d = data[i];

            while (d)
            {
                d = d & (d - 1);
                count++;
            }
        }
        return count;
    }
};

template <typename K, typename V>
class Item
{
  public:
    K key;
    V value;
    Item() {}
    Item(K k, V v) : key(k), value(v) {}
};

template <typename K, typename V, int size>
class Node
{
  public:
    virtual bool isLeaf() = 0;
};

template <typename K, typename V, int size>
class alignas(64) LN : public Node<K, V, size>
{
  public:
    alignas(64) unsigned char finger_prints[size];
    Bitmap<size> bitmap;
    Item<K, V> kv[size];

    LN<K, V, size> *next;
    int number;

    alignas(64) volatile int lock;

    void reset()
    {
        LN();
    }
    inline bool isFull() { return number == size; }
    bool isLeaf() { return true; }
    LN()
    {
        number = lock = 0;
        next = NULL;
        memset(finger_prints, 0, size);
        memset(bitmap.data, 0, sizeof(unsigned long) * words_a_bitmap);
    }
};

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

    node_t *find(K key)
    {
        return children_ptrs[binary_search(key)];
    }

    inline bool contains(K key)
    {
        if (!infinite_lower_bound && key < lower_bound)
        {
            return false;
        }
        if (!infinite_upper_bound && key >= upper_bound)
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
            assert(d >= children_number - 1);
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
    enum Status
    {
        Abort,
        Split,
        Insert,
        Update,
        _Status_number,
        Delete,
        Empty
    };

    typedef Node<K, V, size> node_t;
    typedef IN<K, V, size> inner_node_t;
    typedef LN<K, V, size> leaf_node_t;
    typedef Item<K, V> item_t;

    std::pair<leaf_node_t *, inner_node_t *> find_leaf(K key)
    {
        if (root->isLeaf())
        {
            return std::make_pair((leaf_node_t *)root, (inner_node_t *)NULL);
        }

        inner_node_t *parent = (inner_node_t *)root;
        node_t *child = parent->find(key);

        while (!child->isLeaf())
        {
            parent = (inner_node_t *)child;
            child = parent->find(key);
        }

        return std::make_pair((leaf_node_t *)child, parent);
    }

    std::pair<std::pair<leaf_node_t *, leaf_node_t *>, inner_node_t *> find_leaf_and_pre(K key)
    {
        std::pair<leaf_node_t *, inner_node_t *> p = find_leaf(key);
        leaf_node_t *prev = NULL;
        if (p.second != NULL)
            prev = (leaf_node_t *)p.second->find_pre(key);
        return make_pair(make_pair(p.first, prev), p.second);
    }
    node_t *root;

    inner_node_t *split_inner_node(inner_node_t *node)
    {
        assert(node->isLeaf() == false);
        assert(node->children_number == size);
        inner_node_t *succeed = new inner_node_t();

        succeed->children_number = node->children_number = size / 2;
        for (int i = 0; i < size / 2; i++)
        {
            succeed->children_ptrs[i] = node->children_ptrs[i + size / 2];
            if (!succeed->children_ptrs[i]->isLeaf())
            {
                ((inner_node_t *)(succeed->children_ptrs[i]))->parent = succeed;
            }
            succeed->keys[i] = node->keys[i + size / 2];
        }
        succeed->parent = node->parent;
        return succeed;
    }
    void update_parents(K splitKey, inner_node_t *parent, leaf_node_t *leaf)
    {
        bool need_split = true;

        node_t* child = leaf;
        node_t* next = leaf->next;

        while (need_split)
        {
            if (parent == NULL)
            {
                inner_node_t *newroot = new inner_node_t();
                newroot->children_ptrs[0] = child;
                newroot->children_ptrs[1] = next;
                newroot->children_number = 2;
                newroot->keys[0] = splitKey;

                if (child->isLeaf() == false){
                    ((inner_node_t*)child)->parent = newroot;
                    ((inner_node_t*)next)->parent = newroot;
                }
                root = newroot;
                printf("root: %p \n", root);
                return;
            }else
            {
                need_split = parent->insert(splitKey, next);
                if (need_split){
                    next = split_inner_node(parent);
                    child = parent;
                    splitKey = parent->keys[size / 2 - 1];
                    parent = parent->parent;
                }
            }
        }
    }

    typedef struct UlogStructure
    {
        leaf_node_t *p_current_leaf;
        leaf_node_t *p_new_leaf;
        struct UlogStructure *next;
    } ulog_t;
    class UlogQueue
    {
        ulog_t *ulog_pool;
        ulog_t *active_ulog;

      public:
        UlogQueue(int initsize = 1000)
        {
            ulog_pool = new ulog_t[initsize];
            for (int i = 0; i < initsize - 1; i++)
            {
                ulog_pool[i]->next = ulog_pool[i + 1];
            }
            active_ulog = new ulog_t;
            active_ulog->next = NULL;
        }
        ulog_t *get_ulog()
        {
            ulog_t *ulog = ulog_pool;
            ulog_pool = ulog_pool->next;

            ulog->next = active_ulog->next;
            active_ulog->next = ulog;
        }

        void reset_ulog(ulog_t *ulog)
        {
            ulog->p_current_leaf = ulog->p_new_leaf = NULL;
        }
    };

    K SplitLeaf(leaf_node_t *leaf, inner_node_t *parent)
    {

        leaf_node_t *newleaf = new (alloc_leaf()) leaf_node_t;

        assert(leaf->number == size);
        K keys[size];

        for (int i = 0; i < size; i++)
        {
            keys[i] = leaf->kv[i].key;
        }
        std::sort(keys, keys + size);
        K splitKey = keys[size / 2 - 1];

        for (int i = 0; i < size; i++)
        {
            if (leaf->kv[i].key > splitKey)
            {
                newleaf->bitmap.set_slot(i);
            }
        }
        memcpy(newleaf->kv, leaf->kv, sizeof(item_t) * size);
        memcpy(newleaf->finger_prints, leaf->finger_prints, sizeof(char) * size);

        newleaf->next = leaf->next;
        leaf->bitmap.inverse(newleaf->bitmap);
        leaf->next = newleaf;
        newleaf->number = leaf->number = size / 2;
        flush_data(newleaf, sizeof(leaf_node_t));
        flush_data(&leaf->bitmap, sizeof(Bitmap<size>));
        flush_data(&leaf->next, sizeof(leaf_node_t *));

        assert(leaf->bitmap.non_zero() == size / 2);

        assert(newleaf->bitmap.non_zero() == size / 2);
        return splitKey;
    }

    static const uint64_t kFNVPrime64 = 1099511628211;

    static inline unsigned char hashfunc(K val)
    {
        unsigned char hash = 123;
        int i;
        for (i = 0; i < sizeof(K); i++)
        {
            uint64_t octet = val & 0x00ff;
            val = val >> 8;

            hash = hash ^ octet;
            hash = hash * kFNVPrime64;
        }
        return hash;
    }
    leaf_node_t* anchor;
  public:
    Btree()
    {
        set_leaf_size(sizeof(leaf_node_t));
        void* thread_info;
        int threads;
        bool safe;
        bool init = init_nvm_mgr(thread_info, threads, safe);

        register_threadinfo();

        anchor = new (alloc_leaf()) leaf_node_t;
        root = anchor;
        printf("***** New FP tree **** \n");
    }

    typedef tbb::speculative_spin_rw_mutex speculative_lock_t;
    speculative_lock_t mtx;

    void scan(K key, bool (*function)(K key, V value))
    {
        std::pair<leaf_node_t *, inner_node_t *> p = find_leaf(key);
        leaf_node_t *leaf = p.first;

        int entry = 0;
        item_t tmps[size];

        while (leaf)
        {
            for (int i = 0; i < size; i++)
            {
                if (leaf->bitmap.test(i))
                {
                    tmps[entry++] = leaf->kv[i];
                }
            }
            std::sort(tmps, tmps + entry, [](item_t &n1, item_t &n2) {
                return n1.key < n2.key;
            });
            for (int i = 0; i < entry; i++)
            {
                if ((*function)(tmps[i].key, tmps[i].value) == true)
                {
                    return;
                }
            }
            entry = 0;
            leaf = leaf->next;
        }
    }

    inline int find_item(K key, leaf_node_t *leaf)
    {
        int res = -1;
        for (int i = 0; i < size; i++)
        {
            if (leaf->bitmap.test(i) && leaf->finger_prints[i] == hashfunc(key))
            {
                if (leaf->kv[i].key == key)
                {
                    res = i;
                    return i;
                }
            }
        }
        return res;
    }

    V get(K key, double latency_breaks[3])
    {
        std::pair<leaf_node_t *, inner_node_t *> p;
        V res(NULL);

        #ifdef PERF_LATENCY
        cpuCycleTimer t1, t2;
        t1.start();
        #endif

        while (true)
        {
            speculative_lock_t::scoped_lock lock;
            lock.acquire(mtx, false);

            p = find_leaf(key);

            if (p.first->lock)
            {
                lock.release();
                continue;
            }

        #ifdef PERF_LATENCY
            t2.start();
        #endif

            for (int i = 0; i < size; i++)
            {
                if (p.first->bitmap.test(i) && p.first->finger_prints[i] == hashfunc(key) && key == p.first->kv[i].key)
                {
                    res = p.first->kv[i].value;
                    break;
                }
            }

            lock.release();

        #ifdef PERF_LATENCY
            t1.end();
            t2.end();
            latency_breaks[0] = t1.duration();
            latency_breaks[1] = t2.duration();
            if (latency_breaks[0] > 1000000 || latency_breaks[1] > 1000000)
            {
                latency_breaks[0] = 0;
            }
        #endif
            return res;
        }
    }

    // 0: insert,  1:  update,  2: remove
    bool modify(K key, V value, int modify_type, double latency_breaks[3])
    {
        Status decision = Status::Abort;
        std::pair<leaf_node_t *, inner_node_t *> p;
        K splitKey;

        speculative_lock_t::scoped_lock lock;
        int old_slot;

        #ifdef PERF_LATENCY
        cpuCycleTimer t1, t2, t3;
        t1.start();  // 总时间
        t2.start();  // traverse 时间
        #endif

        while (decision == Status::Abort)
        {
        #ifndef NO_CONCURRENT
            lock.acquire(mtx);
        #endif
            p = find_leaf(key);

        #ifndef NO_CONCURRENT
            if (p.first->lock)
            {
                decision = Status::Abort;
                lock.release();
                continue;
            }
        #endif
            p.first->lock = 1;
            decision = p.first->isFull() ? Status::Split : Status::Insert;
            old_slot = find_item(key, p.first);
        #ifndef NO_CONCURRENT
            lock.release();
        #endif
        }
        leaf_node_t *inserted_leaf = p.first;

        #ifdef PERF_LATENCY
        t2.end();
        #endif

        /* conditional write check */
        int new_slot;

        if (old_slot >= 0)
        {
            if (modify_type == 0){
                p.first->lock = 0;
                return false;
            }if (modify_type == 2){
                inserted_leaf->bitmap.clear_slot(old_slot);
                inserted_leaf->number--;
                goto _end;
            }
        }else{ // not found
            if (modify_type != 0){
                p.first->lock = 0;
                return false;
            }
        }

        if (decision == Status::Split)
        {
            splitKey = SplitLeaf(p.first, p.second);
            if (splitKey < key)
            {
                inserted_leaf = p.first->next;
            }
        }

        new_slot = inserted_leaf->bitmap.find_first_zero();
        assert(new_slot != -1);
        #ifdef PERF_LATENCY
        t3.start();
        #endif

        flush_data(&inserted_leaf->kv[new_slot], sizeof(item_t));
        inserted_leaf->finger_prints[new_slot] = hashfunc(key);
        flush_data(&inserted_leaf->finger_prints, size);

        if (modify_type == 0){
            inserted_leaf->kv[new_slot] = item_t(key, value);
            inserted_leaf->bitmap.set_slot(new_slot);
            inserted_leaf->number++;
        }else{
            inserted_leaf->kv[new_slot] = item_t(key, value);
            unsigned long long tmpBitmap = inserted_leaf->bitmap.data[new_slot/bits_a_word];
            tmpBitmap |= 1llu << (new_slot%bits_a_word);
            tmpBitmap &= ~(1llu << (old_slot%bits_a_word));
            inserted_leaf->bitmap.data[new_slot/bits_a_word] = tmpBitmap;
        }

         _end:
        flush_data(&inserted_leaf->bitmap, sizeof(Bitmap<size>));

        #ifdef PERF_LATENCY
        t3.end();
        #endif

        if (decision == Status::Split && modify_type != 2)
        {
        #ifndef NO_CONCURRENT
            lock.acquire(mtx);
        #endif
            update_parents(splitKey, p.second, p.first);

        #ifndef NO_CONCURRENT
            lock.release();
        #endif
        }

        #ifdef PERF_LATENCY
        t1.end();
        latency_breaks[0] = t1.duration();
        latency_breaks[1] = t2.duration();
        latency_breaks[2] = t3.duration();
        #endif

        p.first->lock = 0;
        return true;
    }


    bool insert(K key, V value)
    {
        double a[3];
        return modify(key, value, 0, a);
    }

    bool update(K key, V value, double latency_breaks[3])
    {
        return modify(key, value, 1, latency_breaks);
    }

    bool remove(K key)
    {
        double a[3];
        return modify(key, V(-1), 2, a);
    }


    void rebuild()
    {
        leaf_node_t* leaf = anchor;
        if (leaf->next == nullptr){
            root = leaf;
            return;
        }

        inner_node_t* parent = nullptr;
        int total_leaves = 1;
        while(leaf){
            leaf = leaf->next;
            total_leaves++;
        }
        printf("total level nodes: %d", total_leaves);

        K* seps = new K[total_leaves];
        node_t** nodes = new node_t*[total_leaves];

        leaf = anchor;
        int i = 0;
        while(leaf){
            K max_sep = K(-1);
            for (int j=0; j<size; j++){
                if (leaf->bitmap.test(j) && leaf->kv[j].key > max_sep){
                    max_sep = leaf->kv[j].key;
                }
            }
            seps[i] = max_sep;
            nodes[i] = leaf;
            leaf = leaf->next;
        }

        int current_level_size = total_leaves;
        while(current_level_size > 1){
            int parent_size = (current_level_size+size-1)/size;
            inner_node_t* inner_nodes = new inner_node_t[parent_size];
            for(int i=0; i<parent_size; i++){
                for (int j=0; j<size && i*size + j < current_level_size; j++){
                    inner_nodes[i].keys[j] = seps[i*size + j];
                    inner_nodes[i].children_ptrs[j] = nodes[i*size+j];
                }
                inner_nodes[i].children_number = std::min(size, current_level_size-i*size);
                seps[i] = inner_nodes[i].keys[inner_nodes[i].children_number-1];
                nodes[i] = &inner_nodes[i];
            }
            current_level_size = parent_size;
        }
    }
};

}; // namespace FP_tree
} // nvindex
#endif
