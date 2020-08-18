#ifndef WBTREE_2_H
#define WBTREE_2_H

#include "index.h"
#include "util.h"

namespace nvindex{
namespace WBTREE_2
{

static const int LN_SIZE = 7;

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
    virtual bool isLeaf() = 0;
};

class SlotOperator{
public:
    static int get_slotnumber(uint64_t s){
        return s >> 56;
    }
    static int get_slot(uint64_t s, int k){
        return (s >> (8*(7-k))) & 0xff;
    }
    static uint64_t set_slot(uint64_t s, int k, int i){ // s 的第k个替换成i。
        //[number][1][2][3][loc][5][6][7]
        assert(i <= 7); assert(k <= 7);
        int loc = (8*(7-k));
        s &= (~(0xffllu << loc));
        s |= ((uint64_t)i << loc);
        return s;
    }
    static uint64_t remove_slot(uint64_t s, int k){ // 删掉s的第k个。
        //[number][1][2][3][loc][5][6][7]
        uint64_t tot = s >> 56; int loc = 8*(8-k);
        uint64_t num = (tot - 1) << 56;
        uint64_t head = ((s >> loc) << loc) & ((1llu<<56) - 1);
        uint64_t tail = (s << 8) & ((1llu<<loc)-1);
        return num | head | tail;
    }
    static uint64_t insert_slot(uint64_t s, int k, int p){ // 在第k个位置插入值为p的数
        //[number][1][2][3][k][5][6][7]
        uint64_t tot = s >> 56; int loc = 8*(8-k);
        uint64_t num = (tot + 1) << 56;
        uint64_t tail = (s >> 8) & ((1llu<<(loc-8)) - 1);
        uint64_t head = ((s >> loc) << loc) & ((1llu<<56) - 1);
        uint64_t mid = ((uint64_t)p << (loc-8));
        return num | head | tail | mid;
    }
};

template <typename K, typename V, int size>
class LN : public Node<K, V, size>
{
    typedef tbb::speculative_spin_rw_mutex speculative_lock_t;

  public:
    bool isLeaf() { return true; }
    LN(){ slot = 0; next = 0; }

    alignas(64)  uint64_t slot;

    int persist_entry;
    LN<K, V, size> *next;
    node<K, V> data[8];

    inline int find_empty_slot(){
        bool bitmap[8] = {0}; int tot = SlotOperator::get_slotnumber(slot);
        for(int i=1; i<=tot; i++){
            int index = SlotOperator::get_slot(slot, i);
            bitmap[index] = 1;
        }
        for(int i=0; i<=7; i++){
            if (!bitmap[i])
                return i;
        }
        return -1;
    }

    inline bool update_slot(K key, int entry)
    {
        int k = find_key(key);
        if (entry < 0)
        {
            // remove
            if (k >= 0)
            {
                slot = SlotOperator::remove_slot(slot, k);
                return true;
            }
            return false;
        }

        if (k >= 0)
        {
            // update
            slot = SlotOperator::set_slot(slot, k, entry);
            return false;
        }
        else
        {
            // insert
            k = -k - 1; // 比key大的第一个数
            slot = SlotOperator::insert_slot(slot, k, entry);
            return true;
        }
    }

    inline int scan_search(K key)
    {
        int cont = SlotOperator::get_slotnumber(slot);
        for (int i = 1; i <= cont; i++)
        {
            int index = SlotOperator::get_slot(slot, i);
            if (data[index].key == key)
                return i;
            if (data[index].key > key)
                return -(i + 1);
        }
        return -(cont + 1 + 1);
    }

    int find_key(K key)
    {
        return scan_search(key);
    }

    inline void _prefetch(){
        char * start_ptr =(char*)this;
        int length = (sizeof(LN<K, V, size>))/64;
        while(length-- > 0){
            prefetch(start_ptr);
            start_ptr += 64;
        }
    }

    inline void flush()
    {
        flush_data(&slot, 1);
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
    typedef Node<K, V, size> node_t;
    typedef IN<K, V, size> inner_node_t;
    typedef LN<K, V, size> leaf_node_t;
    typedef node<K, V> item_t;

    node_t *root;

    typedef tbb::speculative_spin_rw_mutex speculative_lock_t;
    speculative_lock_t mtx;

    void htmTraverseLeaf(K key, inner_node_t *&parent, leaf_node_t *&leaf)
    {

        parent = nullptr;
        node_t *child = root;

        while (!child->isLeaf())
        {
            parent = (inner_node_t *)child;
            child = parent->find(key);
        }
        leaf = (leaf_node_t *)child;
    }

    bool htmLeafUpdateSlot(leaf_node_t *leaf, K key, int entry)
    {
        bool res = leaf->update_slot(key, entry);
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
                assert(child == root);
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
                std::cout << "new root " << root << std::endl;
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
        if (parent == nullptr || parent->contains(sep))
        {
            insertInnerNode(parent, leaf, next, sep);
        }
        else
        {
            assert(0);
            leaf_node_t *nleaf;
            //TODO: 更精确的函数
            htmTraverseLeaf(sep, parent, nleaf);

            assert(nleaf == leaf);
            assert(parent->contains(sep));
            insertInnerNode(parent, leaf, next, sep);
        }
    }

    void generateNextLeaf(leaf_node_t *leaf, K &sep)
    {
        // 1. log leaf，创建新的leaf 和 nextleaf。
        leaf_node_t *next = new (alloc_leaf()) leaf_node_t;
        leaf_node_t *log = (leaf_node_t*)(static_leaf());
        memcpy(log, leaf, sizeof(leaf_node_t));
        log->flush();

        //
        int split = LN_SIZE / 2;
        uint64_t slot = leaf->slot;
        leaf->slot = next->slot = 0;
        for (int i = 0; i < split; i++)
        {
            leaf->slot = SlotOperator::insert_slot(leaf->slot, i+1, i);
            leaf->data[i] = log->data[SlotOperator::get_slot(slot, i+1)];
        }
        for (int i = 0; i < LN_SIZE-split; i++)
        {
            next->slot = SlotOperator::insert_slot(next->slot, i+1, i);
            next->data[i] = log->data[SlotOperator::get_slot(slot, i+1+split)];
        }
        next->next = leaf->next;
        leaf->next = next;
        sep = leaf->data[split - 1].key;
        //std::cout << "leaf sep: " << sep << std::endl;
        leaf->flush();
    }

    void shrinkLeaf(leaf_node_t *leaf)
    {
        //leaf_node_t *log = new leaf_node_t;
        leaf_node_t* log = (leaf_node_t*) static_leaf();
        memcpy(log, leaf, sizeof(leaf_node_t));
        log->flush();

        leaf->persist_entry = leaf->entry = leaf->slot[0] = log->slot[0];
        for (int i = 0; i < log->slot[0]; i++)
        {
            leaf->slot[i + 1] = i;
			leaf->dslot[i+1] = i;
            leaf->data[i] = log->data[log->slot[i + 1]];
        }
		leaf->dslot[0] = log->slot[0];
        leaf->flush();
        //delete log;
    }

    void splitLeafNode(leaf_node_t *leaf, inner_node_t *parent)
    {
        K sep;
        generateNextLeaf(leaf, sep);
        htmTreeUpdate(parent, leaf, leaf->next, sep);
    }

    leaf_node_t* anchor;


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
            htmTraverseLeaf(key, parent, leaf);
            leaf->_prefetch();

            if (remove)
            {
                bool res = htmLeafUpdateSlot(leaf, key, -1);
                flush_data(&leaf->slot, 64);
                return res;
            }
            int entry = leaf->find_empty_slot();
            assert(entry >= 0 && entry < LN_SIZE);

            leaf->data[entry].key = key;
            leaf->data[entry].data = value;
            flush_data(&leaf->data[entry], sizeof(node<K, V>));

            htmLeafUpdateSlot(leaf, key, entry);
            flush_data(&leaf->slot, 64);

            int pentry = SlotOperator::get_slotnumber(leaf->slot);
            assert(pentry <= LN_SIZE);

            if (pentry == LN_SIZE)
            {
                splitLeafNode(leaf, parent);
            }

            return true;
        }
    }

  public:
    Btree()
    {
        set_leaf_size(sizeof(leaf_node_t));
        void* thread_info;
        int threads;
        bool safe;
        bool init = init_nvm_mgr(thread_info, threads, safe);

        register_threadinfo();

        root = new (alloc_leaf()) leaf_node_t;
        anchor = (leaf_node_t*) root; // Never changed.
        printf("new wB+ tree (8 slot array)\n");
    }

    bool insert(K key, V value)
    {
        double a[3];
        return modify(key, value, a);
    }

    void scan(K key, bool (*function)(K key, V value)) {
        //Not Implemented
    }

    V get(K key, double latency_breaks[3])
    {
        inner_node_t *parent;
        leaf_node_t *leaf;
        htmTraverseLeaf(key, parent, leaf);
        leaf->_prefetch();
        int pos = leaf->find_key(key);
        if (pos < 0)
            return V(-1);
        else
            return leaf->data[SlotOperator::get_slot(leaf->slot, pos)].data;
    }

    bool update(K key, V value, double latency_breaks[3])
    {
        return modify(key, value, latency_breaks);
    }

    bool remove(K key)
    {
        return modify(key, V(0), nullptr, true);
    }

    void rebuild()
    {
        // Not Implemented
    }
};
} // WBTREE_2
}// nvindex
#endif
