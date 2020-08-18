/******************************************************************************
 * ptst.h
 * 
 * Per-thread state management.
 * 
 * Copyright (c) 2002-2003, K A Fraser
 */

#ifndef __PTST_H__
#define __PTST_H__

typedef struct ptst_st ptst_t;

/* A really simple random-number generator. Crappy linear congruential
 * taken from glibc, but has at least a 2^32 period. */
typedef unsigned long rand_t;
#define rand_init(_ptst) ((_ptst)->rand = RDTICK())
#define rand_next(_ptst) ((_ptst)->rand = ((_ptst)->rand * 1103515245) + 12345)



typedef struct gc_st gc_t;

/* Most of these functions peek into a per-thread state struct. */

/* Initialise GC section of given per-thread state structure. */
gc_t *gc_init(void);
int gc_add_allocator(int alloc_size);
void gc_remove_allocator(int alloc_id);

/* Memory allocate/free. An unsafe free can be used when an object was
 * not made visible to other processes. */
void *gc_alloc(ptst_t *ptst, int alloc_id);
void gc_free(ptst_t *ptst, void *p, int alloc_id);
void gc_unsafe_free(ptst_t *ptst, void *p, int alloc_id);

/* Hook registry. Allows users to hook in their own per-epoch delay lists. */
typedef void (*hook_fn_t)(ptst_t *, void *);
int gc_add_hook(hook_fn_t fn);
void gc_remove_hook(int hook_id);
void gc_add_ptr_to_hook_list(ptst_t *ptst, void *ptr, int hook_id);

/* Per-thread entry/exit from critical regions */
void gc_enter(ptst_t *ptst);
void gc_exit(ptst_t *ptst);

/* Start-of-day initialisation of garbage collector. */
void _init_gc_subsystem(void);
void _destroy_gc_subsystem(void);





struct ptst_st
{
    /* Thread id */
    unsigned int id;
    /* State management 状态管理 */
    ptst_t      *next;
    unsigned int count;
    /* Utility structures 公用数据结构 */
    gc_t        *gc;
    rand_t       rand;
};

extern pthread_key_t ptst_key;

/* Enter/leave a critical region. A thread gets a state handle for use during critical regions.  */
ptst_t *critical_enter(void);           //初始化这个thread state，并进入gc
#define critical_exit(_p) gc_exit(_p)   //离开gc

/* Iterators 递归访问ptst状态链表 */
extern ptst_t *ptst_list;               //访问整个list链表
#define ptst_first()  (ptst_list)       
#define ptst_next(_p) ((_p)->next)

/* Called once at start-of-day for entire application. */
void _init_ptst_subsystem(void);        //初始化，其实感觉是将thread和私有数据挂钩

#endif /* __PTST_H__ */
