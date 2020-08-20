#include <vr_core.h>
#include <libpmemobj.h>

#ifdef USE_DAX
extern __thread char* start_addr;
extern __thread char* curr_addr;
__thread PMEMobjpool *pop;
extern cpu_set_t cpuset[2];
extern const uint64_t SPACE_PER_THREAD;
extern const uint64_t SPACE_PER_MAIN_THREAD;
#endif
int
vr_thread_init(vr_thread *thread)
{    
    if (thread == NULL) {
        return VR_ERROR;
    }

    thread->id = 0;
    thread->thread_id = 0;
    thread->fun_run = NULL;
    thread->data = NULL;

    return VR_OK;
}

void
vr_thread_deinit(vr_thread *thread)
{
    if (thread == NULL) {
        return;
    }

    thread->id = 0;
    thread->thread_id = 0;
    thread->fun_run = NULL;
    thread->data = NULL;
}

static void *vr_thread_run(void *data)
{
    vr_thread *thread = data;
    srand(vr_usec_now()^(int)pthread_self());
#ifdef USE_DAX
    /*
    if (thread->affinityNodeID >= 0){
      pthread_t t = pthread_self();
      int ret = pthread_setaffinity_np(t, sizeof(cpu_set_t), &cpuset[thread->affinityNodeID]);
      if (ret)
        perror("pthread_setaffinity_np");
    }*/
#ifdef USE_PMDK
    char pathname[100] = "/home/fkd/CPTree-202006/mount/pmem0/pool-";
    uint64_t allocate_size = SPACE_PER_THREAD;
    if (thread->affinityNodeID < 0) { //负数表示是master或者backend线程，不是worker线程
      allocate_size = SPACE_PER_MAIN_THREAD;
      char main_thread[30] = "main-thread";
      char backend_thread[30] = "backend-thread";
      char* p = main_thread;
      if (thread->affinityNodeID == -2)
        p = backend_thread;
      strcat(pathname, p);
    } else {
      char str_num[50];
      if (thread->affinityNodeID == 1) {
        pathname[34] = '1';
      }
      sprintf(str_num, "%ld", thread->thread_id);
      strcat(pathname, str_num);
    }
    log_debug(LOG_EMERG, "use %s", pathname);
    openPmemobjPool(pathname, allocate_size);
#else
    start_addr = thread->start_addr;
    curr_addr = start_addr;
#endif
    log_debug(LOG_EMERG, "new thread: start_addr is %p, curr_addr is %p", start_addr, curr_addr);
#endif
    thread->fun_run(thread->data);
}

int vr_thread_start(vr_thread *thread)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    
    if (thread == NULL || thread->fun_run == NULL) {
        return VR_ERROR;
    }
    //log_debug(LOG_DEBUG, "enter vr_thread_start");
    pthread_create(&thread->thread_id, 
        &attr, vr_thread_run, thread);

    return VR_OK;
}
