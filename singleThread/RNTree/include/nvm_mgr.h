#ifndef nvm_mgr_h
#define nvm_mgr_h

#include <stdlib.h>
#include <stdio.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include "util.h"
#include <string>

namespace nvindex{
class NVMMgr{
    /*
    *
    * A simple structure to manage the NVM file
    *
    *  /  4K  /     4K          /  1024 * 4K   / ...  /
    *  / head / Root node(meta) / thread local / data /
    *
    *  head:
    *       Avaiable to the NVM manager, including how many blocks allocated.
    *
    *  Root node:
    *       Root node for applications. The application has total control to this area,
    *       which can be used as the metadata of the application. The address of the
    *       root node can be obtained by the function "alloc_tree_meta"
    *
    *  thread local:
    *       Each thread can own a thread local persistent memory area. After system crashes,
    *       NVM manager gives these thread local persistent memory to the application. The
    *       application may use thread local variables to help recovery. For example, in RNTree,
    *       a leaf node is logged in the thread local area before it is splitted. The recovery
    *       procedure can use the thread local log to guarantee the crash consistency of the leaf.
    *
    *       The function "recover_done" should be invoked after the recovery, otherwise these
    *       thread local persistent memories are leaked. The maximum number of thread local blocks
    *       can be allocated is hard coded in this file.
    *
    *  data:
    *       True persistent memory allocated for applications. For simplicity, we do not recyle memory.
    *
    */
    static const size_t start_addr = 0x90000000;
    static const int PGSIZE = 4096;
    static const long long filesize = 8ll * 1024 * 1024 * PGSIZE; //32GB

    static const char* get_filename(){
        static const std::string filename = "/dev/dax0.0";
        return filename.c_str();
    }
    int fd;

    struct Head{
        static const int magic_number = 12345;
        static const int max_threads = 1024;
        int status;
        int threads;
        size_t allocated_pages; // pages
        const static size_t tree_meta_start = start_addr + PGSIZE;
        const static size_t thread_local_start = tree_meta_start + PGSIZE ;
        const static size_t data_block_start = thread_local_start + PGSIZE * max_threads;
    };
public:
    NVMMgr();
    ~NVMMgr();

    // Return whether the NVM file is created for the first time.
    // If not, this function will set references passed to it correctly:
    //      thread_info: the start address of thread local memories.
    //      threads: the number of thread local pages. Each page is 4K Bytes.
    //      safe: whether the NVM manager normally existed last time (by call the destruction function)
    bool init(void* &thread_info, int& threads, bool& safe);

    // TODO: raise exception if this function is not invoked before any alloc function.
    void recover_done();

    static void* alloc_tree_meta(){
        return (void*)(Head::tree_meta_start);
    }
    void* alloc_thread_info();
    void* alloc_block(int pages);
};

NVMMgr* get_nvm_mgr();
bool init_nvm_mgr(void* &thread_info, int& threads, bool& safe);
void close_nvm_mgr();

}// nvindex

#endif // nvm_mgr_h
