#include "nvm_mgr.h"

#include <mutex>
#include <stdio.h>
#include <cassert>

namespace nvindex{

NVMMgr* nvm_mgr = NULL;
std::mutex _mtx;

NVMMgr::NVMMgr(){
    fd = open(get_filename(), O_RDWR);
    if (fd < 0){
        printf("[NVM MGR]\tfailed to open nvm file\n");
        exit(-1);
    }
    
    void* addr = mmap((void*)start_addr, filesize, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);


    if (addr != (void*)start_addr){
        printf("[NVM MGR]\tmmap failed %p \n", addr);
        exit(0);
    }
    printf("[NVM MGR]\tmmap successfully\n");
}

NVMMgr::~NVMMgr(){
    // 正常结束
    printf("[NVM MGR]\tnormally exits, NVM reset..\n");
    Head* head = (Head*) start_addr;
    head->threads = 0;
    flush_data((void*) head, sizeof(Head));
    munmap((void*) start_addr, filesize);
}

bool NVMMgr::init(void* &thread_info, int& threads, bool& safe){
    Head* head = (Head*) start_addr;
    bool res;

    if (head->status == Head::magic_number){//reopen
        // do nothing
        safe = head->threads == 0;
        res = false;
    }else{  // first time
        head->status = Head::magic_number;
        head->threads = 0;
        flush_data((void*) head, sizeof(Head));
        safe = true;
        res = true;
    }
    thread_info = (void*)head->thread_local_start;
    threads = head->threads;

    return res;
}

void NVMMgr::recover_done(){
    Head* head = (Head*) start_addr;
    head->threads = 0;
    flush_data((void*) head, sizeof(Head));
}

void* NVMMgr::alloc_thread_info(){
    // not thread safe
    Head* head = (Head*) start_addr;
    size_t index = head->threads++;
    flush_data((void*) head, sizeof(Head));
    return (void*)(head->thread_local_start + index * PGSIZE);
}

void* NVMMgr::alloc_block(int pages){
    std::lock_guard<std::mutex> lock(_mtx);

    Head* head = (Head*) start_addr;
    size_t index = head->allocated_pages;
    head->allocated_pages += pages;
    flush_data((void*) head, sizeof(Head));
    return (void*)(head->data_block_start + index * PGSIZE);
}

NVMMgr* get_nvm_mgr(){
    std::lock_guard<std::mutex> lock(_mtx);

    if (nvm_mgr == NULL){
        printf("[NVM MGR]\tnvm manager is not initilized.\n");
        assert(0);
    }
    return nvm_mgr;
}

bool init_nvm_mgr(void* &thread_info, int& threads, bool& safe)
{
    std::lock_guard<std::mutex> lock(_mtx);

    if (nvm_mgr){
        printf("[NVM MGR]\tnvm manager has already been initilized.\n");
        return true;
    }
    nvm_mgr = new NVMMgr();
    return nvm_mgr->init(thread_info, threads, safe);
}

void close_nvm_mgr(){
    std::lock_guard<std::mutex> lock(_mtx);

    delete nvm_mgr;
    nvm_mgr = NULL;
}

}//nvindex
