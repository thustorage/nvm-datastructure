#ifndef pmalloc_wrap_h
#define pmalloc_wrap_h

#include <stdlib.h>
#include <list>
#include <stdio.h>
#include <numa.h>
#include <assert.h>
#include "nvm_mgr.h"
//#define USE_NVM_MALLOC

namespace nvindex{

class PMBlockAllocator{
	int alignment = 64;
	NVMMgr* mgr;
public:
	PMBlockAllocator(){
		mgr = NULL;
	}

	void* alloc_block(int size){
	#ifdef USE_NVM_MALLOC
		if (mgr == NULL){
			mgr = get_nvm_mgr();
		}
		return mgr->alloc_block((size+4095)/4096);
	#else
		return aligned_alloc(alignment, size);
	#endif  // USE_NVM_MALLOC
	}

	void free_block(void* block){
		assert(0);
	}
};

} //nvindex
#endif
