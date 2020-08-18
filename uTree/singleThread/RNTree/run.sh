#!/bin/bash

#numactl --physcpubind=0 ./build/fptree_main #> result_fptree.txt
numactl --physcpubind=0 ./build/nvtree_main #> result_nvtree.txt
#numactl --physcpubind=0 ./build/wb+tree_main #> result_wb+tree.txt
