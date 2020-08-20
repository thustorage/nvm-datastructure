#!/bin/bash

#gcc -O3 -DNDEBUG -DMUTEX -m64 -D_REENTRANT -fno-strict-aliasing -DINTEL -Wno-unused-value -Wno-format -c -o gc/ptst.o gc/ptst.c
#g++ -O3 -DNDEBUG -DMUTEX -m64 -D_REENTRANT -fno-strict-aliasing -I./atomic_ops -DINTEL -Wno-unused-value -Wno-format -fPIC -c -o fptree.o fptree.cpp -m64 -lpthread
g++ -O3 -DHAVE_CONFIG_H -DNDEBUG -DMUTEX -m64 -D_REENTRANT -fno-strict-aliasing -DINTEL -Wno-unused-value -Wno-format -I. -I.. -c -o funcForC.o funcForC.c -lpthread -lpmemobj -lpmem
