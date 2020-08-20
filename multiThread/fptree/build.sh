#!/bin/bash

gcc -O3 -DNDEBUG -DMUTEX -m64 -D_REENTRANT -fno-strict-aliasing -DINTEL -Wno-unused-value -Wno-format -c -o gc/ptst.o gc/ptst.c
g++ -O0 -DNDEBUG -DMUTEX -m64 -g -D_REENTRANT -fno-strict-aliasing -I./atomic_ops -DINTEL -Wno-unused-value -Wno-format -fPIC -c -o fptree.o fptree.cpp -m64 -lpthread
ar crv libfptree.a fptree.o gc/ptst.o
g++ -O3 -std=c++11 -DNDEBUG -DMUTEX -m64 -D_REENTRANT -fno-strict-aliasing -I./atomic_ops -DINTEL -Wno-unused-value -Wno-format  -o ./main-gu-zipfian gc/ptst.o main-gu-zipfian.c -m64 -L. -lfptree -lpmemobj -lpmem -lpthread
