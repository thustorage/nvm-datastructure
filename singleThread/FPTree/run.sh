#!/bin/bash

numactl --cpunodebind=1 ./main 
rm /home/fkd/CPTree-202006/mount/pool