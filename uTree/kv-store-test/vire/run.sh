#!/bin/bash

rm log
rm /home/fkd/CPTree-202006/mount/pmem0/main_pool
rm /home/fkd/CPTree-202006/mount/pmem0/pool-*
rm /home/fkd/CPTree-202006/mount/pmem1/pool-*
echo $1 threads
src/vire -c conf/vire.conf -p pid_file -v 0 -o log -T $1 -d
