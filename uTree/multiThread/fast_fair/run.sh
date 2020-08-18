#!/bin/bash

for skewness in 99 #0 90 99
do

for thread_num in 36 #1 4 8 12 16 20 24 28 32 36 
do

echo thread_num = ${thread_num} skewness = ${skewness} 
./main-gu-zipfian -t ${thread_num} -d 5000 -i 10000000 -u 100 -f 0 #> result_0706/u100_0.${skewness}_${thread_num}.txt
rm ../../mount/pmem0/main_pool
rm ../../mount/pmem0/pool-*
rm ../../mount/pmem1/pool-*

sleep 3
done
done


