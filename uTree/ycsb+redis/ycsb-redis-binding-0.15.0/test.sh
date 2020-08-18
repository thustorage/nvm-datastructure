#!/bin/bash

for task in a # a d
do

echo load workload${task}
./bin/ycsb load redis -s -threads 24 -P workloads/workload${task} > outputLoad_${task}.txt

for thread_num in 1 2 4 8 12 16 20 24
do

echo workload${task} : ${thread_num} client threads VS $1 server threads
./bin/ycsb run redis -s -threads ${thread_num} -P workloads/workload${task} > fastfair_nobind_1kw/${task}_${thread_num}.txt

sleep 3

done

done