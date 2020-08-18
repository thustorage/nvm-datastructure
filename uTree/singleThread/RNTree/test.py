#!/home/mengxing/anaconda3/bin//python
#
# @Author: liumx10
# @Date:   2018-04-17 21:45:42
# @Last Modified by:   liumx10
# @Last Modified time: 2018-07-29 15:16:29
#


import os
import sys
import getopt
import numpy as np

TEST_TIME = 3
TREE_SIZE = 24

def runtest(latency, tree, benchmark, initkeys, thread, workload, skewness, scan_length, read_ratio):
    duration = 0.1 if benchmark == 3 else 5
    command = "NVM_LATENCY={} ./build/benchmark -n {} -t {} -b {} -k {} -d {} -w {} -S {} -l {} -r {}|\
         grep -Eo '([0-9]*\.[0-9]*) Mtps'".format(
            latency, thread, tree, benchmark, initkeys, duration, workload, skewness, scan_length, read_ratio)
#    print(command)
    r = os.popen(command)
    for line in r.readlines():
        words = line.split()
        return float(words[0])

def multi_times_test(latency, tree, benchmark, initkeys, thread=1, workload=0, skewness=0.99, scan_length=10, read_ratio=50):

    result = []
    for i in range(0, TEST_TIME):
        res = runtest(latency, tree, benchmark, initkeys, thread, workload, skewness, scan_length, read_ratio)
        result.append(res)

    print(latency, thread, tree, benchmark, initkeys, np.average(result))
    return np.average(result), np.max(result), np.min(result)


def single():
    print("single thread test")
    f = open("singlethread.csv", "w")

    for latency in [0]:
        f.write('[\n')
        for tree in [6,5,2,0,1]:
            f.write('[')
            for benchmark in range(0, 5):
                f.write(str(multi_times_test(latency, tree, benchmark, TREE_SIZE)[0]))
                f.write(',')
            f.write(']\n')
        f.write(']\n')
    f.close()

def find():
    print("differen work size for find")
    f = open("find.csv", "w")

    for tree in [0, 3, 2]:
        f.write('[')
        for k in [14, 16, 18, 20, 22, 24, 26]:
            f.write(str(multi_times_test(0, tree, 0, k)[0]))
            f.write(',')
        f.write(']\n')


def multicore():
    print("multiply thread test")
    f = open("multithread.csv", "w")

    for workload in [0]:
        f.write('[\n')
        for tree in [1, 2, 3]:
            f.write('[')
            for thread in [1, 2, 4, 8, 16, 24]:
                f.write(str(multi_times_test(0, tree, 5, 20, thread, workload, 0.8, 10, 50)[0]))
                f.write(',')
            f.write(']\n')
        f.write(']\n')
    f.close()

def skew():
    print("skew test")
    f = open("skew.csv", "w")
    for tree in [1, 2, 3]:
        f.write('[')
        for s in [0.55, 0.6, 0.65, 0.68, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95, 0.99]:
            f.write(str(multi_times_test(0, tree, 5, 20, 8, 1, s)[0]))
            f.write(',')
        f.write(']\n')
    f.close()

def scan():
    print("scan test")
    f = open("scan.csv", "w")
    for tree in [0, 2, 3]:
        f.write('[')
        #for length in [20]:
        for length in [5, 10, 50, 100, 200, 500, 1000]:
            f.write(str(multi_times_test(0, tree, 10, 20, 1, 0, 0, length)[0]))
            f.write(',')
        f.write('],\n')

def read_ratio():
    print("read/update ratio test")
    f = open("read_ratio.csv", "w")
    for tree in [1, 2, 3]:
        f.write('[')
        for r in range(10, 100, 10):
            f.write(str(multi_times_test(0, tree, 5, 20, 22, 1, 0.7, read_ratio=r)[0]))
            f.write(",")
        f.write('],')

def main():
    try:
        options, args = getopt.getopt(sys.argv[1:], "sfmSlr", ["single", "find", "multicore", "skew", "scan", "read"])
    except:
        sys.exit()

    for name, value in options:
        if name in ('-s', '--single'):
            single()
            sys.exit()
        if name in ('-f', '--find'):
            find()
            sys.exit()
        if name in ('-S', '--skew'):
            skew()
            sys.exit()
        if name in ('-m', '--multicore'):
            multicore()
            sys.exit()
        if name in ('-l', '--scan'):
            scan()
            sys.exit()
        if name in ('-r', '--read'):
            read_ratio()
            sys.exit()


if __name__ == "__main__":
    main()
