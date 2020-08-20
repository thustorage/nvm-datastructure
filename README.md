# uTree: a Persistent B+Tree with Low Tail Latency (VLDB 2020)

## uTree

* we propose a B+-tree variant named μTree to achieve both *high throughput* and *low tail latency*. It incorporates a shadow list-based layer to the leaf nodes of a B+-tree to gain benefits from both list and tree data structures. The list layer in PM is exempt from the structural refinement operations since list nodes in the list layer own separate PM spaces, which are organized in an _element-based_ way. Meanwhile, μTree still gains the locality benefit from the tree-based nodes. To alleviate the interference overhead, μTree coordinates the concurrency control between the tree and list layer, which moves the slow PM accesses out of the critical path.

<p align="center">
<img src="./Documentation/uTree-overview.PNG" >
</p>

## Tutorial

* This repo consists of three parts:
  * Single Thread Evaluation
  * Multiple Thread Evaluation
  * Key-Value Store Evaluation
* uTree use a `tail pointer` to allocate space in PM, while other indexes allocate space in PM with PMDK.

### Dependencies

* gcc: 7.5.0
* CMake: 3.10.2
* pmdk-tools: 1.4.1
* ipmctl: 02.00.00.3545
* ndctl: 66
* automake: 1.15.1
* autoconf: 2.69
* bzip2: 1.0.6

### Prepare

1. Set Optane DCPMM to `App-Direct` mode

```
    $ ipmctl create -f -goal persistentmemorytype=appdirect
```

2. Create namespace

```
    $ ndctl create-namespace --mode=devdax -f
```

3. If use PMDK, set mode of namespace to `fsdax`, assume step 2 created a namespace named "namespace1.0"

```
    $ ndctl create-namespace -e namespace1.0 --mode=fsdax -f
```

4. Format current AEP device and mount it to the corresponding location (i.e. ./mount/pmem0), assume step 3 create an AEP device /dev/pmem0

```
    $ mkfs.ext4 /dev/pmem0
    $ mount -o dax /dev/pmem0 ./mount/pmem0
```

### Single Thread Evaluation

* In single thread evaluation, we implemented wB+-Tree, NV-Tree, FAST&FAIR, FPTree and Skip List (a persistent version). Note that the single-thread version of FPTree and NV-Tree have also been implemented by [Liu et al.](https://github.com/liumx10/ICPP-RNTree), you can also refer to their code accordingly.
* For uTree, wB+-Tree, FAST&FAIR and Skip List, after entering the related folder, compile with `build.sh` and run tests with `run.sh`, `main.cpp` is the single-thread test program.

```
    $ ./build.sh
    $ ./run.sh
```

* Liu et al's implementation is placed under `RNTree/`, their single-thread test programs are placed under `RNTree/test/`, execute the following commands. Remember to bind cpu properly to avoid cross-NUMA accessing.

```
    $ mkdir build
    $ cd build
    $ cmake ..
    $ make 
    $ ../run.sh
```

### Multiple Thread Evaluation

* In multiple thread evaluation, we compare μTree with FAST&FAIR and FPTree under different update ratio and skewness.
* skewness is set via `kZipfianConst` in `zipfian.h`. `main-gu-zipfian.c` is the test program. There are several important options:

```
    -t: Number of threads
    -i: Number of elements to insert before test
    -d: Test duration in milliseconds
    -u: Percentage of update transactions
```

* After entering the corresponding dirctory, compile with `build.sh` and run tests with `run.sh`.

```
    $ ./build.sh
    $ ./run.sh
```

### Key-Value Store Evaluation

* We use Redis, an in-memory key-value store to evaluate μTree in real-world environments. Redis is modified to support multi-thread execution, and we replace its storage engine with our index structures.
* Execute the following commands. Remember to modify `src/Makefile` before `make` to add funcForC.o to compilation.

```
    $ cd vire
    $ autoreconf -fvi
    $ ./configure --enable-debug=full
    $ cd src/
    $ ./build_funcForC.sh
    $ cd ../
    $ make
    $ ./run.sh
```

## Contact

* Youmin Chen: chenym16@mails.tsinghua.edu.cn
* Youyou Lu: luyouyou@tsinghua.edu.cn
* Kedong Fang: fkd19@mails.tsinghua.edu.cn
* Qing Wang: q-wang18@mails.tsinghua.edu.cn
* Jiwu Shu: shujw@tsinghua.edu.cn

For more NVM-related research, please check out our publication list at [Storage Research Group @Tsinghua University
](http://storage.cs.tsinghua.edu.cn/pub/).

## Cite

If you are using uTree for your work, please cite:

```
Youmin Chen, Youyou Lu, Kedong Fang, Qing Wang, Jiwu Shu. uTree: a Persistent B+-Tree with Low Tail Latency. 
PVLDB, 13(11): 2634-2648, 2020. DOI: https://doi.org/10.14778/3407790.3407850
```
