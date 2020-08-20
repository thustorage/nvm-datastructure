#pragma once

#include <iostream>
#include <cassert>
#include <unistd.h>
#include <getopt.h>

enum IndexType
{
  NV_TREE,
  FP_TREE,
  RN_TREE,
  RN_TREE_R,
  WB_TREE,
  WB_TREE_2,
  _IndexTypeNumber
};

enum DataDistrubute
{
  RANDOM,
  ZIPFIAN,
  _DataDistrbuteNumber
};

enum BenchMarkType
{
  READ_ONLY,
  INSERT_ONLY,
  UPDATE_ONLY,
  DELETE_ONLY,
  MIXED_BENCH,

  YCSB_A,
  YCSB_B,
  YCSB_C,
  YCSB_D,
  YCSB_E,

  SCAN_BENCH,
  RECOVERY_BENCH,
  _BenchMarkType
};

struct Config
{
  IndexType type;
  BenchMarkType benchmark;

  int num_threads;
  unsigned long long init_keys;
  int time;
  bool share_memory;
  float duration;

  std::string filename;
  DataDistrubute workload;
  int read_ratio; // for read-upadte benchmark, (read_ratio)%.

  float skewness;
  int scan_length;
  int throughput;
  bool latency_test;

  void report(){
      printf("--- Config ---\n");
      printf("type:\t %d\nbenchmark:\t %d\nthreads:\t %d\ninit_keys:\t %lld\n",
              type, benchmark, num_threads, init_keys);
      printf("--------------\n");
  }
};

static struct option opts[] = {
    {"help", no_argument, NULL, 'h'},
    {"type", required_argument, NULL, 't'},
    {"num_threads", required_argument, NULL, 'n'},
    {"keys", required_argument, NULL, 'k'},
    {"share_memory", no_argument, NULL, 's'},
    {"duration", required_argument, NULL, 'd'},
    {"benchmark", required_argument, NULL, 'b'},
    {"filename", required_argument, NULL, 'f'},
    {"workload", required_argument, NULL, 'w'},
    {"skewness", required_argument, NULL, 'S'},
    {"scan_length", required_argument, NULL, 'l'},
    {"read_ratio", required_argument, NULL, 'r'},
};

static void usage_exit(FILE *out)
{
  fprintf(out, "Command line options : nstore <options> \n"
               "   -h --help              : Print help message \n"
               "   -t --type              : Index type : 0 (NV_TREE) 1 (FP_TREE) 2(RN_TREE) 3(RN_TREE_R) 4(WB_TREE) 5(WB_TREE_2)\n"
               "   -n --num_threads       : Number of workers \n"
               "   -k --keys              : Number of key-value pairs at begin\n"
               "   -s --non_share_memory  : Use different index instances among different workers\n"
               "   -d --duration          : Execution time\n"
               "   -b --benchmark         : Benchmark type, 0-%d\n"
               "   -w --workload          : type of workload: 0 (RANDOM) 1 (ZIPFIAN)\n"
               "   -S --skewed            : skewness: 0-1 (default 0.99)\n"
               "   -l --scan_length       : scan_length: int (default 100)\n"
               "   -r --read_ratio        : read ratio: int (default 50)\n",
          _BenchMarkType - 1);
  exit(EXIT_FAILURE);
}

static void parse_arguments(int argc, char *argv[], Config &state)
{

  // Default Values
  state.type = NV_TREE;
  state.num_threads = 4;
  state.init_keys = 1000000;
  state.time = 5;
  state.share_memory = true;
  state.duration = 1;
  state.benchmark = INSERT_ONLY;
  state.workload = RANDOM;
  state.skewness = 0.99;
  state.scan_length = 100;
  state.read_ratio = 50;
  state.throughput = 10000000;
  state.latency_test = false;

  // Parse args
  while (1)
  {
    int idx = 0;
    int c = getopt_long(argc, argv, "f:t:n:k:sd:b:w:S:l:r:T:", opts,
                        &idx);

    if (c == -1)
      break;

    switch (c)
    {
    case 'b':
      state.benchmark = (BenchMarkType)atoi(optarg);
      break;
    case 'd':
      state.duration = atof(optarg);
      break;
    case 't':
      state.type = (IndexType)atoi(optarg);
      break;
    case 'n':
      state.num_threads = atoi(optarg);
      break;
    case 'k':
      state.init_keys = (1llu << atoi(optarg));
      break;
    case 's':
      state.share_memory = false;
      break;
    case 'f':
      state.filename = std::string(optarg);
      break;
    case 'w':
      state.workload = (DataDistrubute)atoi(optarg);
      break;
    case 'S':
      state.skewness = atof(optarg);
      break;
    case 'l':
      state.scan_length = atoi(optarg);
      break;
    case 'r':
      state.read_ratio = atoi(optarg);
      break;
    case 'T':
      state.throughput = atoi(optarg);
      state.latency_test = true;
      break;
    case 'h':
      usage_exit(stdout);
      break;
    default:
      fprintf(stderr, "\nUnknown option: -%c-\n", c);
      usage_exit(stderr);
    }
  }
  //state.report();

}
