#pragma once

#include <cstdlib>
#include <sys/time.h>
#include "util.h"
namespace nvindex{

class timer {
 public:

  timer() {
    total.tv_sec = total.tv_usec = 0;
    diff.tv_sec = diff.tv_usec = 0;
  }

  double duration() {
    double duration;

    duration = (total.tv_sec) * 1000000.0;      // sec to us
    duration += (total.tv_usec);      // us

    return duration*1000.0;  //ns
  }

  void start() {
    gettimeofday(&t1, NULL);
  }

  void end() {
    gettimeofday(&t2, NULL);
    timersub(&t2, &t1, &diff);
    timeradd(&diff, &total, &total);
  }

  void reset(){
    total.tv_sec = total.tv_usec = 0;
    diff.tv_sec = diff.tv_usec = 0;
  }

  timeval t1, t2, diff;
  timeval total;
};

class cpuCycleTimer{
public:
  long long t1, t2, total;
  int count = 0;

  cpuCycleTimer(){
    reset();
  }
  void start(){
      t1 = rdtsc();
  }
  void end(){
      t2 = rdtsc();
      total += t2 - t1;
      count++;
  }
  void reset(){
    count=t1=t2=total=0;
  }
  double duration(){ // ns
    return total/CPU_FREQUENCY;
  }

};

} //nvindex
