#include <stdio.h>
#include <stdlib.h>

#include "rtm_tree.h"
#include "util.h"
#include "coordinator.h"


int main(int argc, char **argv)
{
    using namespace nvindex;

    int test_mode = 0;
    if (argc > 1)
        test_mode = atoi(argv[1]);

    unsigned short seeds[3] = {0, 0, 0};
    RandomGenerator rdm;
    rdm.setSeed(seeds);

    const int test_size = 10000;
    RN_tree::Btree <long long, long long, 64> btree;

    register_threadinfo();
    if (test_mode == 0){
        for (int i = 0; i < test_size; i++)
        {
            int d = rdm.Next() % test_size;
            btree.insert(d, d + 1);
        }
        printf("insert successfully..\n");
    }else{
        double time_breaks[3];
        for (int i = 0; i < test_size; i++){
            int d = rdm.Next() % test_size;
            long long r = btree.get(d, time_breaks);
            assert(r == d+1);
        }
        printf("check successfully..\n");
    }

    unregister_threadinfo();
}
