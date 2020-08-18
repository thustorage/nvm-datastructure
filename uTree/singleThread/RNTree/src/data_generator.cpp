#include "util.h"
#include <getopt.h>
#include <string>
#include <iostream>
#include <fstream>

using namespace std;

static struct option dataopts[] = {
	{ "help", no_argument, NULL, 'h' },
	{ "type", required_argument, NULL, 't' },
	{ "filename", required_argument, NULL, 'f' },
	{ "skew", required_argument, NULL, 's'},
	{ "datasize", required_argument, NULL, 'd'},
};

enum DataDistrubute{
	RANDOM,
	ZIPFIAN,
	_DataDistrbuteNumber
};

DataDistrubute distribute = RANDOM;
std::string filename;
float zipfp;
unsigned long long datasize = 1llu << 28;   // 64MB input;

static const uint64_t kFNVPrime64 = 1099511628211;
static inline unsigned int hashfunc(uint32_t val) {
	uint32_t hash = 123;
	int i;
	for (i = 0; i < sizeof(uint32_t); i++) {
		uint64_t octet = val & 0x00ff;
		val = val >> 8;

		hash = hash ^ octet;
		hash = hash * kFNVPrime64;
	}
	return hash;
}	

int main(int argc,char** argv){
	while(true){
		int idx = 0;
		int c = getopt_long(argc, argv, "ht:f:d:s:", dataopts, &idx);
		if (c == -1)
			break;
		switch(c){
			case 't':
				distribute = (DataDistrubute) atoi(optarg);
				break;
			case 'f':
				filename = std::string(optarg);
				break;
			case 's':
				zipfp = atof(optarg);
				break;
			case 'd':
				datasize = atoi(optarg);
				break;
			case 'h':
			default:
				fprintf(stdout,"Command line options : %s <options> \n"
							"   -h --help         : Print help message \n"
							"   -t --type         : 0 (random) 1 (zipfian)\n"
							"   -s --skew         : zipfian skew parameter\n"
							"   -f --filename     : output filename\n"
							"   -d --datasize     : data size\n"
							, argv[0]);
				exit(EXIT_FAILURE);
		}
	}

	if (filename.empty()){
		if (distribute == RANDOM){
			filename = string("random_data");
		}else if (distribute == ZIPFIAN){
			filename = string("zipfian_data");
		}
	}

	if (distribute == RANDOM){
		printf("generator random data\n");
		RandomGenerator rdm;
		ofstream myfile;
		myfile.open(filename.c_str(), ios::out|ios::binary);
		for(unsigned long long i=0; i<datasize; i++){
			int d = rdm.Next();
			myfile.write((char*)&d, sizeof(int));
		}
		myfile.close();
	}else{
		printf("generator zipfian data\n");
		ZipfGenerator rdm(zipfp, 1llu<<25);
		ofstream myfile;
		myfile.open(filename, ios::out|ios::binary);
		for(unsigned long long i=0; i<datasize; i++){
			int d = hashfunc(rdm.Next())&((1llu<<31)-1);
			myfile.write((char*)&d, sizeof(int));
		}
		myfile.close();
	}
}
