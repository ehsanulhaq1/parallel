/* Name: HAQ, EHSAN-UL
 * ID: 
 * Email:  euhaq@connect.ust.hk
 */

/*
 * This is code skeleton for COMP5112-17Spring assignment2
 * Compile: g++ -std=c++11 -lpthread -o pthread_dijkstra pthread_dijkstra.cpp
 * Run: ./pthread_dijkstra -n <number of threads> -i <input file>,
 * you will find the output in 'output.txt' file
 */


/*---- not complete yet 

#include <string>
#include <cassert>
#include <iostream>
#include <fstream>
#include <vector>
#include <climits>
#include <cstring>
#include <algorithm>
#include <sys/time.h>
#include <time.h>
#include <getopt.h>

#include <pthread.h>
//#include <mutex>         

#include <queue>
//#include "barrier.h"



using std::string;
using std::cout;
using std::endl;
using std::vector;
using namespace std;

//std::mutex mtx; 


class minHelper{
   int distance;
   int index;
   
   public:
    minHelper(){
        distance = 0;
        index = 0;
    }
   minHelper(int _distance, int _index)
   {
      distance = _distance;
      index = _index;
   }

   int getDis()  { return distance; }
   int getInd()  { return index; }

};

struct compare  
 {  
   int operator() ( minHelper& v1,  minHelper& v2)
    {
        return v1.getDis() > v2.getDis();
    }
 };  

struct argumentsData{
    int pRank;
    int *distanceList;
    int *predicateList;
    int *traversedList;

};

pthread_barrier_t dij_barrier;
pthread_mutex_t mutex;
bool *visit;

int globalMinInd[2];
int *finalDistance;
priority_queue<minHelper,vector<minHelper>, compare > pq; 



void *parallelWork(void* rank);

/*
 * utils is a namespace for utility functions
 * including I/O (read input file and print results) and one matrix dimension convert(2D->1D) function
 */
namespace utils {
    int num_threads; //number of thread
    int N; //number of vertices
    int *mat; // the adjacency matrix

    string filename; // input file name
    string outputfile; //output file name, default: 'output.txt'

    void print_usage() {
        cout << "Usage:\n" << "\tpthread_dijkstra -n <number of threads> -i <input file>" << endl;
        exit(0);
    }

    int parse_args(int argc, char **argv) {
        filename = "";
        outputfile = "output.txt";
        num_threads = 0;

        int opt;
        if (argc < 2) {
            print_usage();
        }
        while ((opt = getopt(argc, argv, "n:i:o:h")) != EOF) {
            switch (opt) {
                case 'n':
                    num_threads = atoi(optarg);
                    break;
                case 'i':
                    filename = optarg;
                    break;
                case 'o':
                    outputfile = optarg;
                    break;
                case 'h':
                case '?':
                default:
                    print_usage();
            }
        }
        if (filename.length() == 0 || num_threads == 0)
            print_usage();
        return 0;
    }

    /*
     * convert 2-dimension coordinate to 1-dimension
     */
    int convert_dimension_2D_1D(int x, int y) {
        return x * N + y;
    }

    int read_file(string filename) {
        std::ifstream inputf(filename, std::ifstream::in);
        inputf >> N;
        assert(N < (1024 * 1024 *
                    20)); 
             //input matrix should be smaller than 20MB * 20MB (400MB, we dont have two much memory for multi-processors)
        mat = (int *) malloc(N * N * sizeof(int));
        for (int i = 0; i < N; i++)
            for (int j = 0; j < N; j++) {
                inputf >> mat[convert_dimension_2D_1D(i, j)];
            }

        return 0;
    }

    string format_path(int i, int *pred) {
        string out("");
        int current_vertex = i;
        while (current_vertex != 0) {
            string s = std::to_string(current_vertex);
            std::reverse(s.begin(), s.end());
            out = out + s + ">-";
            current_vertex = pred[current_vertex];
        }
        out = out + std::to_string(0);
        std::reverse(out.begin(), out.end());
        return out;
    }

    int print_result(int *dist, int *pred) {
        std::ofstream outputf(outputfile, std::ofstream::out);
        outputf << dist[0];
        for (int i = 1; i < N; i++) {
            outputf << " " << dist[i];
        }
        for (int i = 0; i < N; i++) {
            outputf << "\n";
            if (dist[i] >= 1000000) {
                outputf << "NO PATH";
            } else {
                outputf << format_path(i, pred);
            }
        }
        outputf << endl;
        return 0;
    }
}//namespace utils

int find_global_minimum(int *dist, bool *visit) {
    int min = INT_MAX;
    int u = -1;
    for (int i = 0; i < utils::N; i++) {
        if (!visit[i]) {
            if (dist[i] < min) {
                min = dist[i];
                u = i;
            }
        }
    }
    return u;
}


