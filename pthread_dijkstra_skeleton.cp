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
#include <mutex>          // std::mutex

#include <queue>
#include "barrier.h"

// #include <bits/stdc++.h>


using std::string;
using std::cout;
using std::endl;
using std::vector;
using namespace std;

std::mutex mtx; 


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
   int operator() ( minHelper& p1,  minHelper& p2)
    {
        return p1.getDis() > p2.getDis();
    }
 };  

struct argumentsData{
    int pRank;
    int *distanceList;
    int *predicateList;
    int *traversedList;

};

pthread_barrier_t calc_barrier;

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
                    20)); // input matrix should be smaller than 20MB * 20MB (400MB, we don't have two much memory for multi-processors)
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
int find_local_minimum(int *dist, bool *visit,int start, int end) {
    int min = INT_MAX;
    int u = -1;
    for (int i = start; i < end; i++) {
        if (!visit[i]) {
            if (dist[i] < min) {
                min = dist[i];
                u = i;
            }
        }
    }
    return u;
}
struct dijekstraArguments{
    int rank;
    int *distArray;
    int *predic;
    
};

//Hint: use pthread condition variable or pthread barrier to do the synchronization
//------You may add helper functions and global variables here------



void dijkstra(int N, int p, int *mat, int *all_dist, int *all_pred) {
    //------your code starts from here------



    globalMinInd[0]=10000000;

    long thread = 0;
    pthread_t* thread_handles;
    thread_handles = (pthread_t*) malloc (p*sizeof(pthread_t)); 
    pthread_barrier_init(&calc_barrier, NULL, p);

    for (int i = 0; i < utils::N; i++) {
        all_dist[i] = utils::mat[utils::convert_dimension_2D_1D(0, i)];
        all_pred[i] = 0;
        visit[i] = false;
        minHelper mh((utils::mat[utils::convert_dimension_2D_1D(0, i)]),i);
        pq.push(mh);
    }

    all_dist[0]=0;

    visit[0] = true;

   //     cout<<"ready";

    dijekstraArguments *dj = new dijekstraArguments[p];
    for (int i =0; i< p; i++){
        dj[i].rank = i;
        dj[i].distArray = all_dist;
        dj[i].predic = all_pred;
    }

    for (thread = 0; thread < p; thread++)  {

     //   cout<<"passing";
        pthread_create(&thread_handles[thread], NULL,
        parallelWork, (void*) &dj[thread]);  
    }

    //printf("Hello from the main thread\n");

    for (thread = 0; thread < p; thread++) 
        pthread_join(thread_handles[thread], NULL); 


    //------end of your code------
}


int main(int argc, char **argv) {
    assert(utils::parse_args(argc, argv) == 0);
    assert(utils::read_file(utils::filename) == 0);

    assert(utils::num_threads <= utils::N);
    //`all_dist` stores the distances and `all_pred` stores the predecessors

    int *all_dist;
    int *all_pred;

    all_dist = (int *) calloc(utils::N, sizeof(int));
    all_pred = (int *) calloc(utils::N, sizeof(int));
    visit = (bool *) malloc(utils::N * sizeof(bool));


    //time counter
    timeval start_wall_time_t, end_wall_time_t;
    float ms_wall;

    //start timer
    gettimeofday(&start_wall_time_t, nullptr);

    dijkstra(utils::N, utils::num_threads, utils::mat, all_dist, all_pred);

    //end timer
    gettimeofday(&end_wall_time_t, nullptr);
    ms_wall = ((end_wall_time_t.tv_sec - start_wall_time_t.tv_sec) * 1000 * 1000
               + end_wall_time_t.tv_usec - start_wall_time_t.tv_usec) / 1000.0;

    std::cerr << "Time(ms): " << ms_wall << endl;

    utils::print_result(all_dist, all_pred);

    free(utils::mat);
    free(all_dist);
    free(all_pred);

    return 0;
}

bool traversedAll(){
    for(int i = 0; i <utils::N; i ++){
        if (visit[i] == 0){

            return 0;
        }
    }

    return 1;
}

void *parallelWork(void* _rank) {

    
    struct dijekstraArguments *arg = (dijekstraArguments *)_rank;
    
   long my_rank =  arg->rank;  /* Use long in case of 64-bit system */
  //  cout<<"rank is "<<my_rank;

    int startInd = (int)my_rank* utils::N/utils::num_threads;
    int end = (int)(my_rank + 1)* utils::N/utils::num_threads;
    int current;
    while(!pq.empty() && !traversedAll()){
        pthread_barrier_wait(&calc_barrier);
        if(my_rank == 0){
            // cout<<"hell0";

           minHelper min(pq.top());
           pq.pop();
           // cout<<"min is  "<<min.getDis()<<"vertex is "<<min.getInd()<<endl;
           globalMinInd[0] = min.getDis();
           globalMinInd[1] = min.getInd();
           visit[min.getInd()]=1;
        }
        pthread_barrier_wait(&calc_barrier);

        // mtx.lock();
        // cout<<"i am thread "<<my_rank<<"my vertices range is "<<startInd<<" to "<<end<<"\n";
        // mtx.unlock();

        for (int v = startInd; v < end; v++) { //        for (int v = 1; v < utils::N; v++) {
            int u = globalMinInd[1];

            if(utils::mat[utils::convert_dimension_2D_1D(u, v)]>=1000000 || v == u || visit[v]==1) //check for already traversed. 
                continue;
            else{
                mtx.lock();
        
                // cout<<"adjacent node for "<<u <<"is "<<v<<"\n";
                mtx.unlock();
            }

            // int u = globalMinInd[1];
            int sum = arg->distArray[u] + utils::mat[utils::convert_dimension_2D_1D(u, v)];
           // cout<<"---------------sum is "<<all_dist[v]<<"+"<<utils::mat[utils::convert_dimension_2D_1D(u, v)]<<"= "<<sum<<"  and all dist "<<all_dist[v]<<"\n";
            if(arg->distArray[v]>sum){
                mtx.lock();
                // cout<<"updating value for vertix "<<v<<"from "<<all_dist[v]<<" to"<<sum<<"\n";
                pq.push(minHelper(sum,v));
                mtx.unlock();

                arg->distArray[v] = sum;
                arg->predic[v] = u;

            }

//            if(all_dist[v]>sum)

        }



    }


    return NULL;
}

