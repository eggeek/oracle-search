/**
 * This file is to create CPDs by auto partitioning:
 * 0. assuming cpd type is reverse table (other types are not currently supported)
 * 1. make one or more cpds files on a single worker
 * 2. write config file for each cpd file
 * 3. cpd filenames are auto generated, if user wants to specify filename, using `make_cpd` instead
 * 4. cpd filename format is <map>-<wid>-<bid>.cpd 
 *  
 * Example:
 *  - Map file: melb-both.xy, the graph has 167760 nodes
 *  - We have 5 workers, and distribute by method `{div, 9000}` (see definition in distribution_controller.h).
 *  - Then there will be 19 blocks, each with size 9000, except the last one, and id of them are 0, 1, ... 18.
 *  - We distribute these blocks to 5 workers (ids: 0, 1, 2, 3, 4):
 *    - 0:[0, 1, 2, 3], 
 *    - 1:[4, 5, 6, 7], 
 *    - 2:[8, 9, 10, 11], 
 *    - 3:[12, 13, 14, 15], 
 *    - 4:[16, 17, 18]
 *  - On worker=4, we will create 3 cpd files and the corresponding config
 *    - melb-both-4-16.cpd, melb-both-4-16.config
 *    - melb-both-4-17.cpd, melb-both-4-17.config
 *    - melb-both-4-18.cpd, melb-both-4-18.config
 *  - In each config, we describe the graph filename, the distribution method ("{div, 9000}"), 
 *    the cpd method ("reverse table"), and the worker id, see details in function "write_conf".
 *
 */
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <getopt.h>
#include <numeric>
#include <omp.h>
#include <vector>

#include "bidirectional_graph_expansion_policy.h"
#include "cfg.h"
#include "constants.h"
#include "graph_oracle.h"
#include "oracle_listener.h"
#include "log.h"
#include "xy_graph.h"
#include "distribution_controller.h"

using namespace std;

distribute::DistributeController dc;

template<warthog::cpd::symbol S>
int
make_cpd(warthog::graph::xy_graph &g, warthog::cpd::graph_oracle_base<S> &cpd,
         std::vector<warthog::cpd::oracle_listener*> &listeners,
         std::string cpd_filename, std::vector<warthog::sn_id_t> &nodes,
         bool reverse, uint32_t seed, bool verbose=false)
{
    unsigned char pct_done = 0;
    uint32_t nprocessed = 0;
    size_t node_count = nodes.size();

    warthog::timer t;
    t.start();

    info(verbose, "Computing node ordering.");
    cpd.compute_dfs_preorder(seed);

    info(verbose, "Computing Dijkstra labels.");
    std::cerr << "progress: [";
    for(uint32_t i = 0; i < 100; i++) { std::cerr <<" "; }
    std::cerr << "]\rprogress: [";

    #ifndef SINGLE_THREADED
    #pragma omp parallel
    #endif
    {
        int thread_count = omp_get_num_threads();
        int thread_id = omp_get_thread_num();
        size_t start_id = thread_id;
        warthog::sn_id_t source_id;

        std::vector<warthog::cpd::fm_coll> s_row(g.get_num_nodes());
        // each thread has its own copy of Dijkstra and each
        // copy has a separate memory pool
        warthog::bidirectional_graph_expansion_policy expander(&g, reverse);
        warthog::zero_heuristic h;
        warthog::pqueue_min queue;
        warthog::flexible_astar<
            warthog::zero_heuristic,
            warthog::bidirectional_graph_expansion_policy,
            warthog::pqueue_min,
            warthog::cpd::oracle_listener>
            dijk(&h, &expander, &queue);

        listeners.at(thread_id)->set_run(&source_id, &s_row);
        dijk.set_listener(listeners.at(thread_id));

        while (start_id < node_count)
        {
            source_id = nodes.at(start_id);
            cpd.compute_row(source_id, &dijk, s_row);
            // We increment the start_id by the number of threads to *jump* to
            // that id in the vector.
            start_id += thread_count;
            #pragma omp critical
            {
                nprocessed++;

                if ((nprocessed * 100 / node_count) > pct_done)
                {
                    std::cerr << "=";
                    pct_done++;
                }
            }
        }
    }

    std::cerr << std::endl;
    // convert the column order into a map: from vertex id to its ordered index
    cpd.value_index_swap_array();

    info(verbose, "total preproc time (seconds):", t.elapsed_time_sec());

    std::ofstream ofs(cpd_filename);

    if (!ofs.good())
    {
        std::cerr << "Could not open CPD file " << cpd_filename << std::endl;
        return EXIT_FAILURE;
    }

    info(verbose, "Writing results to", cpd_filename);
    ofs << cpd;
    ofs.close();

    for (auto l : listeners)
    {
        delete l;
    }

    return EXIT_SUCCESS;
}

void write_conf(
  const string& conf_file, 
  const string& xyfile,
  const string& method,
  int partkey, int wid, int bid) {
    std::ofstream ofs(conf_file);
    ofs << "xyfile,method,methodkey,wid,bid,cpdtype" << std::endl;
    ofs << xyfile << "," << method << "," << partkey << "," << wid << ","
        << wid << "," << bid << "," << "reverse-table" << std::endl;
}

string format_cpdfile(string graphfile, string outdir, int wid, int bid) {
  // remove ".xy"
  string res = graphfile.substr(0, graphfile.find_last_of("."));
  if (!outdir.empty()) {
    res = outdir + "/" + res.substr(res.find_last_of("\\/"));
  }
  return res + "-" + to_string(wid) + "-" + to_string(bid) + ".cpd";
}

int
main(int argc, char *argv[])
{
    int verbose = 0;
    string outdir = "";
    // default behaviour of partitioning is running everything on this worker
    string partition = "mod"; // default method id mod
    int partkey = 1;          // default behavior is not partition at all
    int workerid = 0;         // default worker is 0 
    int maxworker = 1;        // default maxworker is 1
    warthog::util::param valid_args[] =
    {
        // define the partition method
        {"partition", required_argument, 0, 1},
        // parameter of the partition method
        {"partkey", required_argument, 0, 1},
        // the id of this worker
        {"workerid", required_argument, 0, 1},
        {"input", required_argument, 0, 1},
        {"outdir", required_argument, 0, 1},
        {"seed", required_argument, 0, 1},
        {"maxworker", required_argument, 0, 1},
        {"verbose", no_argument, &verbose, 1},
        {0, 0, 0, 0}
    };

    warthog::util::cfg cfg;
    cfg.parse_args(argc, argv, valid_args);

    bool reverse = true;

    std::string xy_filename = cfg.get_param_value("input");

    if (xy_filename == "")
    {
        std::cerr << "Required argument --input [xy graph] missing."
                  << std::endl;
        return EXIT_FAILURE;
    }

    // We save the incoming edges in case we are building a reverse CPD
    warthog::graph::xy_graph g(0, "", reverse);
    std::ifstream ifs(xy_filename);

    if (!ifs.good())
    {
        std::cerr << "Cannot open file " << xy_filename << std::endl;
        return EXIT_FAILURE;
    }

    ifs >> g;
    ifs.close();

    // parse distribution method
    if (cfg.get_num_values("maxworker") > 0)
      maxworker = stoi(cfg.get_param_value("maxworker"));
    if (cfg.get_num_values("partition") > 0)
      partition = cfg.get_param_value("partition");
    if (cfg.get_num_values("partkey") > 0)
      partkey = stoi(cfg.get_param_value("partkey"));
    if (cfg.get_num_values("outdir") > 0)
      outdir = cfg.get_param_value("outdir");
    if (cfg.get_num_values("workerid") > 0)
      workerid = stoi(cfg.get_param_value("workerid"));

    dc = distribute::DistributeController(g.get_num_nodes(), maxworker, workerid);
    dc.set_method(partition, partkey);

    std::string s_seed = cfg.get_param_value("seed");
    uint32_t seed;

    if (s_seed != "")
    {
        seed = std::stoi(s_seed);
    }
    else
    {
        seed = ((uint32_t)rand() % (uint32_t)g.get_num_nodes());
    }

    #ifdef SINGLE_THREADED
    size_t nthreads = 1;
    #else
    size_t nthreads = omp_get_max_threads();
    #endif

    bool failed;
    for (auto& nodes: dc.get_worker_blocks()) {
      int bid = dc.get_blockid(nodes.back());
      string cpd_filename = format_cpdfile(xy_filename, outdir, dc.wid, bid);
      // remove ".cpd" suffix
      string config = cpd_filename.substr(0, cpd_filename.find_last_of(".")) + ".conf";

      write_conf(config, xy_filename, partition, partkey, dc.wid, bid);

      warthog::cpd::graph_oracle_base<warthog::cpd::REV_TABLE> cpd(&g);

      // We have to explicitly create and pass the different (sub-) types of
      // oracles and listeners or it messes with the template resolution.
      std::vector<warthog::cpd::oracle_listener*> listeners(nthreads);
      for (size_t t = 0; t < nthreads; t++)
      {
          listeners.at(t) = new warthog::cpd::reverse_oracle_listener<
              warthog::cpd::REV_TABLE>(&cpd);
      }

      failed = make_cpd<warthog::cpd::REV_TABLE>(
          g, cpd, listeners, cpd_filename, nodes, reverse, seed,
          verbose);

      if (failed)
        return EXIT_FAILURE;
    }
    return EXIT_SUCCESS;
}
