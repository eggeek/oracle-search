//
#include <cstdlib>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <getopt.h>
#include <csignal>
#include <iostream>
#include <fstream>
#include <vector>
#include <omp.h>
#include <json.hpp>

#include "cfg.h"
#include "cpd_extractions.h"
#include "cpd_graph_expansion_policy.h"
#include "cpd_heuristic.h"
#include "cpd_search.h"
#include "json_config.h"
#include "xy_graph.h"
#include "distribution_controller.h"

typedef std::function<void(warthog::search*, config&)> conf_fn;
typedef warthog::sn_id_t t_query;
typedef warthog::cpd::graph_oracle_base<warthog::cpd::REV_TABLE> revtable;

// Defaults
std::string fifo = "/tmp/warthog.fifo";
std::vector<warthog::search*> algos;
warthog::util::cfg cfg;

distribute::DistributeController dc;

//
// - Functions
//
void
signalHandler(int signum)
{
    warning(true, "Interrupt signal", signum, "received.");

    remove(fifo.c_str());

    exit(signum);
}

void create_distribute_controller(int nodenum) {
  std::string partmethod;
  int partkey, wid, maxworker;
  if (cfg.get_num_values("partmethod") > 0 &&
      cfg.get_num_values("partkey") > 0 &&
      cfg.get_num_values("wid") > 0 && 
      cfg.get_num_values("maxworker")) {
    partmethod = cfg.get_param_value("partmethod");
    partkey = stoi(cfg.get_param_value("partkey"));
    wid = stoi(cfg.get_param_value("wid"));
    maxworker = stoi(cfg.get_param_value("maxworker"));
    dc = distribute::DistributeController(nodenum, maxworker, wid);
    dc.set_method(partmethod, partkey);
  } 
  else {
    std::cerr << "Required argument --partmethod, --partkey, --wid, --maxworker" << std::endl;
    exit(EXIT_FAILURE);
  }
  
}

std::string
read_graph_and_diff(warthog::graph::xy_graph& g)
{
    std::ifstream ifs;
    std::string xy_filename = cfg.get_param_value("input");
    if(xy_filename == "")
    {
        std::cerr << "parameter is missing: --input [xy-graph file]\n";
        return "";
    }

    ifs.open(xy_filename);
    if (!ifs.good())
    {
        std::cerr << "Could not open xy-graph: " << xy_filename << std::endl;
        return "";
    }

    ifs >> g;
    ifs.close();

    // Check if we have a second parameter in the --input
    std::string diff_filename = cfg.get_param_value("input");
    if (diff_filename == "")
    {
        diff_filename = xy_filename + ".diff";
    }

    ifs.open(diff_filename);
    if (!ifs.good())
    {
        std::cerr <<
            "Could not open diff-graph: " << diff_filename << std::endl;
        return "";
    }

    g.perturb(ifs);
    ifs.close();

    return xy_filename;
}

template<warthog::cpd::symbol S>
void
read_oracle(std::string cpdfile, warthog::cpd::graph_oracle_base<S>& oracle)
{
    std::ifstream ifs;

    ifs.open(cpdfile);
    if(ifs.is_open())
    {
        ifs >> oracle;
        ifs.close();
    }
    else
    {
        std::cerr << "Could not find the CPD file." << std::endl;
        return;
    }
}

/**
 * The search function does a bunch of statistics out of the search. It takes a
 * configration object, an output pipe and a list of queries and processes them.
 */
void
run_search(conf_fn& apply_conf, config& conf, const std::string& fifo_out,
           const std::vector<t_query> &reqs, double t_read,
           warthog::graph::xy_graph* g)
{
    assert(reqs.size() % 2 == 0);
    size_t n_results = reqs.size() / 2;
    // Statistics
    unsigned int n_expanded = 0;
    unsigned int n_generated = 0;
    unsigned int n_reopen = 0;
    unsigned int n_surplus = 0;
    unsigned int n_heap_ops = 0;
    unsigned int plen = 0;
    unsigned int finished = 0;
    double t_astar = 0;

#ifdef SINGLE_THREADED
    unsigned int threads = 1;
#else
    unsigned int threads = conf.threads;
#endif

    warthog::timer t;
    user(conf.verbose, "Preparing to process", n_results, "queries using",
         (int)threads, "threads.");

    t.start();

#pragma omp parallel num_threads(threads)                               \
    reduction(+ : t_astar, n_expanded, n_generated, n_reopen, \
              n_surplus, n_heap_ops, plen, finished)
    {
        // Parallel data
        unsigned int thread_count = omp_get_num_threads();
        unsigned int thread_id = omp_get_thread_num();

        warthog::timer t_thread;
        warthog::solution sol;
        warthog::search* alg = algos.at(thread_id);

        apply_conf(alg, conf);

        size_t from = 0;
        size_t to = n_results;

        if (!conf.thread_alloc)
        {
            // Instead of bothering with manual conversion (think 'ceil()'), we
            // use the magic of "usual arithmetic" to achieve the right from/to
            // values.
            size_t step = n_results * thread_id;
            from = step / thread_count;
            to = (step + n_results) / thread_count;
        }

        if (conf.no_cache && g != nullptr)
        {
            // Mini-hack: perturbing no edges will still increment the graph
            // counter
            std::vector<std::pair<uint32_t, warthog::graph::edge>> v;
            g->perturb(v);
        }

        t_thread.start();
        // Iterate over the *requests* then convert to ids ({o,d} pair)
        for (auto id = from; id < to; id += 1)
        {
            size_t i = id * 2;
            warthog::sn_id_t start_id = reqs.at(i);
            warthog::sn_id_t target_id = reqs.at(i + 1);

            // Allocate targets to threads.
            //
            // TODO Better thread alloc?
            //
            // TODO If we have `oracle.mod == thread_count` then only one core
            // will work.
            if (conf.thread_alloc && target_id % thread_count != thread_id)
            { continue; }

            // Actual search
            warthog::problem_instance pi(start_id, target_id, conf.debug);
            alg->get_path(pi, sol);

            // Update stasts
            t_astar += sol.met_.time_elapsed_nano_;
            n_expanded += sol.met_.nodes_expanded_;
            n_generated += sol.met_.nodes_generated_;
            n_heap_ops += sol.met_.heap_ops_;
            n_reopen += sol.met_.nodes_reopen_;
            n_surplus += sol.met_.nodes_surplus_;
            plen += sol.path_.size();
            finished += sol.path_.back() == target_id;
        }

#pragma omp critical
        trace(conf.verbose, "[", thread_id, "] Processed", to - from,
              "trips in", t_thread.elapsed_time_micro(), "us.");
    }

    user(conf.verbose, "Processed", n_results, "in", t.elapsed_time_micro(),
         "us");

    std::streambuf* buf;
    std::ofstream of;
    if (fifo_out == "-")
    {
        buf = std::cout.rdbuf();
    }
    else
    {
        of.open(fifo_out);
        buf = of.rdbuf();
    }

    std::ostream out(buf);

    debug(conf.verbose, "Spawned a writer on", fifo_out);
    out << n_expanded << "," << n_generated << ","
        << n_reopen << "," << n_surplus << "," 
        << n_heap_ops << "," << plen << ","
        << finished << "," << t_read << "," << t_astar << ","
        << t.elapsed_time_nano() << std::endl;

    if (fifo_out != "-") { of.close(); }
}

/**
 * The reader thread reads the data passed to the pipe ('FIFO') in the following
 * order:
 *
 *  1. the configuration for the search;
 *
 *  2. the output pipe's name and the number of queries; and,
 *
 *  3. the queries as (o, d)-pairs.
 *
 * It then passes the data to the search function before calling itself again.
 */
void
reader(conf_fn& apply_conf, warthog::graph::xy_graph* g)
{
    std::ifstream fd;
    config conf;
    std::string fifo_out;
    std::string queries;
    std::string diff;
    std::vector<t_query> lines;
    warthog::timer t;
    std::vector<std::pair<uint32_t, warthog::graph::edge>> edges;

    while (true)
    {
        fd.open(fifo);
        debug(VERBOSE, "waiting for writers...");

        if (fd.good())
        {
            debug(VERBOSE, "Got a writer");
        }
        // else?
        t.start();

        // Start by reading config
        try
        {
            fd >> conf;
            sanitise_conf(conf);
        } // Ignore bad parsing and fall back on default conf
        catch (std::exception& e)
        {
            debug(conf.verbose, e.what());
        }

        trace(conf.verbose, conf);

        // Read input query file, output pipe and diff file
        fd >> queries >> fifo_out >> diff;
        debug(conf.verbose, "Read queries from", queries);
        debug(conf.verbose, "Output to", fifo_out);

        fd.close();
        fd.open(queries);

        if (!fd.good())
        {
            warning("Could not open", queries);
            lines.clear();
        }
        else
        {
            warthog::sn_id_t o, d;
            size_t s = 0;
            size_t i = 0;

            fd >> s;
            lines.resize(s * 2);
            debug(conf.verbose, "Preparing to read", s, "queries");
            while (fd >> o >> d)
            {
                lines.at(i) = o;
                lines.at(i + 1) = d;
                i += 2;
            }
            assert(lines.size() == s * 2);
        }
        fd.close();                 // TODO check if we need to keep this open

        if (diff != "-" && g != nullptr)
        {
            fd.open(queries);
            if (!fd.good())
            {
                warning("Could not open", diff);
                edges.clear();
            }
            else
            {
                uint32_t h, t;
                warthog::cost_t w;
                size_t s = 0;
                size_t i = 0;

                fd >> s;
                edges.resize(s);
                debug(conf.verbose, "Preparing to read", s, "perturbations");
                while (fd >> h >> t >> w)
                {
                    edges.at(i) = {h, warthog::graph::edge(t, w)};
                    i += 1;
                }
                assert(edges.size() == s);
            }
            fd.close();

            g->perturb(edges);
        }

        trace(conf.verbose, "Read", int(lines.size() / 2), "queries in ",
              t.elapsed_time_micro(), "us");

        DO_ON_DEBUG_IF(conf.debug)
        {
            for (size_t i = 0; i < lines.size(); i += 2)
            {
                // Not using `verbose` here, it's a lot of info...
                debug(conf.debug, lines.at(i), ",", lines.at(i + 1));
            }
        }

        if (lines.size() > 0)
        {
            run_search(apply_conf, conf, fifo_out, lines,
                       t.elapsed_time_nano(), g);
        }
    }
}

void
run_table_search(warthog::graph::xy_graph &g)
{
    std::string xy_filename = read_graph_and_diff(g);

    // TODO Have better control flow
    if (xy_filename == "") { return; }
    create_distribute_controller(g.get_num_nodes());
    std::string dir;
    if (cfg.get_num_values("outdir") > 0) {
      dir = cfg.get_param_value("outdir");
    }
    else {
      std::cerr << "parameter is missing: --outdir [dir]\n";
      exit(EXIT_FAILURE);
    }
    std::string cpdfile = distribute::format_cpdfile(xy_filename, dir, dc.wid, 0);

    warthog::cpd::graph_oracle_base<warthog::cpd::REV_TABLE> oracle(&g);
    read_oracle<warthog::cpd::REV_TABLE>(cpdfile, oracle);

    for (auto& alg: algos)
    {
        warthog::simple_graph_expansion_policy* expander =
            new warthog::simple_graph_expansion_policy(&g);
        warthog::cpd_heuristic_base<warthog::cpd::REV_TABLE>* h =
            new warthog::cpd_heuristic_base<warthog::cpd::REV_TABLE>(&oracle, 1.0);
        warthog::pqueue_min* open = new warthog::pqueue_min();

        alg = new warthog::cpd_search<
            warthog::cpd_heuristic_base<warthog::cpd::REV_TABLE>,
            warthog::simple_graph_expansion_policy,
            warthog::pqueue_min>(h, expander, open);
    }

    user(VERBOSE, "Loaded", algos.size(), "search.");

    conf_fn apply_conf = [] (warthog::search* base, config &conf) -> void
    {
        warthog::cpd_search<
            warthog::cpd_heuristic_base<warthog::cpd::REV_TABLE>,
            warthog::simple_graph_expansion_policy,
            warthog::pqueue_min>* alg = static_cast<
                warthog::cpd_search<
                    warthog::cpd_heuristic_base<warthog::cpd::REV_TABLE>,
                    warthog::simple_graph_expansion_policy,
                    warthog::pqueue_min>*>(base);

        // Setup algo's config; we assume sane inputs
        alg->get_heuristic()->set_hscale(conf.hscale);
        alg->set_max_time_cutoff(conf.time); // This needs to be in ns
        alg->set_max_expansions_cutoff(conf.itrs);
        alg->set_max_k_moves(conf.k_moves);
        alg->set_quality_cutoff(conf.fscale);
    };

    reader(apply_conf, &g);
}

void
run_table(warthog::graph::xy_graph &g)
{
    std::string xy_filename = cfg.get_param_value("input");
    if(xy_filename == "")
    {
        std::cerr << "parameter is missing: --input [xy-graph file]\n";
        return;
    }

    std::ifstream ifs(xy_filename);
    ifs >> g;
    ifs.close();

    create_distribute_controller(g.get_num_nodes());
    std::string dir;
    if (cfg.get_num_values("outdir") > 0) {
      dir = cfg.get_param_value("outdir");
    }
    else {
      std::cerr << "parameter is missing: --outdir [dir]\n";
      exit(EXIT_FAILURE);
    }
    std::string cpdfile = distribute::format_cpdfile(xy_filename, dir, dc.wid, 0);

    warthog::cpd::graph_oracle_base<warthog::cpd::REV_TABLE> oracle(&g);
    read_oracle<warthog::cpd::REV_TABLE>(cpdfile, oracle);

    for (auto& alg: algos)
    {
        alg = new warthog::cpd_extractions_base<warthog::cpd::REV_TABLE>(
            &g, &oracle);
    }

    user(VERBOSE, "Loaded", algos.size(), "search.");

    conf_fn apply_conf = [] (warthog::search* base, config &conf) -> void
    {
        warthog::cpd_extractions_base<warthog::cpd::REV_TABLE>* alg =
            static_cast<warthog::cpd_extractions_base<warthog::cpd::REV_TABLE>*>(
                base);

        alg->set_max_k_moves(conf.k_moves);
    };

    reader(apply_conf, &g);
}

/**
 * The main takes care of loading the data and spawning the reader thread.
 */
int
main(int argc, char *argv[])
{
    // parse arguments
    warthog::util::param valid_args[] =
        {
            {"input", required_argument, 0, 1},
            {"fifo",  required_argument, 0, 1},
            {"partmethod",  required_argument, 0, 1},
            {"partkey",  required_argument, 0, 1},
            {"wid",  required_argument, 0, 1},
            {"outdir",  required_argument, 0, 1},
            {"maxworker",  required_argument, 0, 1},
            {"alg",   required_argument, 0, 1},
            // {"problem",  required_argument, 0, 1},
            {0,  0, 0, 0}
        };

    warthog::graph::xy_graph g;

    cfg.parse_args(argc, argv, "-f", valid_args);

    // TODO
    // if(argc == 1 || print_help)
    // {
    //      help();
    //     exit(0);
    // }

    std::string alg_name = cfg.get_param_value("alg");
    if((alg_name == ""))
    {
        std::cerr << "parameter is missing: --alg\n";
        return EXIT_FAILURE;
    }

#ifdef SINGLE_THREADED
    algos.resize(1);
#else
    algos.resize(omp_get_max_threads());
#endif

    std::string other = cfg.get_param_value("fifo");
    if (other != "")
    {
        fifo = other;
    }

    int status = mkfifo(fifo.c_str(), S_IFIFO | 0666);

    if (status < 0)
    {
        perror("mkfifo");
        return EXIT_FAILURE;
    }

    debug(true, "Reading from", fifo);

    // Register signal handlers
    signal(SIGINT, signalHandler);
    signal(SIGTERM, signalHandler);
    signal(SIGABRT, signalHandler);
    if (alg_name == "table-search") {
      run_table_search(g);
    }
    else if (alg_name == "table") {
      run_table(g);
    }
    else {
        std::cerr << "--alg not recognised." << std::endl;
    }
    signalHandler(EXIT_FAILURE); // Is this even legal?

    // We do not exit from here
    return EXIT_FAILURE;
}
