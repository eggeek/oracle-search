/**
 * This file is used to create CPDs in an independent fashion.
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

std::vector<warthog::sn_id_t>
modulo(size_t num, size_t mod, size_t node_count)
{
    std::vector<warthog::sn_id_t> nodes;
    assert(num > 0);
    assert(mod > 1);

    for (warthog::sn_id_t i = num; i < node_count; i += mod)
    {
        nodes.push_back(i);
    }

    return nodes;
}

std::vector<warthog::sn_id_t>
range(size_t from, size_t to, size_t node_count)
{
    std::vector<warthog::sn_id_t> nodes;
    assert(to > 0);
    assert(from < to);

    nodes.resize(to - from);
    std::iota(nodes.begin(), nodes.end(), from);

    return nodes;
}

/**
 * Rebuild a CPD given a list of file containing its parts.
 *
 * The partial CPDs must be given in the order of the nodes.
 */
int
join_cpds(warthog::graph::xy_graph &g, std::string cpd_filename,
          std::vector<std::string> file_list, uint32_t seed, bool verbose,
          uint32_t mod)
{
    uint32_t step = 0;
    std::vector<warthog::sn_id_t> nodes;

    if (mod > 1)
    {
        nodes = modulo(0, mod, g.get_num_nodes());
    }

    // Here, the type of the oracle does not matter.
    warthog::cpd::graph_oracle cpd(&g);
    cpd.clear();                // Need to reset fm_
    cpd.compute_dfs_preorder(seed);
    // convert the column order into a map: from vertex id to its ordered index
    cpd.value_index_swap_array();

    for (auto name: file_list)
    {
        warthog::cpd::graph_oracle part(&g);
        std::ifstream ifs(name);

        if (!ifs.good())
        {
            std::cerr << "Cannot open file " << name << std::endl;
            return EXIT_FAILURE;
        }

        ifs >> part;
        ifs.close();

        // Need to do it by hand as the rows are not consecutive
        if (mod > 1)
        {
            for (auto n : nodes)
            {
                cpd.set_row(step + n, part.get_row(n));
            }
        }
        else
        {
            cpd.append_fm(part);
        }

        step++;
    }

    std::ofstream ofs(cpd_filename);

    if (!ofs.good())
    {
        std::cerr << "Could not open CPD file " << cpd_filename << std::endl;
        return EXIT_FAILURE;
    }

    info(verbose, "Writing results to", cpd_filename);
    ofs << cpd;
    ofs.close();

    return EXIT_SUCCESS;
}

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
  const std::string& conf_file, 
  const std::string& xyfile,
  int div, int mod, int num, int from, int to) {
    std::ofstream ofs(conf_file);
  /*
   * Tell the downstream script how we distribute cpd rows, there are 3 options:
   * 1. (div, num), num < div: 
   *    - divide nodes to k blocks, nodes with id / div == num are in the same block
   *    - get row: t % div, i.e., oracle.set_mod(div)
   * 2. (mod, num), num < mod:
   *    - divide nodes into k blocks, nodes with id % mod == num are in the same block
   *    - get row: t / mod, i.e., oracle.set_div(mod)
   * 3. (from, to)
   *    - nodes in [from, to) are in the same block
   *    - get row: t - from, i.e., oracle.set_offset(from)
   */
    if (div > 0) {
      mod = from = to = 0;
    }
    else if (mod > 0) {
      div = from = to = 0;
    }
    // else we are using [from, to) or not to distribut at all
    ofs << "xyfile,mod,div,num,from,to" << std::endl;
    ofs << xyfile << "," << mod << "," << div << "," << num << ","
        << from << "," << to << std::endl;
}

int
main(int argc, char *argv[])
{
    int verbose = 0;
    warthog::util::param valid_args[] =
    {
        {"from", required_argument, 0, 1},
        {"to", required_argument, 0, 1},
        {"mod", required_argument, 0, 1},
        {"div", required_argument, 0, 1},
        {"num", required_argument, 0, 1},
        {"input", required_argument, 0, 1},
        {"output", required_argument, 0, 1},
        {"join", required_argument, 0, 1},
        {"seed", required_argument, 0, 1},
        {"type", required_argument, 0, 1},
        {"verbose", no_argument, &verbose, 1},
        {0, 0, 0, 0}
    };

    warthog::util::cfg cfg;
    cfg.parse_args(argc, argv, valid_args);

    std::string type = cfg.get_param_value("type");
    bool reverse;
    warthog::cpd::symbol cpd_type;

    if (type == "" || type == "fwd" || type == "forward")
    {
        cpd_type = warthog::cpd::FORWARD;
        reverse = false;
    }
    else if (type == "bwd" || type == "backward" || type == "rev" ||
             type == "reverse")
    {
        cpd_type = warthog::cpd::REVERSE;
        reverse = true;
    }
    else if (type == "bearing")
    {
        cpd_type = warthog::cpd::BEARING;
        reverse = true;
        // TODO Implement forward azimuth compression
    }
    else if (type == "table")
    {
        cpd_type = warthog::cpd::TABLE;
        reverse = false;
    }
    else if (type == "rev-table")
    {
        cpd_type = warthog::cpd::REV_TABLE;
        reverse = true;
    }
    else
    {
        std::cerr << "Unknown CPD type '" << type << "'\n";
        return EXIT_FAILURE;
    }

    std::string xy_filename = cfg.get_param_value("input");
    std::string cpd_filename = cfg.get_param_value("output");
    std::string conf_filenme;

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

    if (cpd_filename == "")
    {
        // Use default name
        cpd_filename = xy_filename;

        switch (cpd_type)
        {
            case warthog::cpd::REVERSE:
                cpd_filename += "-rev";
            case warthog::cpd::BEARING:
                cpd_filename += "-bearing";
                break;
            case warthog::cpd::TABLE:
                cpd_filename += "-table";
                break;
            case warthog::cpd::REV_TABLE:
                cpd_filename += "-rev-table";
                break;
            default: // noop
                break;
        }

        cpd_filename += ".cpd";

        std::cerr << "No --output provided, defaulting to: " << cpd_filename
                  << std::endl;
    }
    uint32_t mod = 0;
    uint32_t num = 0;
    uint32_t div = 0;
    uint32_t from = 0;
    uint32_t to = 0;
    uint32_t node_count = g.get_num_nodes();

    // TODO Message to inform that only one can be used?
    if (cfg.get_num_values("mod") > 0)
    {
        std::string s_mod = cfg.get_param_value("mod");
        std::string s_num = cfg.get_param_value("num");

        // TODO this should be a try/catch block
        if (s_mod != "")
        {
            mod = std::stoi(s_mod);

            if (mod < 1)
            {
                std::cerr << "The modulo must be >= 1, got: " << s_mod
                          << std::endl;
                return EXIT_FAILURE;
            }
        }

        if (s_num != "")
        {
            num = std::stoi(s_num);

            if (num < 0)
            {
                std::cerr << "The offset must be >= 0, got: " << s_num
                          << std::endl;
                return EXIT_FAILURE;
            }
        }
    }
    else if (cfg.get_num_values("div") > 0)
    {
        std::string s_div = cfg.get_param_value("div");
        std::string s_num = cfg.get_param_value("num");

        // TODO this should be a try/catch block
        if (s_div != "") {
            div = std::stoi(s_div);

            if (div < 1)
            {
                std::cerr << "The divisor must be >= 1, got: " << s_div
                          << std::endl;
                return EXIT_FAILURE;
            }
        }

        if (s_num != "")
        {
            num = std::stoi(s_num);

            if (num < 0)
            {
                std::cerr << "The offset must be >= 0, got: " << s_num
                          << std::endl;
                return EXIT_FAILURE;
            }
        }

        int range = g.get_num_nodes() / div;
        from = range * num;
        to = std::min<size_t>(from + range, g.get_num_nodes());
        node_count = to - from;
    }
    else
    {
        std::string s_from = cfg.get_param_value("from");
        std::string s_to = cfg.get_param_value("to");

        if (s_from != "")
        {
            from = std::stoi(s_from);

            if (from < 0)
            {
                std::cerr << "Argument --from [node id] cannot be negative."
                          << std::endl;
                return EXIT_FAILURE;
            }
        }

        if (s_to != "")
        {
            to = std::stoi(s_to);
        }

        if (to <= 0 || node_count < (uint32_t) to)
        {
            to = node_count;
        }
    }

    conf_filenme = cpd_filename + "_conf.csv";
    write_conf(conf_filenme, xy_filename, div, mod, num, from, to);

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

    if (cfg.get_num_values("join") > 0)
    {
        std::vector<std::string> names;
        std::string part;

        while (true)
        {
            part = cfg.get_param_value("join");

            if (part == "") { break; }
            names.push_back(part);
        }

        return join_cpds(g, cpd_filename, names, seed, verbose, mod);
    }
    else
    {
        #ifdef SINGLE_THREADED
        size_t nthreads = 1;
        #else
        size_t nthreads = omp_get_max_threads();
        #endif
        std::vector<warthog::cpd::oracle_listener*> listeners(nthreads);
        std::vector<warthog::sn_id_t> nodes;

        if (mod > 1)
        {
            nodes = modulo(num, mod, node_count);
        }
        else
        {
            nodes = range(from, to, node_count);
        }

        // We have to explicitly create and pass the different (sub-) types of
        // oracles and listeners or it messes with the template resolution.
        switch (cpd_type)
        {
            case warthog::cpd::REVERSE:
            {
                warthog::cpd::graph_oracle_base<warthog::cpd::REVERSE> cpd(&g);

                for (size_t t = 0; t < nthreads; t++)
                {
                    listeners.at(t) = new warthog::cpd::reverse_oracle_listener<
                            warthog::cpd::REVERSE>(&cpd);
                }

                return make_cpd<warthog::cpd::REVERSE>(
                    g, cpd, listeners, cpd_filename, nodes, reverse, seed,
                    verbose);
            }

            case warthog::cpd::BEARING:
            {
                warthog::cpd::graph_oracle_base<warthog::cpd::BEARING> cpd(&g);

                for (size_t t = 0; t < nthreads; t++)
                {
                    listeners.at(t) =
                        new warthog::cpd::reverse_bearing_oracle_listener(&cpd);
                }

                return make_cpd<warthog::cpd::BEARING>(
                    g, cpd, listeners, cpd_filename, nodes, reverse, seed,
                    verbose);
            }

            case warthog::cpd::TABLE:
            {
                warthog::cpd::graph_oracle_base<warthog::cpd::TABLE> cpd(&g);

                for (size_t t = 0; t < nthreads; t++)
                {
                    listeners.at(t) = new warthog::cpd::graph_oracle_listener<
                        warthog::cpd::TABLE>(&cpd);
                }

                return make_cpd<warthog::cpd::TABLE>(
                    g, cpd, listeners, cpd_filename, nodes, reverse, seed,
                    verbose);
            }

            case warthog::cpd::REV_TABLE:
            {
                warthog::cpd::graph_oracle_base<warthog::cpd::REV_TABLE> cpd(&g);

                for (size_t t = 0; t < nthreads; t++)
                {
                    listeners.at(t) = new warthog::cpd::reverse_oracle_listener<
                        warthog::cpd::REV_TABLE>(&cpd);
                }

                return make_cpd<warthog::cpd::REV_TABLE>(
                    g, cpd, listeners, cpd_filename, nodes, reverse, seed,
                    verbose);
            }

            // case warthog::cpd::FORWARD:
            default:
            {
                warthog::cpd::graph_oracle cpd(&g);

                for (size_t t = 0; t < nthreads; t++)
                {
                    listeners.at(t) = new warthog::cpd::graph_oracle_listener<
                        warthog::cpd::FORWARD>(&cpd);
                }

                return make_cpd<warthog::cpd::FORWARD>(
                    g, cpd, listeners, cpd_filename, nodes, reverse, seed,
                    verbose);
            }
        }
    }
}
