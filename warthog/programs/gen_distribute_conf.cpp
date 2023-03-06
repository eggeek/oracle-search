#include <iostream>
#include <fstream>
#include "distribution_controller.h"
#include "cfg.h"
#include "forward.h"
#include "xy_graph.h"
using namespace std;
using namespace distribute;

int main(int argc, char* argv[]) {
    // default behaviour of partitioning is running everything on this worker
    string partition = "mod"; // default method id mod
    int partkey = 1;          // default behavior is not partition at all
    int workerid = 0;         // default worker is 0 
    int maxworker = 1;        // default maxworker is 1

  warthog::util::param valid_args[] =
  {
      // define the partition method
      {"partmethod", required_argument, 0, 1},
      // parameter of the partition method
      {"partkey", required_argument, 0, 1},
      // the id of this worker
      {"nodenum", required_argument, 0, 1},
      {"maxworker", required_argument, 0, 1},
      {0, 0, 0, 0}
  };
  warthog::util::cfg cfg;
  cfg.parse_args(argc, argv, valid_args);

  int nodenum = stoi(cfg.get_param_value("nodenum"));

  // parse distribution method
  if (cfg.get_num_values("maxworker") > 0)
    maxworker = stoi(cfg.get_param_value("maxworker"));
  if (cfg.get_num_values("partmethod") > 0)
    partition = cfg.get_param_value("partmethod");
  if (cfg.get_num_values("partkey") > 0)
    partkey = stoi(cfg.get_param_value("partkey"));

  DistributeController dc(nodenum, maxworker, workerid);
  dc.set_method(partition, partkey);

  string header = "node,worker,block,bindex";
  cout << header << endl;
  for (int i=0; i<maxworker; i++) {
    vector<vector<sn_id_t>> blocks = dc.get_worker_blocks(i);
    for (auto& block: blocks) {
      for (auto& node: block) {
        int wid = i, bid = get<0>(dc.node2block[node]), bindex = get<1>(dc.node2block[node]);
        cout << node << "," << wid << "," << bid << "," << bindex << endl;
      }
    }
  }

  return 0;
}
