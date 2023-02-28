#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "distribution_controller.h"

using namespace std;
using namespace distribute;

TEST_CASE("div") {
  int n = 167758;
  int div = 9000;
  int maxw = 5;
  vector<DistributeController> ds;
  for (int i=0; i<maxw; i++) {
    DistributeController d(n, maxw, i);
    d.set_method(distribute::DIV, div);
    ds.push_back(d);
  }

  int cnt = 0;
  set<int> all;
  for (auto &d: ds) {
    auto& res = d.get_worker_blocks();
    for (auto &it: res) cnt += it.size();

    for (int b=0; b<res.size(); b++) {
      auto& block = res[b];
      for (int i=0; i<block.size(); i++) {
        assert(all.find(block[i]) == all.end());
        all.insert(block[i]);
        int idinblock = d.get_index_in_block(block[i]);
        REQUIRE(idinblock == i);
      }
    }
  }
  REQUIRE(cnt == n);
  REQUIRE(all.size() == n);
}

TEST_CASE("mod") {
  int n = 167758;
  int mod = 100;
  int maxw = 5;
  vector<DistributeController> ds;
  for (int i=0; i<maxw; i++) {
    DistributeController d(n, maxw, i);
    d.set_method(distribute::MOD, mod);
    ds.push_back(d);
  }

  int cnt = 0;
  set<int> all;
  for (auto &d: ds) {
    vector<vector<sn_id_t>> res = d.get_worker_blocks();
    for (auto &it: res) cnt += it.size();

    for (auto &block: res) {
      for (int i=0; i<block.size(); i++) {
        all.insert(block[i]);
        REQUIRE(d.get_index_in_block(block[i]) == i);
      }
    }
  }
  REQUIRE(cnt == n);
  REQUIRE(all.size() == n);
}


int main(int argv, char* args[]) {
  Catch::Session session;
  int res = session.run(argv, args);
  return res;
}
