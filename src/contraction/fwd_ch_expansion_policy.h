#ifndef WARTHOG_FWD_CH_EXPANSION_POLICY
#define WARTHOG_FWD_CH_EXPANSION_POLICY

// contraction/fwd_ch_expansion_policy.h
//
// An expansion policy for forward-driven
// search in contraction hiearchies.
//
// The idea is simple:
// When expanding a node, look at the rank
// of the parent relative to the current node.
//
// If the parent is smaller the search is traveling up in the hierarchy and 
// every neighbour is generated.
//
// If the parent is larger the search is traveling down in the hiearchy
// and only down-ward neighbours are generated.
//
// The approach preserves optimality. 
//
// @author: dharabor
// @created: 2016-07-18
//

#include "expansion_policy.h"
#include <vector>

namespace warthog
{

namespace graph
{
class planar_graph;
}

class problem_instance;
class search_node;
class fwd_ch_expansion_policy : public expansion_policy
{
    public:
        fwd_ch_expansion_policy(
                warthog::graph::planar_graph* graph,
                std::vector<uint32_t>* rank);

        ~fwd_ch_expansion_policy();

		virtual void 
		expand(warthog::search_node*, warthog::problem_instance*);

        virtual void
        get_xy(warthog::search_node*, int32_t& x, int32_t& y);

        virtual uint32_t
        mem()
        {
            return expansion_policy::mem() +
                sizeof(this);
        }

    private:
        std::vector<uint32_t>* rank_;
        warthog::graph::planar_graph* g_;

        inline uint32_t
        get_rank(uint32_t id)
        {
            return rank_->at(id);
        }
};
}

#endif