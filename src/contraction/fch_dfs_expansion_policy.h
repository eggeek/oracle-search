#ifndef WARTHOG_FCH_DFS_EXPANSION_POLICY_H
#define WARTHOG_FCH_DFS_EXPANSION_POLICY_H

// contraction/fch_dfs_expansion_policy.h
//
// An expansion policy that combines FCH with a "down pruning" scheme.
// The idea is to label each down edge with a node-id range such that if
// the target is in the id-range it means the edge appears on an optimal
// down-path to the target.
//
// @author: dharabor
// @created: 2017-12-02
//

#include "contraction.h"
#include "dfs_labelling.h"
#include "expansion_policy.h"
#include "forward.h"
#include "xy_graph.h"

#include <vector>

namespace warthog
{

class fch_dfs_expansion_policy : public expansion_policy
{
    public:
        fch_dfs_expansion_policy(
                warthog::ch::ch_data* chd,
                warthog::label::dfs_labelling* lab);

        virtual 
        ~fch_dfs_expansion_policy() { }

		virtual void 
		expand(warthog::search_node*, warthog::problem_instance*);

        virtual void
        get_xy(warthog::sn_id_t node_id, int32_t& x, int32_t& y);

        virtual warthog::search_node* 
        generate_start_node(warthog::problem_instance* pi);

        virtual warthog::search_node*
        generate_target_node(warthog::problem_instance* pi);

        virtual inline size_t
        mem()
        {
            size_t retval = sizeof(this);
            retval += chd_->mem(); 
            retval += expansion_policy::mem();
            return retval;
        }

    private:
        warthog::ch::ch_data* chd_;

        warthog::label::dfs_labelling* lab_;
        uint32_t s_label, t_label;
        int32_t tx_, ty_;
        uint32_t t_graph_id;
        uint32_t t_level;

        typedef bool
                (warthog::fch_dfs_expansion_policy::*filter_fn)
                (uint32_t node_idx, uint32_t edge_idx);

        filter_fn filter;

        inline bool
        filter_all(uint32_t node_idx, uint32_t edge_idx)
        {
            warthog::label::dfs_label& label = 
                lab_->get_label(node_idx, edge_idx);
            bool retval = label.bbox_.contains(tx_, ty_);
            return !retval; 
        }

        inline bool
        filter_bb_only(uint32_t node_idx, uint32_t edge_idx)
        {
            warthog::label::dfs_label& label = 
                lab_->get_label(node_idx, edge_idx);
            bool retval = label.bbox_.contains(tx_, ty_);
            return !retval; 
        }

        inline uint32_t
        get_level(uint32_t id)
        {
            return chd_->level_->at(id);
        }
};
}

#endif
