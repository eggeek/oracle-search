#ifndef WARTHOG_DIMACS_PARSER_H
#define WARTHOG_DIMACS_PARSER_H

// dimacs_parser.h
//
// A parser for reading road networks of the type
// used at the 9th DIMACS competition.
//
// @author: dharabor
// @created: 2015-01-08
//

#include <stdint.h>
#include <vector>
#include <fstream>


namespace warthog
{

class dimacs_parser
{
    public:
        struct node
        {
            uint32_t id_;
            int32_t x_;
            int32_t y_;
        };

        struct edge
        {
            uint32_t tail_id_;
            uint32_t head_id_;
            int32_t weight_;

            bool 
            operator<(warthog::dimacs_parser::edge& other)
            {
                return head_id_ < other.head_id_;
            }
        };

        typedef std::vector<warthog::dimacs_parser::node>::iterator 
            node_iterator;
        typedef  std::vector<warthog::dimacs_parser::edge>::iterator 
            edge_iterator;

        dimacs_parser();
        dimacs_parser(const char* gr_file);
        dimacs_parser(const char* co_file, const char* gr_file);
        ~dimacs_parser();
        
        // load up a DIMACS file (gr or co)
        // NB: upon invocation, this operation will discard all current nodes
        // (or edges, depending on the type of file passed for loading) and 
        // THEN attempt to load new data.
        bool 
        load(const char* dimacs_file);

        inline int
        get_num_nodes() 
        {
           return n_nodes_;
        }

        inline int
        get_num_edges()
        {
           return n_edges_;
        }

        inline warthog::dimacs_parser::node_iterator
        nodes_begin()
        {
            return nodes_->begin();
        }

        inline warthog::dimacs_parser::node_iterator
        nodes_end()
        {
            return nodes_->end();
        }

        inline warthog::dimacs_parser::edge_iterator
        edges_begin()
        {
            return edges_->begin();
        }
        
        inline warthog::dimacs_parser::edge_iterator
        edges_end()
        {
            return edges_->end();
        }

        void
        print(std::ostream&);

    private:
        void init();
        bool load_co_file(std::istream& fdimacs);
        bool load_gr_file(std::istream& fdimacs);

       uint32_t n_nodes_; // number of nodes
       uint32_t n_edges_;
       std::vector<warthog::dimacs_parser::node>* nodes_;
       std::vector<warthog::dimacs_parser::edge>* edges_;

};

}

#endif
