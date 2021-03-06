/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once
#include <perspective/context_base.h>
#include <perspective/shared_ptrs.h>
#include <perspective/path.h>
#include <perspective/traversal_nodes.h>
#include <perspective/sort_specification.h>

namespace perspective
{

class t_ctx1;
typedef std::shared_ptr<t_ctx1> t_ctx1_sptr;

class PERSPECTIVE_EXPORT t_ctx1 : public t_ctxbase<t_ctx1>
{
public:
    t_ctx1();

    t_ctx1(const t_schema& schema, const t_config& config);

    ~t_ctx1();

#include <perspective/context_common_decls.h>

    t_index open(t_header header, t_tvidx idx);
    t_index open(t_tvidx idx);
    t_index close(t_tvidx idx);

    std::vector<t_tvidx> get_selected_indices() const;
    void select_node(t_tvidx idx);
    void deselect_node(t_tvidx idx);
    void clear_selection();

    t_pivotvec get_row_pivots() const;
    t_aggspec get_aggregate(t_uindex idx) const;
    t_aggspecvec get_aggregates() const;
    t_tscalvec get_row_path(t_tvidx idx) const;
    void set_depth(t_depth depth);
    t_depth get_depth() const;

    t_minmax get_agg_min_max(t_uindex aggidx, t_depth depth) const;

    t_index get_row_idx(const t_tscalvec& path) const;

    t_depth get_trav_depth(t_tvidx idx) const;

    using t_ctxbase<t_ctx1>::get_data;

    t_uindex get_leaf_count() const;
    t_tscalvec get_leaf_data(t_uindex start_row, t_uindex end_row,
        t_uindex start_col, t_uindex end_col) const;

private:
    t_trav_sptr m_traversal;
    t_stree_sptr m_tree;
    t_sortsvec m_sortby;
    t_depth m_depth;
    t_bool m_depth_set;
};

typedef std::vector<t_ctx1_sptr> t_ctx1_svec;

} // end namespace perspective
