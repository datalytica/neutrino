/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once
#include <perspective/portable.h>
SUPPRESS_WARNINGS_VC(4503)

#include <perspective/base.h>
#include <perspective/exports.h>
#include <perspective/sparse_tree_node.h>
#include <perspective/pivot.h>
#include <perspective/aggspec.h>
#include <perspective/step_delta.h>
#include <perspective/min_max.h>
#include <perspective/shared_ptrs.h>
#include <perspective/tree_iterator.h>

namespace perspective
{

class t_gstate;
class t_dtree_ctx;
class t_config;
class t_ctx2;

using boost::multi_index_container;
using namespace boost::multi_index;

typedef std::pair<t_depth, t_ptidx> t_dptipair;
typedef std::vector<t_dptipair> t_dptipairvec;

struct by_idx
{
};

struct by_depth
{
};

struct by_pidx
{
};

struct by_pidx_hash
{
};

struct by_nstrands
{
};

struct by_idx_pkey
{
};

struct by_idx_lfidx
{
};

PERSPECTIVE_EXPORT t_tscalar get_dominant(t_tscalvec& values);

struct t_build_strand_table_common_rval
{
    t_schema m_flattened_schema;
    t_schema m_strand_schema;
    t_schema m_aggschema;
    t_uindex m_npivotlike;
    std::vector<t_str> m_pivot_like_columns;
    t_uindex m_pivsize;
};

struct PERSPECTIVE_EXPORT t_agg_update_info
{
    t_colcptrvec m_src;
    t_colptrvec m_dst;
    t_aggspecvec m_aggspecs;

    std::vector<t_uindex> m_dst_topo_sorted;
};

struct t_tree_unify_rec
{
    t_tree_unify_rec(
        t_uindex sptidx, t_uindex daggidx, t_uindex saggidx, t_uindex nstrands);

    t_uindex m_sptidx;
    t_uindex m_daggidx;
    t_uindex m_saggidx;
    t_uindex m_nstrands;
};

typedef std::vector<t_tree_unify_rec> t_tree_unify_rec_vec;

class PERSPECTIVE_EXPORT t_stree
{
public:
    typedef const t_stree* t_cptr;
    typedef std::shared_ptr<t_stree> t_sptr;
    typedef std::shared_ptr<const t_stree> t_csptr;
    typedef t_stnode t_tnode;
    typedef std::vector<t_stnode> t_tnodevec;

    typedef std::map<const char*, const char*, t_cmp_charptr> t_sidxmap;

    t_stree(const t_pivotvec& pivots, const t_aggspecvec& aggspecs,
        const t_schema& schema, const t_config& cfg);
    ~t_stree();

    void init();

    t_str repr() const;

    t_tscalar get_value(t_tvidx idx) const;
    t_tscalar get_sortby_value(t_tvidx idx) const;

    void build_strand_table_phase_1(t_tscalar pkey, t_op op, t_uindex idx,
        t_uindex npivots, t_uindex strand_count_idx, t_uindex aggcolsize,
        t_bool force_current_row, const t_colcptrvec& piv_pcolcontexts,
        const t_colcptrvec& piv_tcols, const t_colcptrvec& agg_ccols,
        const t_colcptrvec& agg_dcols, t_colptrvec& piv_scols,
        t_colptrvec& agg_acols, t_column* agg_scountspar, t_column* spkey,
        t_uindex& insert_count, t_bool& pivots_neq,
        const std::vector<t_str>& pivot_like) const;

    void build_strand_table_phase_2(t_tscalar pkey, t_uindex idx,
        t_uindex npivots, t_uindex strand_count_idx, t_uindex aggcolsize,
        const t_colcptrvec& piv_pcols, const t_colcptrvec& agg_pcols,
        t_colptrvec& piv_scols, t_colptrvec& agg_acols, t_column* agg_scount,
        t_column* spkey, t_uindex& insert_count,
        const std::vector<t_str>& pivot_like) const;

    std::pair<t_table_sptr, t_table_sptr> build_strand_table(
        const t_table& flattened, const t_table& delta, const t_table& prev,
        const t_table& current, const t_table& transitions,
        const t_aggspecvec& aggspecs, const t_config& config) const;

    std::pair<t_table_sptr, t_table_sptr> build_strand_table(
        const t_table& flattened, const t_aggspecvec& aggspecs,
        const t_config& config) const;

    void update_shape_from_static(const t_dtree_ctx& ctx);
    void update_aggs_from_static(
        const t_dtree_ctx& ctx, const t_gstate& gstate);

    t_uindex size() const;

    t_uindex get_num_children(t_uindex idx) const;
    void get_child_nodes(t_uindex idx, t_tnodevec& nodes) const;
    std::vector<t_uindex> zero_strands() const;

    std::set<t_uindex> non_zero_leaves(
        const std::vector<t_uindex>& zero_strands) const;

    std::set<t_uindex> non_zero_ids(
        const std::vector<t_uindex>& zero_strands) const;

    std::set<t_uindex> non_zero_ids(const std::set<t_uindex>& ptiset,
        const std::vector<t_uindex>& zero_strands) const;

    t_uindex get_parent_idx(t_uindex idx) const;
    std::vector<t_uindex> get_ancestry(t_uindex idx) const;

    t_index get_sibling_idx(
        t_tvidx p_ptidx, t_index p_nchild, t_uindex c_ptidx) const;
    t_uindex get_aggidx(t_uindex idx) const;

    t_table_csptr get_aggtable() const;

    t_table* _get_aggtable();

    t_tnode get_node(t_uindex idx) const;

    void get_path(t_uindex idx, t_tscalvec& path) const;
    void get_sortby_path(t_uindex idx, t_tscalvec& path) const;

    t_uindex resolve_child(t_uindex root, const t_tscalar& datum) const;

    void drop_zero_strands();

    t_depth get_depth(t_uindex ptidx) const;
    void get_drd_indices(
        t_uindex ridx, t_depth rel_depth, std::vector<t_uindex>& leaves) const;
    std::vector<t_uindex> get_leaves(t_uindex idx) const;
    t_tscalvec get_pkeys(t_uindex idx) const;
    std::vector<t_uindex> get_child_idx(t_uindex idx) const;
    std::vector<t_ptipair> get_child_idx_depth(t_uindex idx) const;

    void populate_leaf_index(const std::set<t_uindex>& leaves);

    t_uindex last_level() const;

    const t_pivotvec& get_pivots() const;
    t_ptidx resolve_path(t_uindex root, const t_tscalvec& path) const;

    // aggregates should be presized to be same size
    // as agg_indices
    void get_aggregates_for_sorting(t_uindex nidx,
        const std::vector<t_index>& agg_indices, t_tscalvec& aggregates,
        t_ctx2*) const;

    t_tscalar get_aggregate(t_ptidx idx, t_index aggnum) const;

    void get_child_indices(t_ptidx idx, std::vector<t_ptidx>& out_data) const;

    void set_alerts_enabled(bool enabled_state);

    void set_deltas_enabled(bool enabled_state);

    void set_minmax_enabled(bool enabled_state);

    void set_feature_state(t_ctx_feature feature, t_bool state);

    template <typename ITER_T>
    t_minmax get_agg_min_max(ITER_T biter, ITER_T eiter, t_uindex aggidx) const;
    t_minmax get_agg_min_max(t_uindex aggidx, t_depth depth) const;
    t_minmaxvec get_min_max() const;

    void clear_deltas();

    const t_sptr_tcdeltas& get_deltas() const;

    void clear();

    t_tscalar first_last_helper(
        t_uindex nidx, const t_aggspec& spec, const t_gstate& gstate) const;

    t_bool node_exists(t_uindex nidx);

    t_table* get_aggtable();

    void clear_aggregates(const std::vector<t_uindex>& indices);
    t_bool has_deltas() const;
    void set_has_deltas(t_bool v);

    std::vector<t_uindex> get_descendents(t_uindex nidx) const;

    t_uindex get_num_leaves(t_uindex depth) const;
    std::vector<t_index> get_indices_for_depth(t_uindex depth) const;

    t_bfs_iter<t_stree> bfs() const;
    t_dfs_iter<t_stree> dfs() const;
    void pprint() const;
    t_tscalvec get_pkeys_for_leaf(t_uindex idx) const;
    t_tscalar get_pkey_for_leaf(t_uindex idx) const;
    bool insert_node(const t_tnode& node);
    void add_pkey(t_uindex idx, t_tscalar pkey);

protected:
    void mark_zero_desc();
    t_uindex get_num_aggcols() const;
    typedef std::pair<const t_column*, t_column*> t_srcdst_columns;
    typedef std::vector<t_srcdst_columns> t_srcdst_colvec;

    t_bool pivots_changed(t_value_transition t) const;
    t_uindex genidx();
    t_uindex gen_aggidx();
    std::vector<t_uindex> get_children(t_uindex idx) const;
    void update_agg_table(t_uindex nidx, t_agg_update_info& info,
        t_uindex src_ridx, t_uindex dst_ridx, t_index nstrands,
        const t_gstate& gstate);

    t_bool is_leaf(t_uindex nidx) const;

    t_build_strand_table_common_rval build_strand_table_common(
        const t_table& flattened, const t_aggspecvec& aggspecs,
        const t_config& config) const;

private:
    struct t_stree_p;
    std::unique_ptr<t_stree_p> m_p;
};

typedef std::shared_ptr<t_stree> t_stree_sptr;
typedef std::shared_ptr<const t_stree> t_stree_csptr;
typedef std::vector<t_stree*> t_streeptr_vec;
typedef std::vector<t_stree_csptr> t_stree_csptr_vec;

} // end namespace perspective
