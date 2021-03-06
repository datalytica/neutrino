/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once
#include <perspective/first.h>
#include <perspective/base.h>
#include <perspective/port.h>
#include <perspective/exports.h>
#include <perspective/context_handle.h>
#include <perspective/env_vars.h>
#include <perspective/custom_column.h>
#include <perspective/shared_ptrs.h>
#include <perspective/rlookup.h>
#ifdef PSP_PARALLEL_FOR
#include <tbb/parallel_sort.h>
#include <tbb/tbb.h>
#endif
#include <exprtk/exprtk.hpp>
#include <chrono>

namespace perspective
{

struct PERSPECTIVE_EXPORT t_gnode_options
{
    t_gnode_type m_gnode_type;
    t_schema m_port_schema;
    t_ccol_vec m_custom_columns;
};

struct PERSPECTIVE_EXPORT t_gnode_recipe
{
    t_gnode_recipe() {}
    t_gnode_processing_mode m_mode;
    t_gnode_type m_gnode_type;
    t_schema_recipe m_tblschema;
    t_schema_recipevec m_ischemas;
    t_schema_recipevec m_oschemas;
    t_custom_column_recipevec m_custom_columns;
};

#ifdef PSP_GNODE_VERIFY
#define PSP_GNODE_VERIFY_TABLE(X) (X)->verify()
#else
#define PSP_GNODE_VERIFY_TABLE(X)
#endif

class t_gnode;
typedef std::shared_ptr<t_gnode> t_gnode_sptr;

class t_ctx0;
class t_ctx1;
class t_ctx2;
class t_ctx_grouped_pkey;

typedef std::shared_ptr<t_ctx0> t_ctx0_sptr;
typedef std::shared_ptr<t_ctx1> t_ctx1_sptr;
typedef std::shared_ptr<t_ctx2> t_ctx2_sptr;
typedef std::shared_ptr<t_ctx_grouped_pkey> t_ctx_grouped_pkey_sptr;

class PERSPECTIVE_EXPORT t_gnode
{
public:
    static t_gnode_sptr build(const t_gnode_options& options);
    t_gnode(const t_gnode_recipe& recipe);
    t_gnode(const t_gnode_options& options);
    ~t_gnode();
    void init();

    // send data to input port with at index idx
    // schema should match port schema
    void _send_and_process(const t_table& fragments);
    void _send(t_uindex idx, const t_table& fragments);
    void _process();
    void _register_context(const t_str& name, t_ctx_type type, t_int64 ptr);
    void _unregister_context(const t_str& name);

    t_table* get_table();
    const t_table* get_table() const;

    t_value_transition calc_transition(t_bool prev_existed,
        t_bool row_pre_existed, t_bool exists, t_bool prev_valid,
        t_bool cur_valid, t_bool prev_cur_eq, t_bool prev_pkey_eq);

    void pprint() const;
    std::vector<t_str> get_registered_contexts() const;

    t_streeptr_vec get_trees();

    void set_id(t_uindex id);
    t_uindex get_id() const;

    void release_inputs();
    void release_outputs();
    std::vector<t_str> get_contexts_last_updated() const;

    void reset();
    t_str repr() const;
    void clear_input_ports();
    void clear_output_ports();
    
    t_table* _get_pkeyed_table() const;
    t_table_sptr get_sorted_pkeyed_table() const;

    t_tscalvec get_row_data_pkeys(const t_tscalvec& pkeys) const;
    t_tscalvec has_pkeys(const t_tscalvec& pkeys) const;
    t_tscalvec get_pkeys() const;

    t_ccol_vec get_custom_columns() const;

    t_gnode_recipe get_recipe() const;
    t_bool has_python_dep() const;
    void set_pool_cleanup(std::function<void()> cleanup);
    t_bool was_updated() const;
    void clear_updated();

    // helper function for tests
    t_table_sptr tstep(t_table_csptr input_table);

    // Gnode will steal a reference to the context
    void register_context(const t_str& name, t_ctx0_sptr ctx);
    void register_context(const t_str& name, t_ctx1_sptr ctx);
    void register_context(const t_str& name, t_ctx2_sptr ctx);
    void register_context(const t_str& name, t_ctx_grouped_pkey_sptr ctx);

    t_schema get_tblschema() const;

protected:
    void notify_contexts(const t_table& flattened);

    template <typename CTX_T>
    void notify_context(const t_table& flattened, const t_ctx_handle& ctxh);

    template <typename CTX_T>
    void notify_context(CTX_T* ctx, const t_table& flattened,
        const t_table& delta, const t_table& prev, const t_table& current,
        const t_table& transitions, const t_table& existed);

    template <typename CTX_T>
    void update_context_from_state(CTX_T* ctx, const t_table& tbl);

    template <typename CTX_T>
    void set_ctx_state(void* ptr);

    template <typename DATA_T>
    void _process_helper(const t_column* fcolumn, const t_column* scolumn,
        t_column* dcolumn, t_column* pcolumn, t_column* ccolumn,
        t_column* tcolumn, const t_uint8* op_base, std::vector<t_rlookup>& lkup,
        std::vector<t_bool>& prev_pkey_eq_vec,
        std::vector<t_uindex>& added_vec);

    void _update_contexts_from_state(const t_table& tbl);
    void _update_contexts_from_state();

private:
    void populate_icols_in_flattened(
        const std::vector<t_rlookup>& lkup, t_table_sptr& flat) const;

    void _compile_computed_columns(const t_schema& tblschema, const t_ccol_vec&);
    void _edge_visit(t_uindex i, const std::vector<std::vector<t_uindex>>& edges,
            std::set<t_uindex>& topo_seen, const t_ccol_vec& ccols_unsorted,
            std::vector<t_uindex>& ccols_sorted_idx);

    t_gnode_processing_mode m_mode;
    t_gnode_type m_gnode_type;
    t_schema m_tblschema;
    t_schemavec m_ischemas;
    t_schemavec m_oschemas;
    t_bool m_init;
    t_port_sptrvec m_iports;
    t_port_sptrvec m_oports;
    std::map<t_str, t_ctx_handle> m_contexts;
    t_gstate_sptr m_state;
    t_uindex m_id;
    std::chrono::high_resolution_clock::time_point m_epoch;
    t_ccol_vec m_custom_columns;
    std::set<t_str> m_expr_icols;
    std::function<void()> m_pool_cleanup;
    t_bool m_was_updated;

    exprtk::symbol_table<t_float64> m_symbol_table;
    std::vector<exprtk::expression<t_float64>> m_expr_vec;
};

template <>
void t_gnode::_process_helper<t_str>(const t_column* fcolumn,
    const t_column* scolumn, t_column* dcolumn, t_column* pcolumn,
    t_column* ccolumn, t_column* tcolumn, const t_uint8* op_base,
    std::vector<t_rlookup>& lkup, std::vector<t_bool>& prev_pkey_eq_vec,
    std::vector<t_uindex>& added_vec);

template <typename CTX_T>
void
t_gnode::notify_context(const t_table& flattened, const t_ctx_handle& ctxh)
{
    CTX_T* ctx = ctxh.get<CTX_T>();
    const t_table& delta = *(m_oports[PSP_PORT_DELTA]->get_table().get());
    const t_table& prev = *(m_oports[PSP_PORT_PREV]->get_table().get());
    const t_table& current = *(m_oports[PSP_PORT_CURRENT]->get_table().get());
    const t_table& transitions
        = *(m_oports[PSP_PORT_TRANSITIONS]->get_table().get());
    const t_table& existed = *(m_oports[PSP_PORT_EXISTED]->get_table().get());
    notify_context<CTX_T>(
        ctx, flattened, delta, prev, current, transitions, existed);
}

template <typename CTX_T>
void
t_gnode::notify_context(CTX_T* ctx, const t_table& flattened,
    const t_table& delta, const t_table& prev, const t_table& current,
    const t_table& transitions, const t_table& existed)
{
    auto t1 = std::chrono::high_resolution_clock::now();
    ctx->step_begin();
    ctx->notify(flattened, delta, prev, current, transitions, existed);
    ctx->step_end();
    if (t_env::log_time_ctx_notify())
    {
        auto t2 = std::chrono::high_resolution_clock::now();
        std::cout << ctx->repr() << " ctx_notify "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(
                         t2 - t1)
                         .count()
                  << std::endl;
    }
}

template <typename CTX_T>
void
t_gnode::update_context_from_state(CTX_T* ctx, const t_table& tbl)
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    PSP_VERBOSE_ASSERT(m_mode == NODE_PROCESSING_SIMPLE_DATAFLOW,
        "Only simple dataflows supported currently");

    if (tbl.size() == 0)
        return;

    ctx->step_begin();
    ctx->notify(tbl);
    ctx->step_end();
}

template <typename DATA_T>
void
t_gnode::_process_helper(const t_column* fcolumn, const t_column* scolumn,
    t_column* dcolumn, t_column* pcolumn, t_column* ccolumn, t_column* tcolumn,
    const t_uint8* op_base, std::vector<t_rlookup>& lkup,
    std::vector<t_bool>& prev_pkey_eq_vec, std::vector<t_uindex>& added_vec)
{
    for (t_uindex idx = 0, loop_end = fcolumn->size(); idx < loop_end; ++idx)
    {
        t_uint8 op_ = op_base[idx];
        t_op op = static_cast<t_op>(op_);
        t_uindex added_count = added_vec[idx];

        const t_rlookup& rlookup = lkup[idx];
        t_bool row_pre_existed = rlookup.m_exists;

        switch (op)
        {
            case OP_INSERT:
            {
                row_pre_existed = row_pre_existed && !prev_pkey_eq_vec[idx];

                DATA_T prev_value;
                memset(&prev_value, 0, sizeof(DATA_T));
                t_bool prev_valid = false;

                DATA_T cur_value = *(fcolumn->get_nth<DATA_T>(idx));
                t_bool cur_valid = !fcolumn->is_invalid(idx);

                if (row_pre_existed)
                {
                    prev_value = *(scolumn->get_nth<DATA_T>(rlookup.m_idx));
                    prev_valid = !scolumn->is_invalid(rlookup.m_idx);
                }

                t_bool exists = cur_valid;
                t_bool prev_existed = row_pre_existed && prev_valid;
                t_bool prev_cur_eq = prev_value == cur_value;

                auto trans = calc_transition(prev_existed, row_pre_existed,
                    exists, prev_valid, cur_valid, prev_cur_eq,
                    prev_pkey_eq_vec[idx]);

                dcolumn->set_nth<DATA_T>(added_count,
                    cur_valid ? cur_value - prev_value : DATA_T(0));
                dcolumn->set_valid(added_count, true);

                pcolumn->set_nth<DATA_T>(added_count, prev_value);
                pcolumn->set_valid(added_count, prev_valid);

                ccolumn->set_nth<DATA_T>(
                    added_count, cur_valid ? cur_value : prev_value);

                ccolumn->set_valid(
                    added_count, cur_valid ? cur_valid : prev_valid);

                tcolumn->set_nth<t_uint8>(idx, trans);
            }
            break;
            case OP_DELETE:
            {
                if (row_pre_existed)
                {
                    DATA_T prev_value
                        = *(scolumn->get_nth<DATA_T>(rlookup.m_idx));
                    t_bool prev_valid = !scolumn->is_invalid(rlookup.m_idx);

                    pcolumn->set_nth<DATA_T>(added_count, prev_value);
                    pcolumn->set_valid(added_count, prev_valid);

                    ccolumn->set_nth<DATA_T>(added_count, prev_value);
                    ccolumn->set_valid(added_count, prev_valid);

                    SUPPRESS_WARNINGS_VC(4146)
                    dcolumn->set_nth<DATA_T>(added_count, -prev_value);
                    RESTORE_WARNINGS_VC()
                    dcolumn->set_valid(added_count, true);

                    tcolumn->set_nth<t_uint8>(
                        added_count, VALUE_TRANSITION_NEQ_TDF);
                }
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unknown OP");
            }
        }
    }
}

} // end namespace perspective
