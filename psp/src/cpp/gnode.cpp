/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#include <perspective/first.h>
#include <perspective/context_one.h>
#include <perspective/context_two.h>
#include <perspective/context_zero.h>
#include <perspective/context_grouped_pkey.h>
#include <perspective/gnode.h>
#include <perspective/gnode_state.h>
#include <perspective/mask.h>
#include <perspective/env_vars.h>
#include <perspective/logtime.h>
#include <perspective/utils.h>

namespace perspective
{

t_gnode::t_gnode(const t_gnode_recipe& recipe)
    : m_mode(recipe.m_mode)
    , m_gnode_type(recipe.m_gnode_type)
    , m_tblschema(recipe.m_tblschema)
    , m_init(false)
    , m_id(0)
    , m_pool_cleanup([]() {})
{
    PSP_TRACE_SENTINEL();
    LOG_CONSTRUCTOR("t_gnode");

    PSP_VERBOSE_ASSERT(recipe.m_mode == NODE_PROCESSING_SIMPLE_DATAFLOW,
        "Only simple dataflows supported currently");

    for (const auto& s : recipe.m_ischemas)
    {
        m_ischemas.push_back(t_schema(s));
    }

    PSP_VERBOSE_ASSERT(
        m_ischemas.size() == 1, "Single input port supported currently");

    for (const auto& s : recipe.m_oschemas)
    {
        m_oschemas.push_back(t_schema(s));
    }

    t_ccol_vec custom_columns;
    for (const auto& cc : recipe.m_custom_columns)
    {
        custom_columns.push_back(t_custom_column(cc));
    }

    m_epoch = std::chrono::high_resolution_clock::now();

    _compile_computed_columns(m_tblschema, custom_columns);
}

t_gnode::t_gnode(const t_gnode_options& options)
    : m_mode(NODE_PROCESSING_SIMPLE_DATAFLOW)
    , m_gnode_type(options.m_gnode_type)
    , m_init(false)
    , m_id(0)
    , m_pool_cleanup([]() {})
{
    PSP_TRACE_SENTINEL();
    LOG_CONSTRUCTOR("t_gnode");

    t_schema port_schema(options.m_port_schema);

    if (m_gnode_type == GNODE_TYPE_IMPLICIT_PKEYED) {
        // Make sure that gnode type is consistent with input schema
        if (port_schema.is_pkey()) {
            PSP_COMPLAIN_AND_ABORT("gnode type specified as implicit pkey, however input schema has psp_pkey column.");
        }
        port_schema = t_schema{{"psp_op", "psp_pkey"}, {DTYPE_UINT8, DTYPE_INT64}} + port_schema;
    } else {
        if (!(port_schema.is_pkey())) {
            PSP_COMPLAIN_AND_ABORT("gnode type specified as explicit pkey, however input schema is missing required columns.");
        }
    }

    // Add custom column outputs to port schema
    for (const auto& ccol: options.m_custom_columns) {
        port_schema.add_column(ccol.get_ocol(), ccol.get_dtype());
    }

    m_tblschema = port_schema.drop({"psp_op", "psp_pkey"});

    t_schema trans_schema(m_tblschema.columns(), std::vector<t_dtype>(m_tblschema.size(), DTYPE_UINT8));

    t_schema existed_schema({"psp_existed"}, {DTYPE_BOOL});

    m_ischemas = t_schemavec{port_schema};
    m_oschemas = t_schemavec{port_schema, m_tblschema, m_tblschema, m_tblschema,
        trans_schema, existed_schema};
    m_epoch = std::chrono::high_resolution_clock::now();

    _compile_computed_columns(m_tblschema, options.m_custom_columns);
}

t_gnode_sptr
t_gnode::build(const t_gnode_options& options)
{
    auto rv = std::make_shared<t_gnode>(options);
    rv->init();
    return rv;
}

t_gnode::~t_gnode()
{
    PSP_TRACE_SENTINEL();
    LOG_DESTRUCTOR("t_gnode");
    m_pool_cleanup();
}

void
t_gnode::init()
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(
        m_ischemas.size() == 1, "Single input port supported currently");

    m_state = std::make_shared<t_gstate>(m_tblschema, m_ischemas[0]);
    m_state->init();

    for (t_uindex idx = 0, loop_end = m_ischemas.size(); idx < loop_end; ++idx)
    {
        t_port_sptr port = std::make_shared<t_port>(m_ischemas[idx]);
        port->init();
        m_iports.push_back(port);
    }

    for (t_uindex idx = 0, loop_end = m_oschemas.size(); idx < loop_end; ++idx)
    {
        t_port_sptr port = std::make_shared<t_port>(m_oschemas[idx]);
        port->init();
        m_oports.push_back(port);
    }

    /* This doesn't seem to do anything */
    t_port_sptr& iport = m_iports[0];
    t_table_sptr flattened = iport->get_table()->flatten();
    for (t_index idx = 0; idx < m_custom_columns.size(); ++idx)
    {
        const auto& ccol = m_custom_columns[idx];
        flattened->fill_expr(ccol.get_icols(),
                             ccol.get_ocol(),
                             m_symbol_table,
                             m_expr_vec[idx]);
    }

    m_init = true;
}

t_str
t_gnode::repr() const
{
    std::stringstream ss;
    ss << "t_gnode<" << this << ">";
    return ss.str();
}

void
t_gnode::_send_and_process(const t_table& fragments)
{
    _send(0, fragments);
    _process();
}

void
t_gnode::_send(t_uindex portid, const t_table& fragments)
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    PSP_VERBOSE_ASSERT(
        portid == 0, "Only simple dataflows supported currently");

    if (m_gnode_type == GNODE_TYPE_IMPLICIT_PKEYED &&
        fragments.is_pkey_table()) {
        PSP_COMPLAIN_AND_ABORT("gnode type specified as implicit pkey, however input table has psp_pkey column.");
    }

    t_port_sptr& iport = m_iports[portid];
    iport->send(fragments);
}


void
t_gnode::_compile_computed_columns(const t_schema& tblschema, const t_ccol_vec& custom_columns)
{
    // Load up current set of table columns into symbol table
    m_symbol_table.clear();
    for (const auto& col: tblschema.columns()) {
        auto dtype = tblschema.get_dtype(col);
        if (is_numeric_type(dtype)) {
            m_symbol_table.create_variable(col);
        } else {
            m_symbol_table.create_stringvar(col);
        }
    }

    // Now compile all the expressions
    typedef exprtk::parser<t_float64>::settings_t settings_t;
    std::size_t compile_options = settings_t::e_replacer          +
                                  settings_t::e_joiner            +
                                  settings_t::e_numeric_check     +
                                  settings_t::e_bracket_check     +
                                  settings_t::e_sequence_check    +
                                  settings_t::e_strength_reduction;

    exprtk::parser<t_float64> parser(compile_options);

    parser.dec().collect_variables() = true;

    typedef typename exprtk::parser<t_float64>::dependent_entity_collector dec;

    std::vector<std::vector<t_uindex>> edges;
    t_ccol_vec ccols_unsorted;

    std::vector<exprtk::expression<t_float64>> expr_vec;

    m_expr_icols.clear();

    for (const auto& ccol : custom_columns)
    {
        exprtk::expression<t_float64> expression;
        expression.register_symbol_table(m_symbol_table);

        if (!parser.compile(ccol.get_expr(), expression))
        {

            std::cout << "Compilation error... expression '" << ccol.get_expr() << "'\n";
            for (std::size_t i = 0; i < parser.error_count(); ++i)
            {
                typedef exprtk::parser_error::type error_t;

                error_t error = parser.get_error(i);

                printf("Error[%02lu] Position: %02lu     Type: [%s] Msg: %s\n",
                    i,
                    error.token.position,
                    exprtk::parser_error::to_str(error.mode).c_str(),
                    error.diagnostic.c_str());

            }
            // Skip this computed column
            continue;
        }

        dec::symbol_list_t symbol_list;
        parser.dec().symbols(symbol_list);

        std::vector<t_str> symbols;
        std::transform(symbol_list.begin(), symbol_list.end(), std::back_inserter(symbols), 
            [](const dec::symbol_t& symbol) { return symbol.first; });

        expr_vec.push_back(expression);
        ccols_unsorted.push_back(
            t_custom_column(symbols, ccol.get_ocol(), ccol.get_dtype(), ccol.get_expr()
        ));

        std::copy(symbols.begin(), symbols.end(),
            std::inserter(m_expr_icols, m_expr_icols.begin()));

        std::vector<t_uindex> edge_list;
        for (const auto& symbol : symbols) {

            const auto citer = std::find_if(custom_columns.begin(), custom_columns.end(),
                 [symbol](const auto& ccol) -> bool { return ccol.get_ocol() == symbol; });

            if (citer != custom_columns.end()) {
                edge_list.push_back(citer - custom_columns.begin());
            }
        }
        edges.push_back(edge_list);
    }

    std::set<t_uindex> topo_seen;
    std::vector<t_uindex> ccols_sorted_idx;
    for (t_uindex i = 0; i < edges.size(); ++i) {
        _edge_visit(i, edges, topo_seen, ccols_unsorted, ccols_sorted_idx);
    }

    m_custom_columns.clear();
    m_expr_vec.clear();
    for (t_uindex idx: ccols_sorted_idx) {
        m_custom_columns.push_back(ccols_unsorted[idx]);
        m_expr_vec.push_back(expr_vec[idx]);
    }

    /*std::cout<<"custom columns - [";
    for (const auto ccol: ccols_unsorted) { std::cout << ccol.get_ocol() << ","; }
    std::cout<<"]" << std::endl;
    std::cout<<"after sort     - [";
    for (const auto ccol: m_custom_columns) { std::cout << ccol.get_ocol() << ","; }
    std::cout<<"]" << std::endl;*/
}

void
t_gnode::_edge_visit(t_uindex idx,
    const std::vector<std::vector<t_uindex>>& edges,
    std::set<t_uindex>& topo_seen,
    const t_ccol_vec& ccols_unsorted,
    std::vector<t_uindex>& ccols_sorted_idx)
{
    if (topo_seen.find(idx) != topo_seen.end()) {
        return;
    }
    topo_seen.insert(idx);
    for (auto cid: edges[idx]) {
        _edge_visit(cid, edges, topo_seen, ccols_unsorted, ccols_sorted_idx);
    }
    ccols_sorted_idx.push_back(idx);
}

void
t_gnode::populate_icols_in_flattened(
    const std::vector<t_rlookup>& lkup, t_table_sptr& flat) const
{
    PSP_VERBOSE_ASSERT(
        lkup.size() == flat->size(), "Mismatched sizes encountered");

    t_uindex nrows = lkup.size();
    t_uindex ncols = m_expr_icols.size();

    t_colcptrvec icols(ncols);
    t_colptrvec ocols(ncols);
    std::vector<t_str> cnames(ncols);

    t_uindex count = 0;
    const t_table* stable = get_table();

    for (const auto& cname : m_expr_icols)
    {
        icols[count] = stable->get_const_column(cname).get();
        ocols[count] = flat->get_column(cname).get();
        cnames[count] = cname;
        ++count;
    }

#ifdef PSP_PARALLEL_FOR
    PSP_PFOR(0, int(ncols), 1,
        [&lkup, &icols, &ocols, nrows](int colidx)
#else
    for (t_uindex colidx = 0; colidx < ncols; ++colidx)
#endif
        {
            auto icol = icols[colidx];
            auto ocol = ocols[colidx];

            for (t_uindex ridx = 0; ridx < nrows; ++ridx)
            {
                const auto& lk = lkup[ridx];
                if (ocol->is_invalid(ridx) && lk.m_exists)
                {
                    ocol->set_scalar(ridx, icol->get_scalar(lk.m_idx));
                }
            }
        }

#ifdef PSP_PARALLEL_FOR
    );
#endif
}

void
t_gnode::_process()
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    m_was_updated = false;
    PSP_VERBOSE_ASSERT(m_mode == NODE_PROCESSING_SIMPLE_DATAFLOW,
        "Only simple dataflows supported currently");
    psp_log_time(repr() + " _process.enter");
    auto t1 = std::chrono::high_resolution_clock::now();

    t_port_sptr& iport = m_iports[0];

    if (iport->get_table()->size() == 0)
    {
        return;
    }

    m_was_updated = true;

    if (m_gnode_type == GNODE_TYPE_IMPLICIT_PKEYED) {
        // Add implicit pkey
        auto tbl = iport->get_table();

        auto op_col = tbl->get_column("psp_op");
        op_col->raw_fill<t_uint8>(OP_INSERT);

        auto key_col = tbl->get_column("psp_pkey");

        // Get current table size as starting index
        t_uindex start = get_table()->size();

        for (t_uindex ridx = 0; ridx < tbl->size(); ++ridx)
        {
            key_col->set_nth<t_int64>(ridx, start + ridx);
        }
    }

    t_table_sptr flattened(iport->get_table()->flatten());
    PSP_GNODE_VERIFY_TABLE(flattened);
    PSP_GNODE_VERIFY_TABLE(get_table());

    psp_log_time(repr() + " _process.post_flatten");

    if (t_env::log_data_gnode_flattened())
    {
        std::cout << repr() << "gnode_process_flattened" << std::endl;
        flattened->pprint();
    }

    if (t_env::log_schema_gnode_flattened())
    {
        std::cout << repr() << "gnode_schema_flattened" << std::endl;
        std::cout << flattened->get_schema();
    }

    if (m_state->mapping_size() == 0)
    {
        for (t_index idx = 0; idx < m_custom_columns.size(); ++idx)
        {
            const auto& ccol = m_custom_columns[idx];
            flattened->fill_expr(ccol.get_icols(),
                                 ccol.get_ocol(),
                                 m_symbol_table,
                                 m_expr_vec[idx]);
        }
        psp_log_time(repr() + " _process.init_path.post_fill_expr");

        m_state->update_history(flattened.get());
        psp_log_time(repr() + " _process.init_path.post_update_history");
        _update_contexts_from_state(*flattened);
        psp_log_time(
            repr() + " _process.init_path.post_update_contexts_from_state");
        m_oports[PSP_PORT_FLATTENED]->set_table(flattened);

        release_inputs();
        psp_log_time(repr() + " _process.init_path.post_release_inputs");

        release_outputs();
        psp_log_time(repr() + " _process.init_path.exit");

#ifdef PSP_GNODE_VERIFY
        auto stable = get_table();
        PSP_GNODE_VERIFY_TABLE(stable);
#endif

        return;
    }

    for (t_uindex idx = 0, loop_end = m_iports.size(); idx < loop_end; ++idx)
    {
        m_iports[idx]->release_or_clear();
    }

    t_uindex fnrows = flattened->num_rows();

    t_table_sptr delta = m_oports[PSP_PORT_DELTA]->get_table();
    delta->clear();
    delta->reserve(fnrows);

    t_table_sptr prev = m_oports[PSP_PORT_PREV]->get_table();
    prev->clear();
    prev->reserve(fnrows);

    t_table_sptr current = m_oports[PSP_PORT_CURRENT]->get_table();
    current->clear();
    current->reserve(fnrows);

    t_table_sptr transitions = m_oports[PSP_PORT_TRANSITIONS]->get_table();
    transitions->clear();
    transitions->reserve(fnrows);

    t_table_sptr existed = m_oports[PSP_PORT_EXISTED]->get_table();
    existed->clear();
    existed->reserve(fnrows);
    existed->set_size(fnrows);

    const t_schema& fschema = flattened->get_schema();

    t_col_sptr pkey_col_sptr = flattened->get_column("psp_pkey");
    t_col_sptr op_col_sptr = flattened->get_column("psp_op");

    t_column* pkey_col = pkey_col_sptr.get();
    t_column* op_col = op_col_sptr.get();
    t_table* stable = get_table();
    PSP_GNODE_VERIFY_TABLE(stable);
    const t_schema& sschema = m_state->get_schema();

    t_colcptrvec fcolumns(flattened->num_columns());
    t_uindex ncols = sschema.get_num_columns();

    t_colcptrvec scolumns(ncols);
    t_colptrvec dcolumns(ncols);
    t_colptrvec pcolumns(ncols);
    t_colptrvec ccolumns(ncols);
    t_colptrvec tcolumns(ncols);

    std::vector<t_uindex> col_translation(stable->num_columns());
    t_uindex count = 0;

    t_str opname("psp_op");
    t_str pkeyname("psp_pkey");

    for (t_uindex idx = 0, loop_end = fschema.size(); idx < loop_end; ++idx)
    {
        const t_str& cname = fschema.m_columns[idx];
        if (cname != opname && cname != pkeyname)
        {
            col_translation[count] = idx;
            fcolumns[idx] = flattened->get_column(cname).get();
            ++count;
        }
    }

    for (t_uindex idx = 0, loop_end = sschema.size(); idx < loop_end; ++idx)
    {
        const t_str& cname = sschema.m_columns[idx];
        scolumns[idx] = stable->get_column(cname).get();
        pcolumns[idx] = prev->get_column(cname).get();
        ccolumns[idx] = current->get_column(cname).get();
        dcolumns[idx] = delta->get_column(cname).get();
        tcolumns[idx] = transitions->get_column(cname).get();
    }

    t_column* ecolumn = existed->get_column("psp_existed").get();

    t_tscalar prev_pkey;
    prev_pkey.clear();

    const t_gstate& cstate = *(m_state.get());

    t_tscalvec existing_insert_pkeys;

    t_mask mask(fnrows);

    t_uindex added_count = 0;

    auto op_base = op_col->get_nth<t_uint8>(0);
    std::vector<t_uindex> added_offset(fnrows);
    std::vector<t_rlookup> lkup(fnrows);
    std::vector<t_bool> prev_pkey_eq_vec(fnrows);

    for (t_uindex idx = 0; idx < fnrows; ++idx)
    {
        t_tscalar pkey = pkey_col->get_scalar(idx);
        t_uint8 op_ = op_base[idx];
        t_op op = static_cast<t_op>(op_);

        lkup[idx] = cstate.lookup(pkey);
        t_bool row_pre_existed = lkup[idx].m_exists;
        prev_pkey_eq_vec[idx] = pkey == prev_pkey;

        added_offset[idx] = added_count;

        switch (op)
        {
            case OP_INSERT:
            {
                row_pre_existed = row_pre_existed && !prev_pkey_eq_vec[idx];
                mask.set(idx, true);
                ecolumn->set_nth(added_count, row_pre_existed);
                ++added_count;
            }
            break;
            case OP_DELETE:
            {
                if (row_pre_existed)
                {
                    mask.set(idx, true);
                    ecolumn->set_nth(added_count, row_pre_existed);
                    ++added_count;
                }
                else
                {
                    mask.set(idx, false);
                }
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unknown OP");
            }
        }

        prev_pkey = pkey;
    }

    auto mask_count = mask.count();

    PSP_VERBOSE_ASSERT(mask_count == added_count, "Expected equality");

    delta->set_size(mask_count);
    prev->set_size(mask_count);
    current->set_size(mask_count);
    transitions->set_size(mask_count);
    existed->set_size(mask_count);

    psp_log_time(repr() + " _process.noinit_path.post_rlkup_loop");
    if (!m_expr_icols.empty())
    {
        populate_icols_in_flattened(lkup, flattened);
        for (t_index idx = 0; idx < m_custom_columns.size(); ++idx)
        {
            const auto& ccol = m_custom_columns[idx];
            flattened->fill_expr(ccol.get_icols(),
                                 ccol.get_ocol(),
                                 m_symbol_table,
                                 m_expr_vec[idx]);
        }
    }

#ifdef PSP_PARALLEL_FOR
        [&fcolumns, &scolumns, &dcolumns, &pcolumns, &ccolumns, &tcolumns,
            &col_translation, &op_base, &lkup, &prev_pkey_eq_vec, &added_offset,
            this](int colidx)
#else
    for (t_uindex colidx = 0; colidx < ncols; ++colidx)
#endif
        {
        auto fcolumn = fcolumns[col_translation[colidx]];
        auto scolumn = scolumns[colidx];
        auto dcolumn = dcolumns[colidx];
        auto pcolumn = pcolumns[colidx];
        auto ccolumn = ccolumns[colidx];
        auto tcolumn = tcolumns[colidx];

        t_dtype col_dtype = fcolumn->get_dtype();

        switch (col_dtype)
        {
            case DTYPE_INT64:
            {
                _process_helper<t_int64>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_INT32:
            {
                _process_helper<t_int32>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_INT16:
            {
                _process_helper<t_int16>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_INT8:
            {
                _process_helper<t_int8>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_UINT64:
            {
                _process_helper<t_uint64>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_UINT32:
            {
                _process_helper<t_uint32>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_UINT16:
            {
                _process_helper<t_uint16>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_UINT8:
            {
                _process_helper<t_uint8>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_FLOAT64:
            {
                _process_helper<t_float64>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_FLOAT32:
            {
                _process_helper<t_float32>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_BOOL:
            {
                _process_helper<t_uint8>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_TIME:
            {
                _process_helper<t_int64>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_DATE:
            {
                _process_helper<t_uint32>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            case DTYPE_STR:
            {
                _process_helper<t_str>(fcolumn, scolumn, dcolumn, pcolumn,
                    ccolumn, tcolumn, op_base, lkup, prev_pkey_eq_vec,
                    added_offset);
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unsupported column dtype");
            }
        }
        }
#ifdef PSP_PARALLEL_FOR
    );
#endif

        psp_log_time(repr() + " _process.noinit_path.post_process_helper");

        t_table_sptr flattened_masked = mask.count() == flattened->size()
            ? flattened
            : flattened->clone(mask);
        PSP_GNODE_VERIFY_TABLE(flattened_masked);
#ifdef PSP_GNODE_VERIFY
        {
            auto updated_table = get_table();
            PSP_GNODE_VERIFY_TABLE(updated_table);
        }
#endif
        m_state->update_history(flattened_masked.get());
#ifdef PSP_GNODE_VERIFY
        {
            auto updated_table = get_table();
            PSP_GNODE_VERIFY_TABLE(updated_table);
        }
#endif

        psp_log_time(repr() + " _process.noinit_path.post_update_history");

        m_oports[PSP_PORT_FLATTENED]->set_table(flattened_masked);

        if (t_env::log_data_gnode_flattened_mask())
        {
            std::cout << repr() << "gnode_process_flattened_mask" << std::endl;
            flattened_masked->pprint();
            std::cout << std::endl;
        }

        if (t_env::log_data_gnode_delta())
        {
            std::cout << repr() << "gnode_process_delta" << std::endl;
            delta->pprint();
            std::cout << std::endl;
        }

        if (t_env::log_data_gnode_prev())
        {
            std::cout << repr() << "gnode_process_prev" << std::endl;
            prev->pprint();
            std::cout << std::endl;
        }

        if (t_env::log_data_gnode_current())
        {
            std::cout << repr() << "gnode_process_current" << std::endl;
            current->pprint();
            std::cout << std::endl;
        }

        if (t_env::log_data_gnode_transitions())
        {
            std::cout << repr() << "gnode_process_transitions" << std::endl;
            transitions->pprint();
            std::cout << std::endl;
        }

        if (t_env::log_data_gnode_existed())
        {
            std::cout << repr() << "gnode_process_existed" << std::endl;
            existed->pprint();
            std::cout << std::endl;
        }

        if (t_env::log_time_gnode_process())
        {
            auto t2 = std::chrono::high_resolution_clock::now();
            std::cout << repr() << " gnode_process_time "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(
                             t2 - t1)
                             .count()
                      << std::endl;
            std::cout << repr() << "gnode_process_time since begin=> "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(
                             t2 - m_epoch)
                             .count()
                      << std::endl;
            std::cout << std::endl;
        }

        notify_contexts(*flattened_masked);

        psp_log_time(repr() + " _process.noinit_path.exit");
}

t_table*
t_gnode::get_table()
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    return m_state->get_table().get();
}

const t_table*
t_gnode::get_table() const
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    return m_state->get_table().get();
}

void
t_gnode::pprint() const
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    m_state->pprint();
}

template <typename CTX_T>
void
t_gnode::set_ctx_state(void* ptr)
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    CTX_T* ctx = static_cast<CTX_T*>(ptr);
    ctx->set_state(m_state);
}

void
t_gnode::_update_contexts_from_state(const t_table& tbl)
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");

    for (auto& kv : m_contexts)
    {
        auto& ctxh = kv.second;
        switch (ctxh.m_ctx_type)
        {
            case TWO_SIDED_CONTEXT:
            {
                auto ctx = static_cast<t_ctx2*>(ctxh.m_ctx);
                ctx->reset();
                update_context_from_state<t_ctx2>(ctx, tbl);
            }
            break;
            case ONE_SIDED_CONTEXT:
            {
                auto ctx = static_cast<t_ctx1*>(ctxh.m_ctx);
                ctx->reset();
                update_context_from_state<t_ctx1>(ctx, tbl);
            }
            break;
            case ZERO_SIDED_CONTEXT:
            {
                auto ctx = static_cast<t_ctx0*>(ctxh.m_ctx);
                ctx->reset();
                update_context_from_state<t_ctx0>(ctx, tbl);
            }
            break;
            case GROUPED_PKEY_CONTEXT:
            {
                auto ctx = static_cast<t_ctx_grouped_pkey*>(ctxh.m_ctx);
                ctx->reset();
                update_context_from_state<t_ctx_grouped_pkey>(ctx, tbl);
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unexpected context type");
            }
            break;
        }
    }
}

std::vector<t_str>
t_gnode::get_registered_contexts() const
{
    std::vector<t_str> rval;

    for (const auto& kv : m_contexts)
    {
        std::stringstream ss;
        const auto& ctxh = kv.second;
        ss << "(ctx_name => " << kv.first << ", ";

        switch (ctxh.m_ctx_type)
        {
            case TWO_SIDED_CONTEXT:
            {
                auto ctx = static_cast<const t_ctx2*>(ctxh.m_ctx);
                ss << ctx->repr() << ")";
            }
            break;
            case ONE_SIDED_CONTEXT:
            {
                auto ctx = static_cast<const t_ctx1*>(ctxh.m_ctx);
                ss << ctx->repr() << ")";
            }
            break;
            case ZERO_SIDED_CONTEXT:
            {
                auto ctx = static_cast<const t_ctx0*>(ctxh.m_ctx);
                ss << ctx->repr() << ")";
            }
            break;
            case GROUPED_PKEY_CONTEXT:
            {
                auto ctx = static_cast<const t_ctx_grouped_pkey*>(ctxh.m_ctx);
                ss << ctx->repr() << ")";
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unexpected context type");
            }
            break;
        }

        rval.push_back(ss.str());
    }

    return rval;
}

void
t_gnode::_update_contexts_from_state()
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    auto flattened = m_state->get_pkeyed_table();
    _update_contexts_from_state(*flattened);
}

void
t_gnode::_register_context(const t_str& name, t_ctx_type type, t_int64 ptr)
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    void* ptr_ = reinterpret_cast<void*>(ptr);
    t_ctx_handle ch(ptr_, type);
    m_contexts[name] = ch;

    t_bool should_update = m_state->mapping_size() > 0;

    t_table_sptr flattened;

    if (should_update)
    {
        flattened = m_state->get_pkeyed_table();
    }

    switch (type)
    {
        case TWO_SIDED_CONTEXT:
        {
            set_ctx_state<t_ctx2>(ptr_);
            t_ctx2* ctx = static_cast<t_ctx2*>(ptr_);
            if (t_env::log_progress())
            {
                std::cout << repr() << " << gnode.register_context: "
                          << " name => " << name << " type => " << type
                          << " ctx => " << ctx->repr() << std::endl;
            }

            ctx->reset();

            if (should_update)
                update_context_from_state<t_ctx2>(ctx, *flattened);
        }
        break;
        case ONE_SIDED_CONTEXT:
        {
            set_ctx_state<t_ctx1>(ptr_);
            t_ctx1* ctx = static_cast<t_ctx1*>(ptr_);
            if (t_env::log_progress())
            {
                std::cout << repr() << " << gnode.register_context: "
                          << " name => " << name << " type => " << type
                          << " ctx => " << ctx->repr() << std::endl;
            }

            ctx->reset();

            if (should_update)
                update_context_from_state<t_ctx1>(ctx, *flattened);
        }
        break;
        case ZERO_SIDED_CONTEXT:
        {
            set_ctx_state<t_ctx0>(ptr_);
            t_ctx0* ctx = static_cast<t_ctx0*>(ptr_);
            if (t_env::log_progress())
            {
                std::cout << repr() << " << gnode.register_context: "
                          << " name => " << name << " type => " << type
                          << " ctx => " << ctx->repr() << std::endl;
            }

            ctx->reset();

            if (should_update)
                update_context_from_state<t_ctx0>(ctx, *flattened);
        }
        break;
        case GROUPED_PKEY_CONTEXT:
        {
            set_ctx_state<t_ctx0>(ptr_);
            auto ctx = static_cast<t_ctx_grouped_pkey*>(ptr_);
            if (t_env::log_progress())
            {
                std::cout << repr() << " << gnode.register_context: "
                          << " name => " << name << " type => " << type
                          << " ctx => " << ctx->repr() << std::endl;
            }

            ctx->reset();

            if (should_update)
                update_context_from_state<t_ctx_grouped_pkey>(ctx, *flattened);
        }
        break;
        default:
        {
            PSP_COMPLAIN_AND_ABORT("Unexpected context type");
        }
        break;
    }
}

void
t_gnode::_unregister_context(const t_str& name)
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    if ((m_contexts.find(name) == m_contexts.end()))
        return;

    PSP_VERBOSE_ASSERT(
        m_contexts.find(name) != m_contexts.end(), "Context not found.");
    m_contexts.erase(name);
}

void
t_gnode::notify_contexts(const t_table& flattened)
{
    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");
    psp_log_time(repr() + "notify_contexts.enter");
    t_index num_ctx = m_contexts.size();
    std::vector<t_ctx_handle> ctxhvec(num_ctx);

    t_index ctxh_count = 0;
    for (const auto& context : m_contexts)
    {
        ctxhvec[ctxh_count] = context.second;
        ctxh_count++;
    }

    auto notify_context_helper = [this, &ctxhvec, &flattened](t_index ctxidx) {
        const t_ctx_handle& ctxh = ctxhvec[ctxidx];
        switch (ctxh.get_type())
        {
            case TWO_SIDED_CONTEXT:
            {
                notify_context<t_ctx2>(flattened, ctxh);
            }
            break;
            case ONE_SIDED_CONTEXT:
            {
                notify_context<t_ctx1>(flattened, ctxh);
            }
            break;
            case ZERO_SIDED_CONTEXT:
            {
                notify_context<t_ctx0>(flattened, ctxh);
            }
            break;
            case GROUPED_PKEY_CONTEXT:
            {
                notify_context<t_ctx_grouped_pkey>(flattened, ctxh);
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unexpected context type");
            }
            break;
        }
    };

    if (has_python_dep())
    {
        for (t_index ctxidx = 0; ctxidx < num_ctx; ++ctxidx)
        {
            notify_context_helper(ctxidx);
        }
    }
    else
    {
#ifdef PSP_PARALLEL_FOR
        PSP_PFOR(0, int(num_ctx), 1,
            [this, &notify_context_helper](int ctxidx)
#else
        for (t_index ctxidx = 0; ctxidx < num_ctx; ++ctxidx)
#endif
            { notify_context_helper(ctxidx); }

#ifdef PSP_PARALLEL_FOR
        );
#endif
    }

    psp_log_time(repr() + "notify_contexts.exit");
}

t_streeptr_vec
t_gnode::get_trees()
{

    PSP_TRACE_SENTINEL();
    PSP_VERBOSE_ASSERT(m_init, "touching uninited object");

    t_streeptr_vec rval;

    for (const auto& kv : m_contexts)
    {
        auto& ctxh = kv.second;

        switch (ctxh.m_ctx_type)
        {
            case TWO_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx2*>(ctxh.m_ctx);
                auto trees = ctx->get_trees();
                rval.insert(rval.end(), std::begin(trees), std::end(trees));
            }
            break;
            case ONE_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx1*>(ctxh.m_ctx);
                auto trees = ctx->get_trees();
                rval.insert(rval.end(), std::begin(trees), std::end(trees));
            }
            break;
            case ZERO_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx0*>(ctxh.m_ctx);
                auto trees = ctx->get_trees();
                rval.insert(rval.end(), std::begin(trees), std::end(trees));
            }
            break;
            case GROUPED_PKEY_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx_grouped_pkey*>(ctxh.m_ctx);
                auto trees = ctx->get_trees();
                rval.insert(rval.end(), std::begin(trees), std::end(trees));
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unexpected context type");
            }
            break;
        }
    }
    return rval;
}

void
t_gnode::set_id(t_uindex id)
{
    m_id = id;
}

t_uindex
t_gnode::get_id() const
{
    return m_id;
}

void
t_gnode::release_inputs()
{
    for (const auto& p : m_iports)
    {
        p->release();
    }
}

void
t_gnode::release_outputs()
{
    for (const auto& p : m_oports)
    {
        p->release();
    }
}

std::vector<t_str>
t_gnode::get_contexts_last_updated() const
{
    std::vector<t_str> rval;

    for (const auto& kv : m_contexts)
    {
        auto ctxh = kv.second;
        switch (ctxh.m_ctx_type)
        {
            case TWO_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx2*>(ctxh.m_ctx);
                if (ctx->has_deltas())
                {
                    rval.push_back(kv.first);
                }
            }
            break;
            case ONE_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx1*>(ctxh.m_ctx);
                if (ctx->has_deltas())
                {
                    rval.push_back(kv.first);
                }
            }
            break;
            case ZERO_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx0*>(ctxh.m_ctx);
                if (ctx->has_deltas())
                {
                    rval.push_back(kv.first);
                }
            }
            break;
            case GROUPED_PKEY_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx_grouped_pkey*>(ctxh.m_ctx);
                if (ctx->has_deltas())
                {
                    rval.push_back(kv.first);
                }
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unexpected context type");
            }
            break;
        }
    }

    if (t_env::log_progress())
    {
        std::cout << "get_contexts_last_updated<" << std::endl;
        for (const auto& s : rval)
        {
            std::cout << "\t" << s << std::endl;
        }
        std::cout << ">\n";
    }
    return rval;
}

t_tscalvec
t_gnode::get_row_data_pkeys(const t_tscalvec& pkeys) const
{
    return m_state->get_row_data_pkeys(pkeys);
}

t_tscalvec
t_gnode::has_pkeys(const t_tscalvec& pkeys) const
{
    return m_state->has_pkeys(pkeys);
}

t_tscalvec
t_gnode::get_pkeys() const
{
    return m_state->get_pkeys();
}

void
t_gnode::reset()
{
    std::vector<t_str> rval;

    for (const auto& kv : m_contexts)
    {
        auto ctxh = kv.second;
        switch (ctxh.m_ctx_type)
        {
            case TWO_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx2*>(ctxh.m_ctx);
                ctx->reset();
            }
            break;
            case ONE_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx1*>(ctxh.m_ctx);
                ctx->reset();
            }
            break;
            case ZERO_SIDED_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx0*>(ctxh.m_ctx);
                ctx->reset();
            }
            break;
            case GROUPED_PKEY_CONTEXT:
            {
                auto ctx = reinterpret_cast<t_ctx_grouped_pkey*>(ctxh.m_ctx);
                ctx->reset();
            }
            break;
            default:
            {
                PSP_COMPLAIN_AND_ABORT("Unexpected context type");
            }
            break;
        }
    }

    m_state->reset();
}

void
t_gnode::clear_input_ports()
{
    for (t_uindex idx = 0, loop_end = m_oports.size(); idx < loop_end; ++idx)
    {
        m_iports[idx]->get_table()->clear();
    }
}

void
t_gnode::clear_output_ports()
{
    for (t_uindex idx = 0, loop_end = m_oports.size(); idx < loop_end; ++idx)
    {
        m_oports[idx]->get_table()->clear();
    }
}

template <>
void
t_gnode::_process_helper<t_str>(const t_column* fcolumn,
    const t_column* scolumn, t_column* dcolumn, t_column* pcolumn,
    t_column* ccolumn, t_column* tcolumn, const t_uint8* op_base,
    std::vector<t_rlookup>& lkup, std::vector<t_bool>& prev_pkey_eq_vec,
    std::vector<t_uindex>& added_vec)
{
    for (t_uindex idx = 0, loop_end = fcolumn->size(); idx < loop_end; ++idx)
    {
        pcolumn->borrow_vocabulary(*scolumn);
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

                const char* prev_value = 0;
                t_bool prev_valid = false;

                auto cur_value = fcolumn->get_nth<const char>(idx);
                //t_str curs(cur_value);

                t_bool cur_valid = !fcolumn->is_invalid(idx);

                if (row_pre_existed)
                {
                    prev_value = scolumn->get_nth<const char>(rlookup.m_idx);
                    prev_valid = !scolumn->is_invalid(rlookup.m_idx);
                }

                t_bool exists = cur_valid;
                t_bool prev_existed = row_pre_existed && prev_valid;
                t_bool prev_cur_eq = prev_value && cur_value
                    && strcmp(prev_value, cur_value) == 0;

                auto trans = calc_transition(prev_existed, row_pre_existed,
                    exists, prev_valid, cur_valid, prev_cur_eq,
                    prev_pkey_eq_vec[idx]);

                if (prev_valid)
                {
                    pcolumn->set_nth<t_uindex>(added_count,
                        *(scolumn->get_nth<t_uindex>(rlookup.m_idx)));
                }

                pcolumn->set_valid(added_count, prev_valid);
                if (scolumn->is_cleared(rlookup.m_idx)) {
                    pcolumn->clear(added_count, STATUS_CLEAR);
                }

                if (cur_valid)
                {
                    ccolumn->set_nth<const char*>(added_count, cur_value);
                }

                if (!cur_valid && prev_valid)
                {
                    ccolumn->set_nth<const char*>(added_count, prev_value);
                }

                ccolumn->set_valid(
                    added_count, cur_valid ? cur_valid : prev_valid);

                if (fcolumn->is_cleared(idx)) {
                    ccolumn->clear(added_count, STATUS_CLEAR);
                }

                tcolumn->set_nth<t_uint8>(idx, trans);
            }
            break;
            case OP_DELETE:
            {
                if (row_pre_existed)
                {
                    auto prev_value
                        = scolumn->get_nth<const char>(rlookup.m_idx);

                    t_bool prev_valid = !scolumn->is_invalid(rlookup.m_idx);

                    pcolumn->set_nth<const char*>(added_count, prev_value);

                    pcolumn->set_valid(added_count, prev_valid);

                    ccolumn->set_nth<const char*>(added_count, prev_value);

                    ccolumn->set_valid(added_count, prev_valid);

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

t_table*
t_gnode::_get_pkeyed_table() const
{
    return m_state->_get_pkeyed_table();
}

t_ccol_vec
t_gnode::get_custom_columns() const
{
    return m_custom_columns;
}

t_value_transition
t_gnode::calc_transition(t_bool prev_existed, t_bool row_pre_existed,
    t_bool exists, t_bool prev_valid, t_bool cur_valid, t_bool prev_cur_eq,
    t_bool prev_pkey_eq)
{
    t_value_transition trans = VALUE_TRANSITION_EQ_FF;

    if (!row_pre_existed && !cur_valid && !t_env::backout_invalid_neq_ft())
    {
        trans = VALUE_TRANSITION_NEQ_FT;
    }
    else if (row_pre_existed && !prev_valid && !cur_valid
        && !t_env::backout_eq_invalid_invalid())
    {
        trans = VALUE_TRANSITION_EQ_TT;
    }
    else if (!prev_existed && !exists)
    {
        trans = VALUE_TRANSITION_EQ_FF;
    }
    else if (row_pre_existed && exists && !prev_valid && cur_valid
        && !t_env::backout_nveq_ft())
    {
        trans = VALUE_TRANSITION_NVEQ_FT;
    }
    else if (prev_existed && exists && prev_cur_eq)
    {
        trans = VALUE_TRANSITION_EQ_TT;
    }
    else if (!prev_existed && exists)
    {
        trans = VALUE_TRANSITION_NEQ_FT;
    }
    else if (prev_existed && !exists)
    {
        trans = VALUE_TRANSITION_NEQ_TF;
    }

    else if (prev_existed && exists && !prev_cur_eq)
    {
        trans = VALUE_TRANSITION_NEQ_TT;
    }
    else if (prev_pkey_eq)
    {
        // prev op must have been a delete
        trans = VALUE_TRANSITION_NEQ_TDT;
    }
    else
    {
        PSP_COMPLAIN_AND_ABORT("Hit unexpected condition");
    }
    return trans;
}

t_gnode_recipe
t_gnode::get_recipe() const
{
    t_gnode_recipe rv;
    rv.m_mode = m_mode;

    rv.m_tblschema = m_tblschema.get_recipe();

    for (const auto& s : m_ischemas)
    {
        rv.m_ischemas.push_back(s.get_recipe());
    }

    for (const auto& s : m_oschemas)
    {
        rv.m_oschemas.push_back(s.get_recipe());
    }

    for (const auto& cc : m_custom_columns)
    {
        rv.m_custom_columns.push_back(cc.get_recipe());
    }
    return rv;
}

t_bool
t_gnode::has_python_dep() const
{
    return !m_custom_columns.empty();
}

void
t_gnode::set_pool_cleanup(std::function<void()> cleanup)
{
    m_pool_cleanup = cleanup;
}

t_bool
t_gnode::was_updated() const
{
    return m_was_updated;
}

void
t_gnode::clear_updated()
{
    m_was_updated = false;
}

t_table_sptr
t_gnode::get_sorted_pkeyed_table() const
{
    return m_state->get_sorted_pkeyed_table();
}

// helper function for tests
t_table_sptr
t_gnode::tstep(t_table_csptr input_table)
{
    _send_and_process(*input_table);
    return get_sorted_pkeyed_table();
}

void
t_gnode::register_context(const t_str& name, t_ctx0_sptr ctx)
{
    _register_context(
        name, ZERO_SIDED_CONTEXT, reinterpret_cast<t_int64>(ctx.get()));
}

void
t_gnode::register_context(const t_str& name, t_ctx1_sptr ctx)
{
    _register_context(
        name, ONE_SIDED_CONTEXT, reinterpret_cast<t_int64>(ctx.get()));
}

void
t_gnode::register_context(const t_str& name, t_ctx2_sptr ctx)
{
    _register_context(
        name, TWO_SIDED_CONTEXT, reinterpret_cast<t_int64>(ctx.get()));
}
void
t_gnode::register_context(const t_str& name, t_ctx_grouped_pkey_sptr ctx)
{
    _register_context(
        name, GROUPED_PKEY_CONTEXT, reinterpret_cast<t_int64>(ctx.get()));
}

t_schema
t_gnode::get_tblschema() const
{
    return m_tblschema;
}

} // end namespace perspective
