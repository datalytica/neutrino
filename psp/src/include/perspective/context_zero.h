/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once
#include <perspective/base.h>
#include <perspective/context_base.h>
#include <perspective/sort_specification.h>
#include <perspective/shared_ptrs.h>

namespace perspective
{

class t_table;

class PERSPECTIVE_EXPORT t_ctx0 : public t_ctxbase<t_ctx0>
{
public:
    t_ctx0();

    t_ctx0(const t_schema& schema, const t_config& config);

    ~t_ctx0();
#include <perspective/context_common_decls.h>

    t_tscalar get_column_name(t_index idx);

    std::vector<t_str> get_column_names() const;

    std::vector<t_tvidx> get_selected_indices() const;
    void select_node(t_tvidx idx);
    void deselect_node(t_tvidx idx);
    void clear_selection();

    void sort_by();
    t_sortsvec get_sort_by() const;

    using t_ctxbase<t_ctx0>::get_data;

protected:
    t_tscalvec get_all_pkeys(const std::vector<t_uidxpair>& cells) const;

    void calc_step_delta(const t_table& flattened, const t_table& prev,
        const t_table& curr, const t_table& transitions);

private:
    t_ftrav_sptr m_traversal;
    t_sptr_zcdeltas m_deltas;
    t_minmaxvec m_minmax;
    t_symtable_sptr m_symtable;
    t_bool m_has_delta;
};

typedef std::shared_ptr<t_ctx0> t_ctx0_sptr;

} // end namespace perspective
