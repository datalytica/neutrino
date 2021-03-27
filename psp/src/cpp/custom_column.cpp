/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#include <perspective/custom_column.h>

namespace perspective
{

t_custom_column::t_custom_column(const t_custom_column_recipe& ccr)
    : m_icols(ccr.m_icols)
    , m_ocol(ccr.m_ocol)
    , m_dtype(ccr.m_dtype)
    , m_expr(ccr.m_expr)
{
}

t_custom_column::t_custom_column(const std::vector<t_str>& icols,
    const t_str& ocol, const t_dtype dtype, const t_str& expr)
    : m_icols(icols)
    , m_ocol(ocol)
    , m_dtype(dtype)
    , m_expr(expr)
{
}

t_str
t_custom_column::get_ocol() const
{
    return m_ocol;
}

t_str
t_custom_column::get_expr() const
{
    return m_expr;
}

t_dtype
t_custom_column::get_dtype() const
{
    return m_dtype;
}

const std::vector<t_str>&
t_custom_column::get_icols() const
{
    return m_icols;
}

t_custom_column_recipe
t_custom_column::get_recipe() const
{
    t_custom_column_recipe rv;
    rv.m_icols = m_icols;
    rv.m_ocol = m_ocol;
    rv.m_expr = m_expr;
    return rv;
}

} // namespace perspective
