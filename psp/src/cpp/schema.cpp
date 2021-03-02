/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#include <perspective/first.h>
#include <perspective/schema.h>
#include <sstream>
#include <iostream>

namespace perspective
{

t_schema_recipe::t_schema_recipe() {}

t_schema::t_schema() {}

t_schema::t_schema(const t_schema_recipe& recipe)
    : t_schema(recipe.m_columns, recipe.m_types)
{
}

t_schema::t_schema(
    const std::vector<t_str>& columns, const std::vector<t_dtype>& types)
    : m_columns(columns)
    , m_types(types)
    , m_status_enabled(columns.size())
    , m_pkeyidx(0)
    , m_opidx(0)
{
    PSP_VERBOSE_ASSERT(columns.size() == types.size(), "Size mismatch");

    t_bool pkey_found = false;
    t_bool op_found = false;

    t_str pkey_str("psp_pkey");
    t_str op_str("psp_op");
    for (std::vector<t_str>::size_type idx = 0, loop_end = types.size();
         idx < loop_end; ++idx)
    {
        m_colidx_map[columns[idx]] = idx;
        PSP_VERBOSE_ASSERT(m_coldt_map.find(columns[idx]) == m_coldt_map.end(),
            "Duplicate column detected");
        m_coldt_map[columns[idx]] = types[idx];
        m_status_enabled[idx] = true;
        if (columns[idx] == pkey_str)
        {
            pkey_found = true;
            m_pkeyidx = idx;
        }

        if (columns[idx] == op_str)
        {
            op_found = true;
            m_opidx = idx;
        }
    }

    m_is_pkey = pkey_found && op_found;
}

t_uindex
t_schema::get_num_columns() const
{
    return m_columns.size();
}

t_uindex
t_schema::size() const
{
    return m_columns.size();
}

t_uindex
t_schema::get_colidx(const t_str& colname) const
{
    auto iter = m_colidx_map.find(colname);
    if (iter == m_colidx_map.end())
    {
        std::cout << "Column " << colname << " does not exist in schema."
                  << std::endl;
        PSP_COMPLAIN_AND_ABORT("");
    }
    return iter->second;
}

t_dtype
t_schema::get_dtype(const t_str& colname) const
{
    auto iter = m_coldt_map.find(colname);
    if (iter == m_coldt_map.end())
    {
        std::cout << "Column " << colname << " does not exist in schema."
                  << std::endl;
        PSP_COMPLAIN_AND_ABORT("");
    }
    return iter->second;
}

t_bool
t_schema::is_pkey() const
{
    return m_is_pkey;
}

t_bool
t_schema::operator==(const t_schema& rhs) const
{
    return m_columns == rhs.m_columns && m_types == rhs.m_types
        && m_status_enabled == rhs.m_status_enabled;
}

void
t_schema::add_column(const t_str& colname, t_dtype dtype)
{
    t_uindex idx = m_columns.size();
    m_columns.push_back(colname);
    m_status_enabled.push_back(true);
    m_types.push_back(dtype);
    m_colidx_map[colname] = idx;
    m_coldt_map[colname] = dtype;

    if (colname == t_str("psp_pkey"))
    {
        m_pkeyidx = idx;
        m_is_pkey = true;
    }

    if (colname == t_str("psp_op"))
    {
        m_opidx = idx;
        m_is_pkey = true;
    }
}

t_schema_recipe
t_schema::get_recipe() const
{
    t_schema_recipe rval;
    rval.m_columns = m_columns;
    rval.m_types = m_types;
    return rval;
}

t_bool
t_schema::has_column(const t_str& colname) const
{
    auto iter = m_colidx_map.find(colname);
    return iter != m_colidx_map.end();
}

const std::vector<t_str>&
t_schema::columns() const
{
    return m_columns;
}

const std::vector<t_dtype>
t_schema::types() const
{
    return m_types;
}

t_str
t_schema::str() const
{
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

t_schema
t_schema::drop(const std::set<t_str>& columns) const
{
    std::vector<t_str> cols;
    std::vector<t_dtype> types;

    for (t_uindex idx = 0, loop_end = m_columns.size(); idx < loop_end; ++idx)
    {
        if (columns.find(m_columns[idx]) == columns.end())
        {
            cols.push_back(m_columns[idx]);
            types.push_back(m_types[idx]);
        }
    }
    return t_schema(cols, types);
}

t_schema
t_schema::operator+(const t_schema& o) const
{
    t_schema rv(m_columns, m_types);
    for (t_uindex idx = 0, loop_end = o.m_columns.size(); idx < loop_end; ++idx)
    {
        rv.add_column(o.m_columns[idx], o.m_types[idx]);
    }
    return rv;
}

} // end namespace perspective

namespace std
{

std::ostream&
operator<<(std::ostream& os, const perspective::t_schema& s)
{
    using namespace perspective;
    const std::vector<t_str>& cols = s.columns();
    const std::vector<t_dtype>& types = s.types();

    os << "t_schema<\n";
    for (size_t idx = 0, loop_end = cols.size(); idx < loop_end; ++idx)
    {
        os << "\t" << idx << ". " << cols[idx] << ", "
           << get_dtype_descr(types[idx]) << std::endl;
    }
    os << ">\n";
    return os;
}
} // namespace std
