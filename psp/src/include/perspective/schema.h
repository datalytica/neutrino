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
#include <perspective/raw_types.h>
#include <perspective/base.h>
#include <perspective/exports.h>
#include <vector>
#include <map>
#include <set>

namespace perspective
{

struct PERSPECTIVE_EXPORT t_schema_recipe
{
    t_schema_recipe();
    std::vector<t_str> m_columns;
    std::vector<t_dtype> m_types;
};

typedef std::vector<t_schema_recipe> t_schema_recipevec;

struct PERSPECTIVE_EXPORT t_schema
{
    typedef std::map<t_str, t_dtype> t_sdtmap;
    typedef std::map<t_str, t_uindex> t_suidxmap;

    t_schema();
    t_schema(const t_schema_recipe& recipe);
    t_schema(
        const std::vector<t_str>& columns, const std::vector<t_dtype>& types);
    t_uindex get_num_columns() const;
    t_uindex size() const;

    t_uindex get_colidx(const t_str& colname) const;
    t_dtype get_dtype(const t_str& colname) const;

    t_bool is_pkey() const;

    t_bool operator==(const t_schema& rhs) const;

    void add_column(const t_str& colname, t_dtype dtype);
    t_schema_recipe get_recipe() const;
    t_bool has_column(const t_str& cname) const;
    const std::vector<t_str>& columns() const;
    const std::vector<t_dtype> types() const;
    t_str str() const;
    t_schema drop(const std::set<t_str>& columns) const;
    t_schema operator+(const t_schema& o) const;
    std::vector<t_str> m_columns;
    std::vector<t_dtype> m_types;
    t_suidxmap m_colidx_map;
    t_sdtmap m_coldt_map;
    std::vector<t_bool> m_status_enabled;
    t_bool m_is_pkey;
    t_uindex m_pkeyidx;
    t_uindex m_opidx;
};

typedef std::vector<t_schema> t_schemavec;

} // end namespace perspective

namespace std
{

PERSPECTIVE_EXPORT std::ostream& operator<<(
    std::ostream& os, const perspective::t_schema& s);
}
