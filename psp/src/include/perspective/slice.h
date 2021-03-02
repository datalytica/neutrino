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
#include <perspective/scalar.h>
#include <perspective/range.h>
#include <perspective/config.h>
#include <perspective/exports.h>
#include <perspective/data.h>
#include <perspective/path.h>

namespace perspective
{

class PERSPECTIVE_EXPORT t_slice
{
public:
    const t_range& range() const;
    const t_pathvec& row_paths() const;
    const t_pathvec& column_paths() const;
    const std::vector<t_index>& row_indices() const;
    const std::vector<t_index>& column_indices() const;
    const t_datavec& row_data() const;
    const t_datavec& column_data() const;
    const std::vector<t_uindex>& row_depth() const;
    const std::vector<t_uindex>& column_depth() const;
    const t_config_recipe& config_recipe() const;
    const std::vector<t_uindex>& is_row_expanded() const;
    const std::vector<t_uindex>& is_column_expanded() const;

    t_range& range();
    t_pathvec& row_paths();
    t_pathvec& column_paths();
    std::vector<t_index>& row_indices();
    std::vector<t_index>& column_indices();
    t_datavec& row_data();
    t_datavec& column_data();
    std::vector<t_uindex>& row_depth();
    std::vector<t_uindex>& column_depth();
    std::vector<t_uindex>& is_row_expanded();
    std::vector<t_uindex>& is_column_expanded();

private:
    t_range m_range;
    t_pathvec m_row_paths;
    t_pathvec m_column_paths;
    std::vector<t_index> m_row_indices;
    std::vector<t_index> m_column_indices;
    t_datavec m_row_data;
    t_datavec m_column_data;
    std::vector<t_uindex> m_is_root;
    std::vector<t_uindex> m_is_row_expanded;
    std::vector<t_uindex> m_is_column_expanded;
    t_config_recipe m_config_recipe;
    std::vector<t_uindex> m_row_depth;
    std::vector<t_uindex> m_column_depth;
};

} // end namespace perspective