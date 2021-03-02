/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once
#include <perspective/raw_types.h>

namespace perspective
{
inline t_uint32
lower32(t_uint64 v)
{
    return static_cast<t_uint32>(v);
}

inline t_uint32
upper32(t_uint64 v)
{
    return v >> 32;
}

t_str str_(int value, const t_str& fill, t_int32 width);

t_str str_(int value);

t_str unique_path(const t_str& path_prefix);

std::vector<t_str> split(const t_str& s, char delim);

} // namespace perspective
