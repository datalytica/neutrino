/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#include <perspective/first.h>
#include <boost/uuid/uuid_io.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <perspective/raw_types.h>
#include <string>
#include <sstream>
#include <sstream>
#include <iomanip>

namespace perspective
{

t_str
unique_path(const t_str& path_prefix)
{
    std::stringstream ss;
    ss << path_prefix << boost::uuids::random_generator()();
    return ss.str();
}

t_str
str_(int value, const t_str& fill, t_int32 width)
{
    std::stringstream ss;
    ss << std::setfill('0') << std::setw(width) << value;
    return ss.str();
}

t_str
str_(int value)
{
    return str_(value, "0", 2);
}

std::vector<t_str>
split(const t_str& s, char delim)
{
    std::vector<t_str> elems;
    std::stringstream ss;
    ss.str(s);
    t_str item;
    while (std::getline(ss, item, delim))
    {
        if (!item.empty())
            elems.push_back(item);
    }
    return elems;
}
} // namespace perspective
