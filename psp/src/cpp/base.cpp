/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#include <perspective/first.h>
#include <perspective/base.h>
#include <cstdint>
#include <limits>
#include <cstring>
#include <boost/functional/hash.hpp>
#include <csignal>
#include <iostream>

namespace perspective
{

void
psp_log(const char* file, t_uint64 line_no, const char* msg)
{
    std::stringstream ss;
    std::cout << file << ":" << line_no << ": " << msg << " : "
              << perspective::get_error_str();
}

void
psp_abort()
{
    std::raise(SIGINT);
}

bool
is_numeric_type(t_dtype dtype)
{
    switch (dtype)
    {
        case DTYPE_UINT8:
        case DTYPE_UINT16:
        case DTYPE_UINT32:
        case DTYPE_UINT64:
        case DTYPE_INT8:
        case DTYPE_INT16:
        case DTYPE_INT32:
        case DTYPE_INT64:
        case DTYPE_FLOAT32:
        case DTYPE_FLOAT64:
        {
            return true;
        }
        break;
        default:
        {
            return false;
        }
    }
}

bool
is_floating_point(t_dtype dtype)
{
    return (dtype == DTYPE_FLOAT32 || dtype == DTYPE_FLOAT64);
}

t_bool
is_deterministic_sized(t_dtype dtype)
{
    switch (dtype)
    {
        case DTYPE_PTR:
        case DTYPE_INT64:
        case DTYPE_UINT64:
        case DTYPE_INT32:
        case DTYPE_UINT32:
        case DTYPE_INT16:
        case DTYPE_UINT16:
        case DTYPE_BOOL:
        case DTYPE_INT8:
        case DTYPE_UINT8:
        case DTYPE_FLOAT64:
        case DTYPE_FLOAT32:
        case DTYPE_STR:
        case DTYPE_TIME:
        case DTYPE_DATE:
        case DTYPE_F64PAIR:
        {
            return true;
        }
        default:
        {
            return false;
        }
    }

    PSP_COMPLAIN_AND_ABORT("Reached unreachable");
    return false;
}

t_uindex
get_dtype_size(t_dtype dtype)
{
    switch (dtype)
    {
        case DTYPE_PTR:
        {
            return sizeof(void*);
        }
        case DTYPE_INT64:
        case DTYPE_UINT64:
        {
            return sizeof(t_int64);
        }
        case DTYPE_INT32:
        case DTYPE_UINT32:
        {
            return sizeof(t_int32);
        }
        case DTYPE_INT16:
        case DTYPE_UINT16:
        {
            return 2;
        }
        case DTYPE_BOOL:
        case DTYPE_INT8:
        case DTYPE_UINT8:
        case DTYPE_NONE:
        {
            return 1;
        }
        case DTYPE_FLOAT64:
        {
            return sizeof(t_float64);
        }
        case DTYPE_FLOAT32:
        {
            return sizeof(t_float32);
        }
        case DTYPE_STR:
        {
            return sizeof(t_stridx);
        }
        case DTYPE_TIME:
        {
            return sizeof(t_int64);
        }
        case DTYPE_DATE:
        {
            return sizeof(t_uint32);
        }
        case DTYPE_F64PAIR:
        {
            return sizeof(t_f64pair);
        }
        default:
        {
            PSP_COMPLAIN_AND_ABORT("Unknown dtype");
        }
    }

    PSP_COMPLAIN_AND_ABORT("Reached unreachable");
    return sizeof(DTYPE_INT64);
}

t_bool
is_vlen_dtype(t_dtype dtype)
{
    if (dtype == DTYPE_STR)
        return true;
    return false;
}

t_str
get_dtype_descr(t_dtype dtype)
{
    switch (dtype)
    {
        case DTYPE_NONE:
        {
            return "none";
        }
        break;
        case DTYPE_INT64:
        {
            return "i64";
        }
        break;
        case DTYPE_INT32:
        {
            return "i32";
        }
        break;
        case DTYPE_INT16:
        {
            return "i16";
        }
        break;
        case DTYPE_INT8:
        {
            return "i8";
        }
        break;
        case DTYPE_UINT64:
        {
            return "u64";
        }
        break;
        case DTYPE_UINT32:
        {
            return "u32";
        }
        break;
        case DTYPE_UINT16:
        {
            return "u16";
        }
        break;
        case DTYPE_UINT8:
        {
            return "u8";
        }
        break;
        case DTYPE_BOOL:
        {
            return "bool";
        }
        break;
        case DTYPE_FLOAT64:
        {
            return "f64";
        }
        break;
        case DTYPE_FLOAT32:
        {
            return "f32";
        }
        break;
        case DTYPE_STR:
        {
            return "str";
        }
        break;
        case DTYPE_TIME:
        {
            return "time";
        }
        break;
        case DTYPE_DATE:
        {
            return "date";
        }
        break;
        case DTYPE_USER_FIXED:
        {
            return "ufix";
        }
        break;
        case DTYPE_LAST:
        {
            return "last";
        }
        break;
        case DTYPE_F64PAIR:
        {
            return "f64pair";
        }
        break;
        default:
        {
            PSP_COMPLAIN_AND_ABORT("Encountered unknown dtype");
        }
    }

    return t_str("dummy");
}

t_str
filter_op_to_str(t_filter_op op)
{
    switch (op)
    {
        case FILTER_OP_LT:
        {
            return "<";
        }
        break;
        case FILTER_OP_LTEQ:
        {
            return "<=";
        }
        break;
        case FILTER_OP_GT:
        {
            return ">";
        }
        break;
        case FILTER_OP_GTEQ:
        {
            return ">=";
        }
        break;
        case FILTER_OP_EQ:
        {
            return "==";
        }
        break;
        case FILTER_OP_NE:
        {
            return "!=";
        }
        break;
        case FILTER_OP_BEGINS_WITH:
        {
            return "startswith";
        }
        break;
        case FILTER_OP_ENDS_WITH:
        {
            return "endswith";
        }
        break;
        case FILTER_OP_CONTAINS:
        {
            return "in";
        }
        break;
        case FILTER_OP_OR:
        {
            return "or";
        }
        break;
        case FILTER_OP_IN:
        {
            return "in";
        }
        break;
        case FILTER_OP_NOT_IN:
        {
            return "not in";
        }
        break;
        case FILTER_OP_AND:
        {
            return "and";
        }
        break;
        case FILTER_OP_IS_NAN:
        {
            return "is_nan";
        }
        break;
        case FILTER_OP_IS_NOT_NAN:
        {
            return "!is_nan";
        }
        break;
        case FILTER_OP_IS_VALID:
        {
            return "is not None";
        }
        break;
        case FILTER_OP_IS_NOT_VALID:
        {
            return "is None";
        }
        break;
    }
    PSP_COMPLAIN_AND_ABORT("Reached end of function");
    return "";
}

t_str
get_status_descr(t_status status)
{
    switch (status)
    {
        case STATUS_INVALID:
        {
            return "i";
        }
        case STATUS_VALID:
        {
            return "v";
        }
        case STATUS_CLEAR:
        {
            return "c";
        }
        default:
        {
            PSP_COMPLAIN_AND_ABORT("Unexpected status found");
        }
    }
    return "";
}

t_uindex
root_pidx()
{
    return std::numeric_limits<t_uindex>::max();
}

bool
t_cmp_charptr::operator()(const char* a, const char* b) const
{
    return std::strcmp(a, b) < 0;
}

bool
t_cchar_umap_cmp::operator()(const char* x, const char* y) const
{
    return strcmp(x, y) == 0;
}

t_uindex
t_cchar_umap_hash::operator()(const char* s) const
{
    return boost::hash_range(s, s + std::strlen(s));
}

template <>
t_dtype
type_to_dtype<t_int64>()
{
    return DTYPE_INT64;
}

template <>
t_dtype
type_to_dtype<t_int32>()
{
    return DTYPE_INT32;
}

template <>
t_dtype
type_to_dtype<t_int16>()
{
    return DTYPE_INT16;
}

template <>
t_dtype
type_to_dtype<t_int8>()
{
    return DTYPE_INT8;
}

template <>
t_dtype
type_to_dtype<t_uint64>()
{
    return DTYPE_UINT64;
}

template <>
t_dtype
type_to_dtype<t_uint32>()
{
    return DTYPE_UINT32;
}

template <>
t_dtype
type_to_dtype<t_uint16>()
{
    return DTYPE_UINT16;
}

template <>
t_dtype
type_to_dtype<t_uint8>()
{
    return DTYPE_UINT8;
}

template <>
t_dtype
type_to_dtype<t_float64>()
{
    return DTYPE_FLOAT64;
}

template <>
t_dtype
type_to_dtype<t_float32>()
{
    return DTYPE_FLOAT32;
}

template <>
t_dtype
type_to_dtype<t_bool>()
{
    return DTYPE_BOOL;
}

class t_date;
class t_time;

template <>
t_dtype
type_to_dtype<t_time>()
{
    return DTYPE_TIME;
}

template <>
t_dtype
type_to_dtype<t_date>()
{
    return DTYPE_DATE;
}

template <>
t_dtype
type_to_dtype<t_str>()
{
    return DTYPE_STR;
}
} // end namespace perspective
