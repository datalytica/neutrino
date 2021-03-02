/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once
// Cannot include anything else in
// perspective apart from first.h
#include <perspective/first.h>
#include <perspective/exports.h>
#include <string>

namespace perspective
{
typedef char t_char;
typedef unsigned char t_uchar;
typedef std::int8_t t_int8;
typedef std::uint8_t t_uint8;
typedef std::int16_t t_int16;
typedef std::uint16_t t_uint16;
typedef std::int32_t t_int32;
typedef std::uint32_t t_uint32;
typedef long long t_int64;
typedef unsigned long long t_uint64;

typedef float t_float32;
typedef double t_float64;
typedef std::string t_str;

typedef bool t_bool;

enum t_status : t_uint8
{
    STATUS_INVALID,
    STATUS_VALID,
    STATUS_CLEAR
};

enum t_dtype
{
    DTYPE_NONE,
    DTYPE_INT64,
    DTYPE_INT32,
    DTYPE_INT16,
    DTYPE_INT8,
    DTYPE_UINT64,
    DTYPE_UINT32,
    DTYPE_UINT16,
    DTYPE_UINT8,
    DTYPE_FLOAT64,
    DTYPE_FLOAT32,
    DTYPE_BOOL,
    DTYPE_TIME,
    DTYPE_DATE,
    DTYPE_ENUM,
    DTYPE_OID,
    DTYPE_PTR,
    DTYPE_F64PAIR,
    DTYPE_USER_FIXED,
    DTYPE_STR,
    DTYPE_USER_VLEN,
    DTYPE_LAST_VLEN,
    DTYPE_LAST
};

#ifdef PSP_ENABLE_WASM
typedef t_uint32 t_uindex;
typedef t_int32 t_index;
#else
typedef t_uint64 t_uindex;
typedef t_int64 t_index;
#endif

#ifdef WIN32
typedef t_uint32 t_fflag;
typedef void* t_handle;
#else
typedef t_int32 t_fflag;
typedef t_int32 t_handle;
#endif

typedef t_index t_rcode;

// high 32 bits , low 32 bits
typedef std::pair<t_uint32, t_uint32> t_szpair;
typedef std::pair<t_str, t_str> t_sspair;

typedef std::pair<t_index, t_index> t_idxpair;
typedef std::pair<t_uindex, t_uindex> t_uidxpair;

typedef t_index t_ptidx;
typedef std::pair<t_ptidx, t_ptidx> t_ptipair;

typedef t_index t_tvidx;

typedef t_uint32 t_depth;

typedef std::pair<t_float64, t_float64> t_f64pair;

typedef t_uindex t_stridx;
typedef std::pair<t_stridx, t_stridx> t_stridxpair;

template <typename T>
struct t_accumulation_type
{
    using type = T;
};

template <>
struct t_accumulation_type<t_int32>
{
    using type = t_int64;
};

template <>
struct t_accumulation_type<t_int16>
{
    using type = t_int64;
};

template <>
struct t_accumulation_type<t_int8>
{
    using type = t_int64;
};

template <>
struct t_accumulation_type<t_uint64>
{
    using type = t_int64;
};
template <>
struct t_accumulation_type<t_uint32>
{
    using type = t_int64;
};
template <>
struct t_accumulation_type<t_uint16>
{
    using type = t_int64;
};
template <>
struct t_accumulation_type<t_uint8>
{
    using type = t_int64;
};

template <>
struct t_accumulation_type<t_float32>
{
    using type = t_float64;
};

template <>
struct t_accumulation_type<t_bool>
{
    using type = t_int64;
};

} // end namespace perspective
