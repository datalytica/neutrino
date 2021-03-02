/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */

#pragma once
#include <perspective/scalar.h>
#include <perspective/base.h>

namespace perspective
{

class t_column;
class t_aggspec;

t_tscalar extract_aggregate(const t_aggspec& aggspec, const t_column* aggcol,
    t_uindex ridx, t_index pridx);
} // end namespace perspective
