/******************************************************************************
 *
 * Copyright (c) 2017, the Perspective Authors.
 *
 * This file is part of the Perspective library, distributed under the terms of
 * the Apache License 2.0.  The full license can be found in the LICENSE file.
 *
 */
#define VERSION 2
#ifndef PSP_ENABLE_WASM
//#define PSP_PARALLEL_FOR
#endif

#pragma once
#ifdef WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif // ifndex nominmax
#endif // win32

// Remove once we are c++11 everywhere
#define TBB_DEPRECATED_MUTEX_COPYING 1
#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define _GLIBCXX_USE_NANOSLEEP 1
