#pragma once
#include <perspective/first.h>
#include <perspective/base.h>
#include <perspective/raw_types.h>

#ifdef PSP_ENABLE_WASM
#include <emscripten.h>
#include <emscripten/val.h>
typedef emscripten::val t_kernel;
namespace em = emscripten;
#else
typedef perspective::t_str t_kernel;
#endif

namespace perspective
{

class t_kernel_evaluator
{
public:
    t_kernel_evaluator();
    template <typename T>
    T reduce(const t_kernel& fn, t_uindex lvl_depth, const std::vector<T>& data);

private:
    std::vector<t_uint8> m_kernels;
};

t_kernel_evaluator* get_evaluator();

} // namespace perspective