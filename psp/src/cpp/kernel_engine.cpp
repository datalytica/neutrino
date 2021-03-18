#include <perspective/kernel_engine.h>

namespace perspective
{
t_kernel_evaluator::t_kernel_evaluator() {}

t_kernel_evaluator*
get_evaluator()
{
    static t_kernel_evaluator* evaluator = new t_kernel_evaluator();
    return evaluator;
}

#ifdef PSP_ENABLE_WASM
template <typename T>
T
t_kernel_evaluator::reduce(
    const t_kernel& fn, t_uindex lvl_depth, const std::vector<T>& data)
{
    auto arr = em::val(em::typed_memory_view(data.size(), data.data()));
    em::val res = fn(arr, em::val(lvl_depth));
    if (res.isNull()) {
        return std::numeric_limits<T>::quiet_NaN();
    } else {
        return res.as<T>();
    }
}

#else
template <typename T>
T
t_kernel_evaluator::reduce(
    const t_kernel& fn, t_uindex lvl_depth, const std::vector<T>& data)
{
    PSP_COMPLAIN_AND_ABORT("Not implemented");
    return T();
}
#endif

template t_float64 t_kernel_evaluator::reduce(const t_kernel&, t_uindex, const std::vector<t_float64>&);

} // end namespace perspective
