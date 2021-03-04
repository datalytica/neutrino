#include <benchmark/benchmark.h>
#include <perspective/table.h>
#include <perspective/config.h>
#include <perspective/test_utils.h>
#include <perspective/context_one.h>
#include <perspective/node_processor.h>
#include <perspective/storage.h>

using namespace perspective;

class PBench : public benchmark::Fixture
{
};

static void
Table_Simple(benchmark::State& state)
{
    for (auto _ : state)
    {
        t_table tbl(
            t_schema({"a", "b"}, {DTYPE_INT64, DTYPE_FLOAT64}), state.range(0));
        tbl.init();
    }
}
BENCHMARK(Table_Simple)->Range(8, 8 << 16);

BENCHMARK_F(PBench, DoPivot1)(benchmark::State& st)
{
    for (auto _ : st)
    {
        t_schema sch{{"b", "s", "i"}, {DTYPE_BOOL, DTYPE_STR, DTYPE_INT64}};
        auto t = mktscalar<t_bool>(true);
        auto f = mktscalar<t_bool>(false);
        t_table tbl(sch,
            {{t, "a"_ts, 1_ts}, {f, "b"_ts, 2_ts}, {t, "c"_ts, 3_ts},
                {f, "d"_ts, 4_ts}});
        t_config cfg{{"b", "s"}, {"sum_i", AGGTYPE_SUM, "i"}};
        auto ctx = do_pivot<t_ctx1, t_int32, DTYPE_INT32>(
            t_do_pivot::PIVOT_NON_PKEYED, tbl, cfg);
        auto ctx_tbl = ctx->get_table();
        auto got = ctx_tbl->get_scalvec();
    }
}

BENCHMARK_F(PBench, DoPivot2)(benchmark::State& st)
{

    t_schema sch{{"b", "s", "i"}, {DTYPE_BOOL, DTYPE_STR, DTYPE_INT64}};
    auto t = mktscalar<t_bool>(true);
    auto f = mktscalar<t_bool>(false);
    t_table tbl(sch,
        {{t, "a"_ts, 1_ts}, {f, "b"_ts, 2_ts}, {t, "c"_ts, 3_ts},
            {f, "d"_ts, 4_ts}});
    t_config cfg{{"b", "s"}, {"sum_i", AGGTYPE_SUM, "i"}};
    for (auto _ : st)
    {
        auto ctx = do_pivot<t_ctx1, t_int32, DTYPE_INT32>(
            t_do_pivot::PIVOT_NON_PKEYED, tbl, cfg);
    }
}

BENCHMARK_F(PBench, Scalar1)(benchmark::State& st)
{
    for (auto _ : st)
    {
        auto v = mktscalar<t_int64>(0);
    }
}

BENCHMARK_F(PBench, Scalar2)(benchmark::State& st)
{
    for (auto _ : st)
    {
        auto v = mktscalar<const char*>("abc");
    }
}

BENCHMARK_F(PBench, Scalar3)(benchmark::State& st)
{
    for (auto _ : st)
    {
        auto v = mktscalar<const char*>("abcdefghijklmnopqrstuvwxyz");
    }
}
