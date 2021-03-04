add_executable(psp_bench bench.cpp)
target_link_libraries(psp_bench benchmark_main tbb tbbmalloc psp)
