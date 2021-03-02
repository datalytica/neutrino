cmake_minimum_required (VERSION 3.0)
project (psp)

if (NOT DEFINED PSP_WASM_BUILD)
    set(PSP_WASM_BUILD ON)
endif()

function (psp_build_dep name cmake_file)
    message(STATUS "psp_build_dep: ${name} ${cmake_file}")
    configure_file(${cmake_file} ${name}-download/CMakeLists.txt)
    execute_process(COMMAND ${CMAKE_COMMAND} -G "${CMAKE_GENERATOR}" .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/${name}-download )
    if(result)
        message(FATAL_ERROR "CMake step for ${name} failed: ${result}")
    endif()
    execute_process(COMMAND ${CMAKE_COMMAND} --build .
            RESULT_VARIABLE result
            WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/${name}-download )
    if(result)
        message(FATAL_ERROR "Build step for ${name} failed: ${result}")
    endif()
    if (name STREQUAL "boost")
        if (DEFINED PSP_BOOST_INCLUDE AND NOT "$ENV{Compiler}" STREQUAL "emscripten")
            include_directories(${PSP_BOOST_INCLUDE})
        else()
            set(BOOST_BASE ${CMAKE_BINARY_DIR}/${name}-src/libs)
            include_directories(
                ${BOOST_BASE}/accumulators/include
                ${BOOST_BASE}/algorithm/include
                ${BOOST_BASE}/align/include
                ${BOOST_BASE}/any/include
                ${BOOST_BASE}/array/include
                ${BOOST_BASE}/asio/include
                ${BOOST_BASE}/assert/include
                ${BOOST_BASE}/assign/include
                ${BOOST_BASE}/atomic/include
                ${BOOST_BASE}/beast/include
                ${BOOST_BASE}/bimap/include
                ${BOOST_BASE}/bind/include
                ${BOOST_BASE}/callable_traits/include
                ${BOOST_BASE}/chrono/include
                ${BOOST_BASE}/circular_buffer/include
                ${BOOST_BASE}/compatibility/include
                ${BOOST_BASE}/compute/include
                ${BOOST_BASE}/concept_check/include
                ${BOOST_BASE}/config/include
                ${BOOST_BASE}/container/include
                ${BOOST_BASE}/container_hash/include
                ${BOOST_BASE}/context/include
                ${BOOST_BASE}/contract/include
                ${BOOST_BASE}/conversion/include
                ${BOOST_BASE}/convert/include
                ${BOOST_BASE}/core/include
                ${BOOST_BASE}/coroutine/include
                ${BOOST_BASE}/coroutine2/include
                ${BOOST_BASE}/crc/include
                ${BOOST_BASE}/date_time/include
                ${BOOST_BASE}/detail/include
                ${BOOST_BASE}/disjoint_sets/include
                ${BOOST_BASE}/dll/include
                ${BOOST_BASE}/dynamic_bitset/include
                ${BOOST_BASE}/endian/include
                ${BOOST_BASE}/exception/include
                ${BOOST_BASE}/fiber/include
                ${BOOST_BASE}/filesystem/include
                ${BOOST_BASE}/flyweight/include
                ${BOOST_BASE}/foreach/include
                ${BOOST_BASE}/format/include
                ${BOOST_BASE}/function/include
                ${BOOST_BASE}/functional/include
                ${BOOST_BASE}/function_types/include
                ${BOOST_BASE}/fusion/include
                ${BOOST_BASE}/geometry/include
                ${BOOST_BASE}/gil/include
                ${BOOST_BASE}/graph/include
                ${BOOST_BASE}/graph_parallel/include
                ${BOOST_BASE}/hana/include
                ${BOOST_BASE}/heap/include
                ${BOOST_BASE}/hof/include
                ${BOOST_BASE}/icl/include
                ${BOOST_BASE}/integer/include
                ${BOOST_BASE}/interprocess/include
                ${BOOST_BASE}/intrusive/include
                ${BOOST_BASE}/io/include
                ${BOOST_BASE}/iostreams/include
                ${BOOST_BASE}/iterator/include
                ${BOOST_BASE}/lambda/include
                ${BOOST_BASE}/lexical_cast/include
                ${BOOST_BASE}/locale/include
                ${BOOST_BASE}/local_function/include
                ${BOOST_BASE}/lockfree/include
                ${BOOST_BASE}/log/include
                ${BOOST_BASE}/logic/include
                ${BOOST_BASE}/math/include
                ${BOOST_BASE}/metaparse/include
                ${BOOST_BASE}/move/include
                ${BOOST_BASE}/mp11/include
                ${BOOST_BASE}/mpi/include
                ${BOOST_BASE}/mpl/include
                ${BOOST_BASE}/msm/include
                ${BOOST_BASE}/multi_array/include
                ${BOOST_BASE}/multi_index/include
                ${BOOST_BASE}/multiprecision/include
                ${BOOST_BASE}/numeric/include
                ${BOOST_BASE}/optional/include
                ${BOOST_BASE}/parameter/include
                ${BOOST_BASE}/parameter_python/include
                ${BOOST_BASE}/phoenix/include
                ${BOOST_BASE}/poly_collection/include
                ${BOOST_BASE}/polygon/include
                ${BOOST_BASE}/pool/include
                ${BOOST_BASE}/predef/include
                ${BOOST_BASE}/preprocessor/include
                ${BOOST_BASE}/process/include
                ${BOOST_BASE}/program_options/include
                ${BOOST_BASE}/property_map/include
                ${BOOST_BASE}/property_tree/include
                ${BOOST_BASE}/proto/include
                ${BOOST_BASE}/ptr_container/include
                ${BOOST_BASE}/python/include
                ${BOOST_BASE}/qvm/include
                ${BOOST_BASE}/random/include
                ${BOOST_BASE}/range/include
                ${BOOST_BASE}/ratio/include
                ${BOOST_BASE}/rational/include
                ${BOOST_BASE}/regex/include
                ${BOOST_BASE}/safe_numerics/include
                ${BOOST_BASE}/scope_exit/include
                ${BOOST_BASE}/serialization/include
                ${BOOST_BASE}/signals2/include
                ${BOOST_BASE}/smart_ptr/include
                ${BOOST_BASE}/sort/include
                ${BOOST_BASE}/spirit/include
                ${BOOST_BASE}/stacktrace/include
                ${BOOST_BASE}/statechart/include
                ${BOOST_BASE}/static_assert/include
                ${BOOST_BASE}/system/include
                ${BOOST_BASE}/test/include
                ${BOOST_BASE}/thread/include
                ${BOOST_BASE}/throw_exception/include
                ${BOOST_BASE}/timer/include
                ${BOOST_BASE}/tokenizer/include
                ${BOOST_BASE}/tti/include
                ${BOOST_BASE}/tuple/include
                ${BOOST_BASE}/type_erasure/include
                ${BOOST_BASE}/type_index/include
                ${BOOST_BASE}/typeof/include
                ${BOOST_BASE}/type_traits/include
                ${BOOST_BASE}/units/include
                ${BOOST_BASE}/unordered/include
                ${BOOST_BASE}/utility/include
                ${BOOST_BASE}/uuid/include
                ${BOOST_BASE}/variant/include
                ${BOOST_BASE}/vmd/include
                ${BOOST_BASE}/wave/include
                ${BOOST_BASE}/winapi/include
                ${BOOST_BASE}/xpressive/include
                ${BOOST_BASE}/yap/include
            )
        endif()
    else()
        add_subdirectory(${CMAKE_BINARY_DIR}/${name}-src
            ${CMAKE_BINARY_DIR}/${name}-build
            EXCLUDE_FROM_ALL)
    endif()
    include_directories(${CMAKE_BINARY_DIR}/${name}-src/extras/gtest/include)
    include_directories(${CMAKE_BINARY_DIR}/${name}-src/include)
    include_directories(${CMAKE_BINARY_DIR}/${name}-src)
endfunction()


set(CMAKE_CXX_STANDARD 14)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
include_directories("${CMAKE_SOURCE_DIR}/src/include")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/install)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/install)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/install)
set (CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")


if (PSP_WASM_BUILD)
    add_definitions(-DPSP_ENABLE_WASM)
    message(STATUS "Using CC: $ENV{EMSCRIPTEN}/emcc")
    message(STATUS "Using CXX: $ENV{EMSCRIPTEN}/em++")
    set(CMAKE_C_COMPILER "$ENV{EMSCRIPTEN}/emcc")
    set(CMAKE_CXX_COMPILER "$ENV{EMSCRIPTEN}/em++")
    set(CMAKE_TOOLCHAIN_FILE "$ENV{EMSCRIPTEN}/cmake/Modules/Platform/Emscripten.cmake")
    set(CMAKE_AR $ENV{EMSCRIPTEN}/emar)
    set(CMAKE_RANLIB $ENV{EMSCRIPTEN}/emranlib)
    set(CMAKE_EXECUTABLE_SUFFIX ".js")

    set(CMAKE_CXX_FLAGS " \
        --bind \
        --memory-init-file 0 \
                -Os \
                -g0 \
                -flto \
                -s STRICT=1 \
                -s MODULARIZE=1 \
                -s EXPORT_ES6=1 \
                -s USE_ES6_IMPORT_META=0 \
                -s EXPORT_NAME=loadModule \
        -s NO_EXIT_RUNTIME=1 \
        -s NO_FILESYSTEM=1 \
        -s ALLOW_MEMORY_GROWTH=1 \
        -s DYNAMIC_EXECUTION=0 \
        -s ASSERTIONS=2 \
        -s EXPORTED_FUNCTIONS=\"['_main']\" \
                --pre-js ../src/prejs \
    ")

        set(CMAKE_CXX_FLAGS_DEBUG " \
            -O0 \
            -g4 \
            -s SAFE_HEAP=1 \
            -s DISABLE_EXCEPTION_CATCHING=0 \
            -s ASSERTIONS=2 \
            -s DEMANGLE_SUPPORT=1 \
        ")

    set(ASYNC_MODE_FLAGS "-s ENVIRONMENT=worker -s WASM=1")
endif()

set(CMAKE_CXX_OUTPUT_EXTENSION_REPLACE ON)

set (SOURCE_FILES
src/cpp/aggregate.cpp
src/cpp/aggspec.cpp
src/cpp/arg_sort.cpp
src/cpp/base.cpp
src/cpp/base_impl_linux.cpp
src/cpp/base_impl_osx.cpp
src/cpp/base_impl_win.cpp
src/cpp/build_filter.cpp
#src/cpp/calc_agg_dtype.cpp
src/cpp/column.cpp
src/cpp/comparators.cpp
src/cpp/compat.cpp
src/cpp/compat_impl_linux.cpp
src/cpp/compat_impl_osx.cpp
src/cpp/compat_impl_win.cpp
src/cpp/config.cpp
src/cpp/context_base.cpp
src/cpp/context_grouped_pkey.cpp
src/cpp/context_handle.cpp
src/cpp/context_one.cpp
src/cpp/context_two.cpp
src/cpp/context_zero.cpp
src/cpp/custom_column.cpp
src/cpp/data.cpp
src/cpp/date.cpp
src/cpp/dense_nodes.cpp
src/cpp/dense_tree_context.cpp
src/cpp/dense_tree.cpp
src/cpp/dependency.cpp
src/cpp/extract_aggregate.cpp
src/cpp/filter.cpp
src/cpp/flat_traversal.cpp
src/cpp/gnode.cpp
src/cpp/gnode_state.cpp
src/cpp/histogram.cpp
src/cpp/kernel_engine.cpp
src/cpp/logtime.cpp
src/cpp/mask.cpp
src/cpp/min_max.cpp
src/cpp/multi_sort.cpp
src/cpp/none.cpp
src/cpp/path.cpp
src/cpp/pivot.cpp
src/cpp/pool.cpp
src/cpp/port.cpp
src/cpp/raii.cpp
src/cpp/raii_impl_linux.cpp
src/cpp/raii_impl_osx.cpp
src/cpp/raii_impl_win.cpp
src/cpp/range.cpp
src/cpp/rlookup.cpp
src/cpp/scalar.cpp
src/cpp/schema_column.cpp
src/cpp/schema.cpp
src/cpp/slice.cpp
src/cpp/sort_specification.cpp
src/cpp/sparse_tree.cpp
src/cpp/sparse_tree_node.cpp
src/cpp/step_delta.cpp
src/cpp/storage.cpp
src/cpp/storage_impl_linux.cpp
src/cpp/storage_impl_osx.cpp
src/cpp/storage_impl_win.cpp
src/cpp/sym_table.cpp
src/cpp/table.cpp
src/cpp/time.cpp
src/cpp/traversal.cpp
src/cpp/traversal_nodes.cpp
src/cpp/tree_context_common.cpp
src/cpp/utils.cpp
src/cpp/update_task.cpp
src/cpp/vocab.cpp
)

file(GLOB HEADER_FILES src/include/perspective/*.h)
if (PSP_WASM_BUILD)
    add_library(psp STATIC ${SOURCE_FILES} ${HEADER_FILES})
else()
    add_library(psp SHARED ${SOURCE_FILES} ${HEADER_FILES})
endif()

if (NOT PSP_WASM_BUILD)
    if (UNIX)
        target_compile_options(psp PRIVATE -Wall -Werror)
        target_compile_options(psp PRIVATE $<$<CONFIG:DEBUG>:-fPIC -O0>)
    endif()
    target_compile_definitions(psp PRIVATE $<$<CONFIG:DEBUG>:D_GLIBCXX_DEBUG PSP_DEBUG>)
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
        target_compile_options(psp PRIVATE $<$<CONFIG:DEBUG>:-fprofile-instr-generate -fcoverage-mapping>)
        target_link_libraries(psp PRIVATE $<$<CONFIG:DEBUG>:--coverage>)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
        target_compile_options(psp PRIVATE -time=build.time -Wno-deprecated-declarations -Wno-deprecated)
        target_compile_options(psp PRIVATE $<$<CONFIG:DEBUG>:-fprofile-arcs -ftest-coverage>)
        target_link_libraries(psp PRIVATE $<$<CONFIG:DEBUG>:--coverage>)
    elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "MSVC")
    endif()

    add_custom_target(gcov
           COMMAND mkdir -p ${CMAKE_BINARY_DIR}/coverage
           WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
           )
    add_custom_command(TARGET gcov
           COMMAND echo "=================== GCOV ===================="
           COMMAND gcovr -r ../ --html --html-details -o coverage/index.html
           COMMAND xdg-open coverage/index.html
           WORKING_DIRECTORY ${CMAKE_BINARY_DIR}
           )
endif()


if (WIN32)
    target_compile_definitions(psp PUBLIC -DPERSPECTIVE_EXPORTS)
    foreach(warning 4244 4251 4267 4275 4290 4786 4305 4996)
        SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /wd${warning}")
    endforeach(warning)
endif()

set(PSP_DIST_DIR ${CMAKE_SOURCE_DIR}/dist)

if (PSP_WASM_BUILD)
    add_executable(perspective.async src/cpp/main.cpp)
    target_link_libraries(perspective.async psp "${ASYNC_MODE_FLAGS}")
    set_target_properties(perspective.async PROPERTIES COMPILE_FLAGS "${ASYNC_MODE_FLAGS}")
    set_target_properties(perspective.async PROPERTIES OUTPUT_NAME "psp.async")
endif()


message(STATUS "PSP_BOOST_INCLUDE: ${PSP_BOOST_INCLUDE} Compiler: $ENV{Compiler}")
if (DEFINED PSP_BOOST_INCLUDE AND NOT "$ENV{Compiler}" STREQUAL "emscripten")
    include_directories(${PSP_BOOST_INCLUDE})
else()
#    psp_build_dep("boost" "cmake/boost.txt.in")
endif()

if (NOT PSP_WASM_BUILD)
   if (WIN32)
       set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
   endif()
   set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
   psp_build_dep("googletest" "cmake/GTest.txt.in")
   #psp_build_dep("tbb" "cmake/TBB.txt.in")
   #psp_build_dep("benchmark" "cmake/benchmark.txt.in")
   add_subdirectory(test)
   #add_subdirectory(bench)
endif()

