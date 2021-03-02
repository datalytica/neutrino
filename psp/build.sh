#!/bin/bash

set -e

echo ""
echo "Compiling wasm bindings"
echo ""

rm -rf build;
mkdir build;
cd build;
emcmake cmake .. -DEMSCRIPTEN_GENERATE_BITCODE_STATIC_LIBRARIES=1

emmake make -j8
