#!/bin/bash

set -e

echo ""
echo "Compiling wasm bindings"
echo ""

rm -rf build;
mkdir build;
cd build;
emcmake cmake ..

emmake make -j8
