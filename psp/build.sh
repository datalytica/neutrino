#!/bin/bash

set -e

echo ""
echo "Compiling wasm bindings"
echo ""

rm -rf release;
mkdir release;
cd release;
emcmake cmake -DCMAKE_BUILD_TYPE=Release ..
emmake make -j8

cd ..
rm -rf debug
mkdir debug
cd debug
emcmake cmake -DCMAKE_BUILD_TYPE=Debug ..
emmake make -j8

