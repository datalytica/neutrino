
const {resolve} = require('path');
let webpack = require('webpack');

const build = 'release';

module.exports = {
  entry: {
    neutrino: './bindings/typescript/neutrino.ts'
  },
  output: {
    filename: '[name].js',
    path: resolve(__dirname, 'dist/js'),
    library: '[name]',
    libraryTarget: 'umd',
    publicPath: './dist/js/'
  },
  resolve: {
    alias: {
        '@internal/psp.async': resolve(__dirname, 'psp/build/install/psp.async.js')
    },
    extensions: ['.ts', '.js'],
  },
  plugins: [],
  module: {
    rules: [
      {test: /\.tsx?$/, use: {loader: 'ts-loader'}},
    ]
  },
  //devtool: 'inline-source-map'
};
