
const TerserPlugin = require('terser-webpack-plugin');
const CopyPlugin = require("copy-webpack-plugin");

const {resolve} = require('path');

const build = 'release';

module.exports = {
  entry: {
    neutrino: './bindings/typescript/neutrino.ts'
  },
  output: {
    filename: '[name].js',
    path: resolve(__dirname, 'dist', 'js'),
    library: '[name]',
    libraryTarget: 'umd',
    publicPath: './dist/js/'
  },
  resolve: {
    alias: {
        '@internal/psp.async': resolve(__dirname, 'psp', build, 'install', 'psp.async.js')
    },
    extensions: ['.ts', '.js'],
  },
  plugins: [
    new CopyPlugin({
      patterns: [
        { from: resolve(__dirname, 'psp', build, 'install', 'psp.async.wasm'),},
      ],
    }),
  ],
  module: {
    rules: [
      {test: /\.tsx?$/, use: {loader: 'ts-loader'}},
    ]
  },
  optimization: {
    minimize: true,
    minimizer: [new TerserPlugin({
      parallel: true,
      //sourceMap: true,
      terserOptions: {
        output: {
          ascii_only: true,
          beautify: false,
       }
     }
    })
    ],
  },
  devtool: 'source-map'
};
