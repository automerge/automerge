var path = require('path');
var webpack = require('webpack');
  
module.exports  = {
  entry: './pkg/index.js',
  mode: 'development',
  output: {
    filename: 'index.js',
    library: 'automerge-backend-wasm',
    libraryTarget: 'umd',
    publicPath: '',
    path: path.resolve(__dirname, 'dist'),
    globalObject: 'this'
  },
  experiments: {
    asyncWebAssembly: true
  },
  module: { }
}
