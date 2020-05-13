var path = require('path');
var webpack = require('webpack');
  
module.exports  = {
  entry: './build/entry.js',
  mode: 'development',
  output: {
    filename: 'index.js',
    library: 'automerge-backend-wasm',
    libraryTarget: 'umd',
    path: path.resolve(__dirname, 'dist'),
    globalObject: 'this'
  },
  plugins: [
         new webpack.optimize.LimitChunkCountPlugin({
            maxChunks: 1,
          })
  ],
  module: { }
}
