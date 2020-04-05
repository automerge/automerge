var path = require('path');
  
module.exports  = {
  entry: './pkg/entry.js',
  mode: 'development',
  output: {
    filename: 'index.js',
    library: 'automerge-backend-wasm',
    libraryTarget: 'umd',
    path: path.resolve(__dirname, 'dist'),
    globalObject: 'this'
  },
  module: {
    rules: [
    ]
  }
}
