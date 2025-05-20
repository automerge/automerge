// @ts-check
const path = require('path');
const HtmlWebpackPlugin = require('html-webpack-plugin');

module.exports = {
  mode: 'development',
  entry: {
    index: './index.js',
  },
  plugins: [
    new HtmlWebpackPlugin({
      title: 'Output Management',
    }),
  ],
  devServer: {
    static: "./dist"
  },
  output: {
    filename: '[name].bundle.js',
    path: path.resolve('./dist'),
  },
  experiments: {
    asyncWebAssembly: true,
  },
}
