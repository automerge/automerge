// @ts-check
import path from "path"
import HtmlWebpackPlugin from "html-webpack-plugin"

export default {
//module.exports = {
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
