import { build } from "esbuild"
import { fileURLToPath } from "url"
import { dirname } from "path"

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

const projectDir = `${__dirname}/..`
const distDir = `${projectDir}/dist`
const inDir = `${distDir}/mjs`
const outDir = `${distDir}/cjs`

await build({
  absWorkingDir: distDir,
  entryPoints: [`${inDir}/*.js`],
  outdir: outDir,
  bundle: true,
  packages: "external",
  format: "cjs",
  target: "node14",
  platform: "node",
  outExtension: { ".js": ".cjs" },
})
