// @ts-check
import path from "path"
import fs from "fs"
import { fileURLToPath } from "url"
import { execSync } from "child_process"
import { build } from "esbuild"
import { parseArgs } from "util"
import os from "os"

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const jsProjectDir = path.dirname(__dirname)
const rustProjectDir = path.join(path.dirname(jsProjectDir), "rust")

/** @typedef {"all" | "build-wasm" | "build-wasm-tarball" | "compile-typescript" | "transpile-cjs"} Step */

const args = parseArgs({
  options: {
    step: {
      type: "string",
      short: "s",
    },
    wasmBuildTarball: {
      type: "string",
      short: "w",
    },
  },
})

/**
 * @returns {{step: Step, wasmBuildTarball: string | null}} step
 */
function parseCli() {
  /** @type {Step} */
  let step = "all"
  if (args.values.step) {
    if (isStep(args.values.step)) {
      step = args.values.step
    } else {
      console.error(`Unknown step ${args.values.step}`)
      process.exit(1)
    }
  }
  if (args.values.wasmBuildTarball) {
    return { step, wasmBuildTarball: args.values.wasmBuildTarball }
  } else if (process.env.WASM_BUILD_LOCATION) {
    return { step, wasmBuildTarball: process.env.WASM_BUILD_TARBALL || null }
  }
  return { step, wasmBuildTarball: null }
}

/**
 * @type {(stepStr: string) => stepStr is Step}
 */
function isStep(stepStr) {
  return [
    "all",
    "build-wasm",
    "build-wasm-tarball",
    "compile-typescript",
    "transpile-cjs",
  ].includes(stepStr)
}

/**
 * @param {string} outputDir
 */
function buildWasm(outputDir) {
  const automergeWasmPath = path.join(rustProjectDir, "automerge-wasm")
  console.log("building automerge-wasm")
  execSync(
    //"cargo build --target wasm32-unknown-unknown --profile dev",
    "cargo build --target wasm32-unknown-unknown --release",
    {
      cwd: automergeWasmPath,
    },
  )

  const wasmBlobPath = path.join(
    rustProjectDir,
    "target",
    "wasm32-unknown-unknown",
    "release",
    //"debug",
    "automerge_wasm.wasm",
  )

  /**
   * Run `wasm-bindgen` for the given target and copy the resulting files to the output directory
   *
   * E.g runWasmBindgen("bundler", "/path/to/output") will run `wasm-bindgen`
   * for the bundler target and copy the resulting files to
   * "src/wasm_bindgen_output/bundler/"
   *
   * @param {string} target
   */
  function runWasmBindgen(target) {
    console.log(`running wasm-bindgen for '${target}' target`)
    const outputPath = path.join(outputDir, target)
    fs.mkdirSync(outputPath, { recursive: true })
    execSync(
      `wasm-bindgen ${wasmBlobPath} --out-dir ${outputPath} --target ${target} --no-typescript --weak-refs`,
      {
        cwd: __dirname,
      },
    )
  }

  runWasmBindgen("bundler")
  runWasmBindgen("web")
  runWasmBindgen("nodejs")
}

/**
 * Build the wasm and create a tarball containing it
 *
 * @param {string} outputLocation - where to create the tarball
 */
function buildWasmTarball(outputLocation) {
  const outputDir = fs.mkdtempSync(
    path.join(os.tmpdir(), "automerge-wasm-tarball-"),
  )
  buildWasm(outputDir)
  execSync(`tar -czf ${outputLocation} -C ${outputDir} .`)
}

/**
 * @param {string | null} wasmBuildTarball
 */
function copyAndFixupWasm(wasmBuildTarball) {
  const outputPath = path.join(jsProjectDir, "src", "wasm_bindgen_output")
  fs.rmSync(outputPath, { recursive: true, force: true })

  const automergeWasmPath = path.join(
    __dirname,
    "..",
    "..",
    "rust",
    "automerge-wasm",
  )
  if (wasmBuildTarball == null) {
    buildWasm(outputPath)
  } else {
    console.log(`using prebuilt wasm tarball at ${wasmBuildTarball}`)
    execSync(`tar -xzf ${wasmBuildTarball} -C ${outputPath}`)
  }

  console.log(
    "renaming 'automerge_wasm.js' to 'automerge_wasm.cjs' in the node target",
  )
  const nodeOutputPath = path.join(
    jsProjectDir,
    "src",
    "wasm_bindgen_output",
    "nodejs",
  )
  fs.cpSync(
    path.join(nodeOutputPath, "automerge_wasm.js"),
    path.join(nodeOutputPath, "automerge_wasm.cjs"),
  )

  console.log("copying the 'web' target to 'workerd' directory")
  const webOutputPath = path.join(
    jsProjectDir,
    "src",
    "wasm_bindgen_output",
    "web",
  )
  const workerdOutputPath = path.join(
    jsProjectDir,
    "src",
    "wasm_bindgen_output",
    "workerd",
  )
  fs.cpSync(webOutputPath, workerdOutputPath, {
    recursive: true,
    dereference: true,
  })

  console.log(
    "encoding the 'wasm' blob in the 'web' target into a base64 string",
  )
  const webWasmPath = path.join(webOutputPath, "automerge_wasm_bg.wasm")
  const wasmBlob = fs.readFileSync(webWasmPath)
  const wasmBlobBase64 = wasmBlob.toString("base64")
  const wasmBlobBase64EsmPath = path.join(
    jsProjectDir,
    "src",
    "wasm_bindgen_output",
    "web",
    "automerge_wasm_bg_base64.js",
  )
  fs.writeFileSync(
    wasmBlobBase64EsmPath,
    `export const automergeWasmBase64 = "${wasmBlobBase64}"`,
  )

  console.log(
    "writing a shim to load the base64 encoded wasm in the 'web' target",
  )
  const wasmBlobBase64ShimPath = path.join(
    jsProjectDir,
    "src",
    "wasm_bindgen_output",
    "web",
    "index.js",
  )
  fs.writeFileSync(
    wasmBlobBase64ShimPath,
    `
    import { automergeWasmBase64 } from "./automerge_wasm_bg_base64.js";
    import { initSync } from "./automerge_wasm.js";
    const wasmBlob = Uint8Array.from(atob(automergeWasmBase64), c => c.charCodeAt(0));
    initSync(wasmBlob);
    export * from "./automerge_wasm.js";
    `,
  )

  console.log("moving wasm blob to top level of '/dist'")
  const wasmBlobDistPath = path.join(jsProjectDir, "dist", "automerge.wasm")
  fs.cpSync(webWasmPath, wasmBlobDistPath)

  console.log("copying 'automerge-wasm/index.d.ts' to 'src/wasm_types.d.ts'")
  fs.copyFileSync(
    path.join(automergeWasmPath, "index.d.ts"),
    path.join(jsProjectDir, "src", "wasm_types.d.ts"),
  )
}

function compileTypescript() {
  console.log("compiling typescript")
  execSync("node_modules/.bin/tsc -p config/mjs.json", {
    cwd: jsProjectDir,
  })

  execSync(
    "node_modules/.bin/tsc -p config/declonly.json --emitDeclarationOnly",
    {
      cwd: jsProjectDir,
    },
  )

  const mjsDir = path.join(jsProjectDir, "dist", "mjs")
  const wasmBindgenSrcDir = path.join(
    jsProjectDir,
    "src",
    "wasm_bindgen_output",
  )
  const mjsWasmDir = path.join(mjsDir, "wasm_bindgen_output")

  console.log(
    "copying wasm_bindgen_output directory to dist/mjs/wasm_bindgen_output",
  )
  fs.mkdirSync(mjsWasmDir, { recursive: true })
  fs.cpSync(wasmBindgenSrcDir, mjsWasmDir, {
    recursive: true,
    dereference: true,
  })

  fs.copyFileSync(
    path.join(jsProjectDir, "src", "wasm_types.d.ts"),
    path.join(jsProjectDir, "dist", "wasm_types.d.ts"),
  )

  console.log("writing a declaration for the base64 encoded wasm")
  fs.writeFileSync(
    path.join(jsProjectDir, "dist", "automerge_wasm_bg_base64.d.ts"),
    `export declare const automergeWasmBase64: string;`
  );
}

async function transpileCjs() {
  const distDir = `${jsProjectDir}/dist`
  const inDir = `${distDir}/mjs`
  const outDir = `${distDir}/cjs`

  console.log("building node CommonJS modules")
  await build({
    absWorkingDir: distDir,
    entryPoints: [
      `${inDir}/entrypoints/fullfat_node.js`,
      `${inDir}/entrypoints/slim.js`,
      `${inDir}/entrypoints/iife.js`,
    ],
    outdir: outDir,
    bundle: true,
    packages: "external",
    format: "cjs",
    target: "node14",
    platform: "node",
    outExtension: { ".js": ".cjs" },
  })

  const iifeDir = path.join(distDir, "iife")
  await build({
    absWorkingDir: distDir,
    entryPoints: [`${inDir}/entrypoints/iife.js`],
    outdir: iifeDir,
    bundle: true,
    format: "iife",
    target: "es2020",
  })

  console.log("building bundler CommonJS modules")
  await build({
    absWorkingDir: distDir,
    entryPoints: [`${inDir}/entrypoints/fullfat_base64.js`],
    outdir: outDir,
    bundle: true,
    packages: "external",
    format: "cjs",
    target: "es2020",
    platform: "node",
    outExtension: { ".js": ".cjs" },
  })
}

const { step, wasmBuildTarball } = parseCli()

if (step === "build-wasm-tarball") {
  if (wasmBuildTarball == null) {
    throw new Error("-w option must be provided when building tarball")
  }
  console.log("building wasm tarball")
  buildWasmTarball(wasmBuildTarball)
}

if (step === "all" || step === "build-wasm") {
  console.log("building wasm")
  copyAndFixupWasm(wasmBuildTarball)
}

if (step === "all" || step === "compile-typescript") {
  console.log("compiling typescript")
  compileTypescript()
}

if (step === "all" || step === "transpile-cjs") {
  console.log("transpiling to cjs")
  await transpileCjs()
  console.log("copying just the nodejs wasm bindgen output to dist/cjs")
  const wasmBindgenSrc = path.join(
    jsProjectDir,
    "/src/wasm_bindgen_output/nodejs/automerge_wasm_bg.wasm",
  )
  const cjsDir = path.join(jsProjectDir, "/dist/cjs")
  fs.copyFileSync(wasmBindgenSrc, path.join(cjsDir, "automerge_wasm_bg.wasm"))

  const wasmBlob = fs.readFileSync(wasmBindgenSrc)
  const wasmBlobBase64 = wasmBlob.toString("base64")
  const wasmBlobBase64CjsPath = path.join(
    cjsDir,
    "automerge_wasm_bg_base64.js",
  )
  fs.writeFileSync(
    wasmBlobBase64CjsPath,
    `module.exports = { automergeWasmBase64: "${wasmBlobBase64}" };`
  );

  fs.copyFileSync(
    path.join(jsProjectDir, "/src/wasm_types.d.ts"),
    path.join(cjsDir, "wasm_types.d.ts"),
  )
}
