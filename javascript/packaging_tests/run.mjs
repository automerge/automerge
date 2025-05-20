// @ts-check
import fs from "fs/promises"
import util from "node:util"
import child_process from "node:child_process"
import os from "os"
import { fileURLToPath } from "node:url"
import path from "path"
import { once } from "node:events"
import { consola } from "consola"
import http from "node:http"
import serveHandler from "serve-handler"
import puppeteer from "puppeteer"
import { setTimeout } from "timers/promises"

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const projectRoot = path.join(__dirname, "..")

const exec = util.promisify(child_process.exec)

// This script builds the current version of the package, then installs it in
// various environments and ensures that all the tests run.
//
// The environments we want to test are:
//
// - In browsers after packaging with a bundler. This means we have to test
//   each combination of (bundler, browser)
// - In browsers but using the slim package and late initializing the wasm
// - In browsers using a completely bundled script
// - In browsers using the slim package and late initializing the wasm
// - In node as a CommonJS module
// - In node using the slim package as a CommonJS module and late initializing
// - In node as an ES module
// - In node using the slim package as an ES module and late initializing
//
// In general the approach we will use to perform these tests is to use
// `npm pack` to create the package, then install it in a temporary project.
//
// For browser tests we will use `puppetteer` to run the tests.

/**
 * @param {string} tmpProjectDir - the path to a temporary directory to run the test in
 */
async function runWebpackTest(tmpProjectDir) {
  consola.info("running webpack")
  const webpackProcess = child_process.spawn(
    "./node_modules/.bin/webpack",
    {
      cwd: tmpProjectDir,
    }
  )
  webpackProcess.stdout.on("data", (data) => {
    for (const line of data.toString().split("\n")) {
      consola.info("webpack: " + line)
    }
  })
  webpackProcess.stderr.on("data", (data) => {
    for (const line of data.toString().split("\n")) {
      consola.info("webpack: " + line)
    }
  })
  try {
    const exitCode = (await once(webpackProcess, "close"))[0]
    if (exitCode != 0) {
      throw new Error("webpack failed with exit code " + exitCode)
    }
  } catch (e) {
    throw e
  }

  consola.info("starting static server")
  const server = await serveStatic(path.join(tmpProjectDir, "dist"))

  /** @type {Promise<{type: "serverDied"}>} */
  const serverDied = once(server, "close").then(() => {return {type: "serverDied"}})
  /** @type {Promise<{type: "finished", result: boolean}>} */
  const success = loadTestPage("http://localhost:3000").then((success) => {return {type: "finished", result: success}})

  const result = await Promise.race([serverDied, success])
  try {
    if (result.type === "serverDied") {
      throw new Error("Webpack dev server died")
    }
    if (result.result === false) {
      throw new Error("Test page failed to load")
    }
  } finally {
    server.close()
  }
}

/**
 * @returns {Promise<http.Server>} - the server that is serving the static files
 */
async function serveStatic(dir) {
  const server = http.createServer((request, response) => {
      return serveHandler(request, response, {
          public: dir,
      })
  })

  const listening = once(server, "listening")

  server.listen(3000, () => {
      console.log("Running at http://localhost:3000")
  })

  await listening

  return server
}

async function runViteDevServerTest(tmpProjectDir) {
  consola.info("running vite")
  const port = await findFreePort()
  const viteProcess = child_process.spawn(
    "./node_modules/.bin/vite",
    ["--port", port.toString()],
    {
      cwd: tmpProjectDir,
    }
  )
  viteProcess.stdout.on("data", (data) => {
    for (const line of data.toString().split("\n")) {
      consola.info("vite: " + line)
    }
  })
  viteProcess.stderr.on("data", (data) => {
    for (const line of data.toString().split("\n")) {
      consola.info("vite: " + line)
    }
  })
  try {
    const result = await loadTestPage(`http://localhost:${port}`)
    if (!result) {
      throw new Error("Test page failed")
    }
  } finally {
    viteProcess.kill()
  }
}

/**
 * @param {string} tmpProjectDir
*/
async function runViteBuildTest(tmpProjectDir) {
  consola.info("running vite build")
  await exec("./node_modules/.bin/vite build", { cwd: tmpProjectDir })

  // Check that only one version of the .wasm file is output to the build
  // directory. See the comments in the `runWasmBindgen` function in `build.mjs`
  // for details on why multiple wasm blobs might be produced
  let assetsDir = path.join(tmpProjectDir, "dist", "assets")
  let wasmBlobs = []
  for await (const blob of fs.glob("*.wasm", { cwd: assetsDir} )){
    wasmBlobs.push(blob)
  }
  if (wasmBlobs.length != 1) {
    throw new Error(`Expected exctly one wasm blob in ${assetsDir} but found ${wasmBlobs.length}`)
  }

  consola.info("running vite preview")
  const port = await findFreePort()
  const viteProcess = child_process.spawn(
    "./node_modules/.bin/vite",
    ["preview", "--port", port.toString()],
    {
      cwd: tmpProjectDir,
    }
  )
  viteProcess.stdout.on("data", (data) => {
    for (const line of data.toString().split("\n")) {
      consola.info("vite: " + line)
    }
  })
  viteProcess.stderr.on("data", (data) => {
    for (const line of data.toString().split("\n")) {
      consola.info("vite: " + line)
    }
  })
  try {
    await loadTestPage(`http://localhost:${port}`)
  } finally {
    viteProcess.kill()
  }
}

async function findFreePort() {
  const server = http.createServer()
  server.listen(0)
  // @ts-ignore
  const port = server.address().port
  server.close()
  return port
}

/**
 *
 * @param {string} url - the URL to load the test page from
 *
 * @returns {Promise<boolean>} - whether we succesfully loaded the test page
 */
async function loadTestPage(url) {
  consola.info("opening test page")
  // This `--no-sandbox` flag is necessary to run puppeteer in github actions where
  // there is no sandbox available. It's dangerous if you're running untrusted content,
  // but everything we are loading is hosted in the respository
  const browser = await puppeteer.launch({ args: ['--no-sandbox', '--disable-setuid-sandbox'] })
  const page = await browser.newPage()
  page.setDefaultTimeout(5000)

  // Retry loading the page a few times in case the server is slow to start
  const MAX_RETRIES = 5
  let retryCount = 0
  while (true) {
    try {
      await page.goto(url)
      break
    } catch(e) {
      if (retryCount >= MAX_RETRIES) {
        consola.error("Failed to connect to test page")
        throw e
      } else {
        consola.warn("connection refused, retrying in 1s")
        retryCount++
        await setTimeout(1000)
      }
    }
  }
  await page.waitForSelector("#result")
  const result = await page.evaluate(() => {
    // @ts-ignore
    return document.querySelector("#result").textContent === "hello automerge"
  })
  await browser.close()
  return result
}

async function runNodeTest(tmpProjectDir) {
  consola.info("running node")
  const { stdout } = await exec("node index.js", { cwd: tmpProjectDir })
  if (stdout !== "hello webpack\n") {
    throw new Error("Node test failed")
  }
}

async function runWorkerdTest(tmpProjectDir) {
  consola.info("starting wrangler dev server")

  // Start wrangler and wait for it to be ready
  const port = await findFreePort()
  const wranglerProcess = child_process.spawn(
    "./node_modules/.bin/wrangler",
    ["dev", "--port", port.toString()],
    {
      cwd: tmpProjectDir,
      env: {
        ...process.env,
        PWD: tmpProjectDir,
      }
    },
  )
  /** @type { null | "started" | "errored"} */
  let status = null
  let resolveReady
  let rejectReady
  const ready = new Promise((resolve, reject) => {
    resolveReady = resolve
    rejectReady = reject
  })
  let interval = setInterval(() => {
    if (status === "started") {
      resolveReady()
      clearInterval(interval)
    } else if (status === "errored") {
      rejectReady()
      clearInterval(interval)
    }
  }, 500)
  wranglerProcess.stdout.on("data", (data) => {
    for (const line of data.toString().split("\n")) {
      if (line.includes("Ready") && status === null) {
        status = "started"
      }
      if (line.includes("ERROR")) {
        status = "errored"
      }
      consola.info("wrangler: " + line)
    }
  })
  wranglerProcess.stderr.on("data", (data) => {
    for (const line of data.toString().split("\n")) {
      if (line.includes("Ready") && status === null) {
        status = "started"
      }
      if (line.includes("ERROR")) {
        status = "errored"
      }
      consola.info("wrangler: " + line)
    }
  })

  try {
    await ready
    const resp = await fetch(`http://localhost:${port}`)
    if (resp.status !== 200) {
      throw new Error("Wrangler dev server failed")
    }
    const data = await resp.json()
    if (typeof data != "object" ||data["message"] !== "hello workerd") {
      throw new Error("unexpected response from workerd")
    }
  } finally {
    wranglerProcess.kill()
  }
}

async function runIifeTest(tmpProjectDir) {
  // Copy the iife.js file from node modules to the local directory
  const iifePath = path.join(
      tmpProjectDir,
      "node_modules",
      "@automerge",
      "automerge",
      "dist",
      "iife",
      "iife.js"
  )
  await fs.copyFile(iifePath, path.join(tmpProjectDir, "automerge.js"))
  const server = await serveStatic(tmpProjectDir)

  /** @type {Promise<{type: "serverDied"}>} */
  const serverDied = once(server, "close").then(() => {return {type: "serverDied"}})
  /** @type {Promise<{type: "finished", result: boolean}>} */
  const success = loadTestPage("http://localhost:3000/index.html").then((success) => {return {type: "finished", result: success}})

  const result = await Promise.race([serverDied, success])
  try {
    if (result.type === "serverDied") {
      throw new Error("serve handler died")
    }
    if (result.result === false) {
      throw new Error("Test page failed to load")
    }
  } finally {
    server.close()
  }
}

/**
  * @returns {Promise<string>} - the path to the tarball produced by `npm pack`
  */
async function pack() {
  consola.info("running npm pack")
  const tempDir = await fs.mkdtemp(path.join(os.tmpdir(), "automerge-packaging-test-tarball"))
  const { stdout } = await exec(`npm pack --pack-destination ${tempDir} --json`, { cwd: projectRoot})
  const filename = JSON.parse(stdout)[0].filename
  return path.join(tempDir, filename)
}

async function run() {
  const tarballPath = await pack()

  let testCases = [
    { dir: "webpack_cjs_fullfat", scenarios:[{ run: runWebpackTest}] },
    { dir: "webpack_cjs_slim", scenarios:[{ run: runWebpackTest}] },
    { dir: "webpack_esm_fullfat", scenarios:[{ run: runWebpackTest}] },
    { dir: "webpack_esm_slim", scenarios:[{ run: runWebpackTest}] },
    { dir: "node_cjs_fullfat", scenarios:[{ run: runNodeTest}] },
    { dir: "node_cjs_slim", scenarios:[{ run: runNodeTest}] },
    { dir: "node_esm_fullfat", scenarios:[{ run: runNodeTest}] },
    { dir: "node_esm_slim", scenarios:[{ run: runNodeTest}] },
    { dir: "vite_fullfat", scenarios:[
      { run: runViteDevServerTest, name: "vite_dev_server_fullfat"},
      { run: runViteBuildTest, name: "vite_build_fullfat"}
    ] },
    { dir: "vite_slim", scenarios:[
      { run: runViteDevServerTest, name: "vite_dev_server_slim"},
      { run: runViteBuildTest, name: "vite_build_slim"}
    ] },
    { dir: "vite_iife_fullfat", scenarios: [{ run: runViteBuildTest}] },
    { dir: "workerd", scenarios: [{run: runWorkerdTest}] },
    { dir: "workerd_slim", scenarios: [{run: runWorkerdTest}] },
    { dir: "iife", scenarios: [{run: runIifeTest}] },
  ]

  let testCase = null
  let scenario = null
  if (process.argv.length > 2) {
    for (const candidateTestCase of testCases) {
      if (candidateTestCase.dir === process.argv[2]) {
        testCase = candidateTestCase
        break
      }
      for (const candidateScenario of candidateTestCase.scenarios) {
        if (candidateScenario.name === process.argv[2]) {
          testCase = candidateTestCase
          scenario = candidateScenario
          break
        }
      }
    }
    if (!testCase) {
      throw new Error(`Unknown test case ${process.argv[2]}`)
    }
  }

  if (testCase) {
    if (scenario) {
      testCases = [{ dir: testCase.dir, scenarios: [scenario] }]
    } else {
      testCases = [testCase]
    }
  }

  for (const testCase of testCases) {
    for (const scenario of testCase.scenarios) {
        let name = testCase.name || testCase.dir
        consola.box(`Running test: ${name}`)
        const tmpProjectDir = await fs.mkdtemp(path.join(os.tmpdir(), `automerge-packaging-test`))

        await fs.cp(path.join(__dirname, testCase.dir), tmpProjectDir, { recursive: true })

        consola.info("npm install in ", tmpProjectDir)
        await exec("npm install", { cwd: tmpProjectDir })
        consola.info("npm install ", tarballPath)
        await exec(`npm install ${tarballPath}`, { cwd: tmpProjectDir })

      if (scenario.name) {
        consola.info(`Running ${scenario.name}`)
      }
      try {
        await scenario.run(tmpProjectDir)
      } catch (e) {
        consola.error(`Testcase ${testCase.dir} failed`)
        consola.error(e)
        consola.error(`The failed build is in ${tmpProjectDir}`)
        process.exit(1)
      }
      await fs.rm(tmpProjectDir, { recursive: true })
      consola.success("Test passed")
    }
  }
}

(async () => {
  try {
    await run()
  } catch (e) {
    console.error(e)
    process.exit(1)
  }
})()
