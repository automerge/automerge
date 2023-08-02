import { once } from "events"
import { setTimeout } from "timers/promises"
import { spawn, ChildProcess } from "child_process"
import * as child_process from "child_process"
import {
  command,
  subcommands,
  run,
  array,
  multioption,
  option,
  Type,
} from "cmd-ts"
import * as path from "path"
import * as fsPromises from "fs/promises"
import fetch from "node-fetch"

const VERDACCIO_DB_PATH = path.normalize(`${__dirname}/verdacciodb`)
const VERDACCIO_CONFIG_PATH = path.normalize(`${__dirname}/verdaccio.yaml`)
const AUTOMERGE_WASM_PATH = path.normalize(
  `${__dirname}/../../rust/automerge-wasm`,
)
const AUTOMERGE_JS_PATH = path.normalize(`${__dirname}/..`)
const EXAMPLES_DIR = path.normalize(path.join(__dirname, "../", "examples"))

// The different example projects in "../examples"
type Example = "webpack" | "vite" | "create-react-app"

// Type to parse strings to `Example` so the types line up for the `buildExamples` commmand
const ReadExample: Type<string, Example> = {
  async from(str) {
    if (str === "webpack") {
      return "webpack"
    } else if (str === "vite") {
      return "vite"
    } else if (str === "create-react-app") {
      return "create-react-app"
    } else {
      throw new Error(`Unknown example type ${str}`)
    }
  },
}

type Profile = "dev" | "release"

const ReadProfile: Type<string, Profile> = {
  async from(str) {
    if (str === "dev") {
      return "dev"
    } else if (str === "release") {
      return "release"
    } else {
      throw new Error(`Unknown profile ${str}`)
    }
  },
}

const buildjs = command({
  name: "buildjs",
  args: {
    profile: option({
      type: ReadProfile,
      long: "profile",
      short: "p",
      defaultValue: () => "dev" as Profile,
    }),
  },
  handler: ({ profile }) => {
    console.log("building js")
    withPublishedWasm(profile, async (registryUrl: string) => {
      await buildAndPublishAutomergeJs(registryUrl)
    })
  },
})

const buildWasm = command({
  name: "buildwasm",
  args: {
    profile: option({
      type: ReadProfile,
      long: "profile",
      short: "p",
      defaultValue: () => "dev" as Profile,
    }),
  },
  handler: ({ profile }) => {
    console.log("building automerge-wasm")
    withRegistry(buildAutomergeWasm(profile))
  },
})

const buildexamples = command({
  name: "buildexamples",
  args: {
    examples: multioption({
      long: "example",
      short: "e",
      type: array(ReadExample),
    }),
    profile: option({
      type: ReadProfile,
      long: "profile",
      short: "p",
      defaultValue: () => "dev" as Profile,
    }),
  },
  handler: ({ examples, profile }) => {
    if (examples.length === 0) {
      examples = ["webpack", "vite", "create-react-app"]
    }
    buildExamples(examples, profile)
  },
})

const runRegistry = command({
  name: "run-registry",
  args: {
    profile: option({
      type: ReadProfile,
      long: "profile",
      short: "p",
      defaultValue: () => "dev" as Profile,
    }),
  },
  handler: ({ profile }) => {
    withPublishedWasm(profile, async (registryUrl: string) => {
      await buildAndPublishAutomergeJs(registryUrl)
      console.log("\n************************")
      console.log(`      Verdaccio NPM registry is running at ${registryUrl}`)
      console.log("      press CTRL-C to exit ")
      console.log("************************")
      await once(process, "SIGINT")
    }).catch(e => {
      console.error(`Failed: ${e}`)
    })
  },
})

const app = subcommands({
  name: "e2e",
  cmds: {
    buildjs,
    buildexamples,
    buildwasm: buildWasm,
    "run-registry": runRegistry,
  },
})

run(app, process.argv.slice(2))

async function buildExamples(examples: Array<Example>, profile: Profile) {
  await withPublishedWasm(profile, async registryUrl => {
    printHeader("building and publishing automerge")
    await buildAndPublishAutomergeJs(registryUrl)
    for (const example of examples) {
      printHeader(`building ${example} example`)
      if (example === "webpack") {
        const projectPath = path.join(EXAMPLES_DIR, example)
        await removeExistingAutomerge(projectPath)
        await fsPromises.rm(path.join(projectPath, "yarn.lock"), {
          force: true,
        })
        await spawnAndWait(
          "yarn",
          [
            "--cwd",
            projectPath,
            "install",
            "--registry",
            registryUrl,
            "--check-files",
          ],
          { stdio: "inherit" },
        )
        await spawnAndWait("yarn", ["--cwd", projectPath, "build"], {
          stdio: "inherit",
        })
      } else if (example === "vite") {
        const projectPath = path.join(EXAMPLES_DIR, example)
        await removeExistingAutomerge(projectPath)
        await fsPromises.rm(path.join(projectPath, "yarn.lock"), {
          force: true,
        })
        await spawnAndWait(
          "yarn",
          [
            "--cwd",
            projectPath,
            "install",
            "--registry",
            registryUrl,
            "--check-files",
          ],
          { stdio: "inherit" },
        )
        await spawnAndWait("yarn", ["--cwd", projectPath, "build"], {
          stdio: "inherit",
        })
      } else if (example === "create-react-app") {
        const projectPath = path.join(EXAMPLES_DIR, example)
        await removeExistingAutomerge(projectPath)
        await fsPromises.rm(path.join(projectPath, "yarn.lock"), {
          force: true,
        })
        await spawnAndWait(
          "yarn",
          [
            "--cwd",
            projectPath,
            "install",
            "--registry",
            registryUrl,
            "--check-files",
          ],
          { stdio: "inherit" },
        )
        await spawnAndWait("yarn", ["--cwd", projectPath, "build"], {
          stdio: "inherit",
        })
      }
    }
  })
}

type WithRegistryAction = (registryUrl: string) => Promise<void>

async function withRegistry(
  action: WithRegistryAction,
  ...actions: Array<WithRegistryAction>
) {
  // First, start verdaccio
  printHeader("Starting verdaccio NPM server")
  const verd = await VerdaccioProcess.start()
  actions.unshift(action)

  for (const action of actions) {
    try {
      type Step = "verd-died" | "action-completed"
      const verdDied: () => Promise<Step> = async () => {
        await verd.died()
        return "verd-died"
      }
      const actionComplete: () => Promise<Step> = async () => {
        await action("http://localhost:4873")
        return "action-completed"
      }
      const result = await Promise.race([verdDied(), actionComplete()])
      if (result === "verd-died") {
        throw new Error("verdaccio unexpectedly exited")
      }
    } catch (e) {
      await verd.kill()
      throw e
    }
  }
  await verd.kill()
}

async function withPublishedWasm(profile: Profile, action: WithRegistryAction) {
  await withRegistry(buildAutomergeWasm(profile), publishAutomergeWasm, action)
}

function buildAutomergeWasm(profile: Profile): WithRegistryAction {
  return async (registryUrl: string) => {
    printHeader("building automerge-wasm")
    await spawnAndWait(
      "yarn",
      ["--cwd", AUTOMERGE_WASM_PATH, "--registry", registryUrl, "install"],
      { stdio: "inherit" },
    )
    const cmd = profile === "release" ? "release" : "debug"
    await spawnAndWait("yarn", ["--cwd", AUTOMERGE_WASM_PATH, cmd], {
      stdio: "inherit",
    })
  }
}

async function publishAutomergeWasm(registryUrl: string) {
  printHeader("Publishing automerge-wasm to verdaccio")
  await fsPromises.rm(
    path.join(VERDACCIO_DB_PATH, "@automerge/automerge-wasm"),
    { recursive: true, force: true },
  )
  await yarnPublish(registryUrl, AUTOMERGE_WASM_PATH)
}

async function buildAndPublishAutomergeJs(registryUrl: string) {
  // Build the js package
  printHeader("Building automerge")
  await removeExistingAutomerge(AUTOMERGE_JS_PATH)
  await removeFromVerdaccio("@automerge/automerge")
  await fsPromises.rm(path.join(AUTOMERGE_JS_PATH, "yarn.lock"), {
    force: true,
  })
  await spawnAndWait(
    "yarn",
    [
      "--cwd",
      AUTOMERGE_JS_PATH,
      "install",
      "--registry",
      registryUrl,
      "--check-files",
    ],
    { stdio: "inherit" },
  )
  await spawnAndWait("yarn", ["--cwd", AUTOMERGE_JS_PATH, "build"], {
    stdio: "inherit",
  })
  await yarnPublish(registryUrl, AUTOMERGE_JS_PATH)
}

/**
 * A running verdaccio process
 *
 */
class VerdaccioProcess {
  child: ChildProcess
  stdout: Array<Buffer>
  stderr: Array<Buffer>

  constructor(child: ChildProcess) {
    this.child = child

    // Collect stdout/stderr otherwise the subprocess gets blocked writing
    this.stdout = []
    this.stderr = []
    this.child.stdout &&
      this.child.stdout.on("data", data => this.stdout.push(data))
    this.child.stderr &&
      this.child.stderr.on("data", data => this.stderr.push(data))

    const errCallback = (e: any) => {
      console.error("!!!!!!!!!ERROR IN VERDACCIO PROCESS!!!!!!!!!")
      console.error("    ", e)
      if (this.stdout.length > 0) {
        console.log("\n**Verdaccio stdout**")
        const stdout = Buffer.concat(this.stdout)
        process.stdout.write(stdout)
      }

      if (this.stderr.length > 0) {
        console.log("\n**Verdaccio stderr**")
        const stdout = Buffer.concat(this.stderr)
        process.stdout.write(stdout)
      }
      process.exit(-1)
    }
    this.child.on("error", errCallback)
  }

  /**
   * Spawn a verdaccio process and wait for it to respond succesfully to http requests
   *
   * The returned `VerdaccioProcess` can be used to control the subprocess
   */
  static async start() {
    const child = spawn(
      "yarn",
      ["verdaccio", "--config", VERDACCIO_CONFIG_PATH],
      { env: { ...process.env, FORCE_COLOR: "true" } },
    )

    // Forward stdout and stderr whilst waiting for startup to complete
    const stdoutCallback = (data: Buffer) => process.stdout.write(data)
    const stderrCallback = (data: Buffer) => process.stderr.write(data)
    child.stdout && child.stdout.on("data", stdoutCallback)
    child.stderr && child.stderr.on("data", stderrCallback)

    const healthCheck = async () => {
      while (true) {
        try {
          const resp = await fetch("http://localhost:4873")
          if (resp.status === 200) {
            return
          } else {
            console.log(`Healthcheck failed: bad status ${resp.status}`)
          }
        } catch (e) {
          console.error(`Healthcheck failed: ${e}`)
        }
        await setTimeout(500)
      }
    }
    await withTimeout(healthCheck(), 10000)

    // Stop forwarding stdout/stderr
    child.stdout && child.stdout.off("data", stdoutCallback)
    child.stderr && child.stderr.off("data", stderrCallback)
    return new VerdaccioProcess(child)
  }

  /**
   * Send a SIGKILL to the process and wait for it to stop
   */
  async kill() {
    this.child.stdout && this.child.stdout.destroy()
    this.child.stderr && this.child.stderr.destroy()
    this.child.kill()
    try {
      await withTimeout(once(this.child, "close"), 500)
    } catch (e) {
      console.error("unable to kill verdaccio subprocess, trying -9")
      this.child.kill(9)
      await withTimeout(once(this.child, "close"), 500)
    }
  }

  /**
   * A promise which resolves if the subprocess exits for some reason
   */
  async died(): Promise<number | null> {
    const [exit, _signal] = await once(this.child, "exit")
    return exit
  }
}

function printHeader(header: string) {
  console.log("\n===============================")
  console.log(`           ${header}`)
  console.log("===============================")
}

/**
 * Removes the automerge, @automerge/automerge-wasm, and @automerge/automerge packages from
 * `$packageDir/node_modules`
 *
 * This is useful to force refreshing a package by use in combination with
 * `yarn install --check-files`, which checks if a package is present in
 * `node_modules` and if it is not forces a reinstall.
 *
 * @param packageDir - The directory containing the package.json of the target project
 */
async function removeExistingAutomerge(packageDir: string) {
  await fsPromises.rm(path.join(packageDir, "node_modules", "@automerge"), {
    recursive: true,
    force: true,
  })
  await fsPromises.rm(path.join(packageDir, "node_modules", "automerge"), {
    recursive: true,
    force: true,
  })
}

type SpawnResult = {
  stdout?: Buffer
  stderr?: Buffer
}

async function spawnAndWait(
  cmd: string,
  args: Array<string>,
  options: child_process.SpawnOptions,
): Promise<SpawnResult> {
  const child = spawn(cmd, args, options)
  let stdout = null
  let stderr = null
  if (child.stdout) {
    stdout = []
    child.stdout.on("data", data => stdout.push(data))
  }
  if (child.stderr) {
    stderr = []
    child.stderr.on("data", data => stderr.push(data))
  }

  const [exit, _signal] = await once(child, "exit")
  if (exit && exit !== 0) {
    throw new Error("nonzero exit code")
  }
  return {
    stderr: stderr ? Buffer.concat(stderr) : null,
    stdout: stdout ? Buffer.concat(stdout) : null,
  }
}

/**
 * Remove a package from the verdaccio registry. This is necessary because we
 * often want to _replace_ a version rather than update the version number.
 * Obviously this is very bad and verboten in normal circumastances, but the
 * whole point here is to be able to test the entire packaging story so it's
 * okay I Promise.
 */
async function removeFromVerdaccio(packageName: string) {
  await fsPromises.rm(path.join(VERDACCIO_DB_PATH, packageName), {
    force: true,
    recursive: true,
  })
}

async function yarnPublish(registryUrl: string, cwd: string) {
  await spawnAndWait(
    "yarn",
    ["--registry", registryUrl, "--cwd", cwd, "publish", "--non-interactive"],
    {
      stdio: "inherit",
      env: {
        ...process.env,
        FORCE_COLOR: "true",
        // This is a fake token, it just has to be the right format
        npm_config__auth:
          "//localhost:4873/:_authToken=Gp2Mgxm4faa/7wp0dMSuRA==",
      },
    },
  )
}

/**
 * Wait for a given delay to resolve a promise, throwing an error if the
 * promise doesn't resolve with the timeout
 *
 * @param promise - the promise to wait for @param timeout - the delay in
 * milliseconds to wait before throwing
 */
async function withTimeout<T>(
  promise: Promise<T>,
  timeout: number,
): Promise<T> {
  type Step = "timed-out" | { result: T }
  const timedOut: () => Promise<Step> = async () => {
    await setTimeout(timeout)
    return "timed-out"
  }
  const succeeded: () => Promise<Step> = async () => {
    const result = await promise
    return { result }
  }
  const result = await Promise.race([timedOut(), succeeded()])
  if (result === "timed-out") {
    throw new Error("timed out")
  } else {
    return result.result
  }
}
