import * as fs from "fs"

const files = ["./deno_dist/proxies.ts"]
for (const filepath of files) {
  const data = fs.readFileSync(filepath)
  fs.writeFileSync(filepath, "// @ts-nocheck \n" + data)

  console.log('Prepended "// @ts-nocheck" to ' + filepath)
}
