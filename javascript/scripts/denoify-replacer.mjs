// @denoify-ignore

import { makeThisModuleAnExecutableReplacer } from "denoify"
// import { assert } from "tsafe";
// import * as path from "path";

makeThisModuleAnExecutableReplacer(
  async ({ parsedImportExportStatement, destDirPath, version }) => {
    version = process.env.VERSION || version

    switch (parsedImportExportStatement.parsedArgument.nodeModuleName) {
      case "@automerge/automerge-wasm":
        {
          const moduleRoot =
            process.env.ROOT_MODULE ||
            `https://deno.land/x/automerge_wasm@${version}`
          /*
           *We expect not to run against statements like
           *import(..).then(...)
           *or
           *export * from "..."
           *in our code.
           */
          if (
            !parsedImportExportStatement.isAsyncImport &&
            (parsedImportExportStatement.statementType === "import" ||
              parsedImportExportStatement.statementType === "export")
          ) {
            if (parsedImportExportStatement.isTypeOnly) {
              return `${parsedImportExportStatement.statementType} type ${parsedImportExportStatement.target} from "${moduleRoot}/index.d.ts";`
            } else {
              return `${parsedImportExportStatement.statementType} ${parsedImportExportStatement.target} from "${moduleRoot}/automerge_wasm.js";`
            }
          }
        }
        break
    }

    //The replacer should return undefined when we want to let denoify replace the statement
    return undefined
  },
)
