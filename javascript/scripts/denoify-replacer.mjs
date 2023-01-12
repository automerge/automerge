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
              return `${parsedImportExportStatement.statementType} type ${parsedImportExportStatement.target} from "https://deno.land/x/automerge_wasm@${version}/index.d.ts";`
            } else {
              return `${parsedImportExportStatement.statementType} ${parsedImportExportStatement.target} from "https://deno.land/x/automerge_wasm@${version}/automerge_wasm.js";`
            }

            // if (parsedImportExportStatement.isTypeOnly) {
            //   return `${parsedImportExportStatement.statementType} type ${parsedImportExportStatement.target} from "https://raw.githubusercontent.com/onsetsoftware/automerge-rs/js/automerge-wasm-0.1.20-alpha.6/deno_wasm_dist/index.d.ts";`
            // } else {
            //   return `${parsedImportExportStatement.statementType} ${parsedImportExportStatement.target} from "https://raw.githubusercontent.com/onsetsoftware/automerge-rs/js/automerge-wasm-0.1.20-alpha.6/deno_wasm_dist/automerge_wasm.js";`
            // }
          }
        }
        break
    }

    //The replacer should return undefined when we want to let denoify replace the statement
    return undefined
  }
)
