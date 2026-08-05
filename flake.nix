{
  description = "automerge";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-26.05";
    nixos-unstable.url = "nixpkgs/nixos-unstable-small";

    command-utils.url = "github:expede/nix-command-utils";
    flake-utils.url = "github:numtide/flake-utils";

    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = {
    self,
    nixpkgs,
    nixos-unstable,
    command-utils,
    flake-utils,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [
          (import rust-overlay)
        ];

        pkgs = import nixpkgs {inherit system overlays;};
        unstable = import nixos-unstable {inherit system overlays;};

        nodejs = pkgs.nodejs_26;
        rust-toolchain = (pkgs.rust-bin.fromRustupToolchainFile ./rust/rust-toolchain.toml).override {
          extensions = [
            "cargo"
            "clippy"
            "llvm-tools-preview"
            "rust-src"
            "rust-std"
            "rustfmt"
          ];

          targets = [
            "aarch64-apple-darwin"
            "x86_64-apple-darwin"

            "x86_64-unknown-linux-musl"
            "aarch64-unknown-linux-musl"

            "wasm32-unknown-unknown"
          ];
        };

        # Pinned nightly for the wasm build (must match WASM_TOOLCHAIN in CI).
        # `-Zbuild-std` needs a nightly cargo/rustc plus the rust-src component.
        wasm-rust-toolchain = pkgs.rust-bin.nightly."2026-04-25".minimal.override {
          extensions = ["rust-src"];
          targets = ["wasm32-unknown-unknown"];
        };

        # rustup-style `cargo +nightly` doesn't work without rustup, so expose
        # the nightly toolchain behind a wrapper that javascript/scripts/build.mjs
        # can use via WASM_CARGO. PATH is prepended so nightly cargo also picks
        # up nightly rustc rather than the stable one from the devshell.
        wasm-cargo = pkgs.writeShellScriptBin "wasm-cargo" ''
          export PATH=${wasm-rust-toolchain}/bin:$PATH
          exec cargo "$@"
        '';

        # CI pins wasm-bindgen-cli 0.2.126 (see .github/workflows/ci.yaml);
        # the CLI version must match the `wasm-bindgen` crate in rust/Cargo.lock.
        wasm-bindgen-cli = unstable.buildWasmBindgenCli rec {
          src = pkgs.fetchCrate {
            pname = "wasm-bindgen-cli";
            version = "0.2.126";
            hash = "sha256-H6Is3fiZVxZCfOMWK5dWMSrtn50VGv0sfdnsT+cTtyk=";
          };

          cargoDeps = unstable.rustPlatform.fetchCargoVendor {
            inherit src;
            inherit (src) pname version;
            hash = "sha256-VucqkXbCi4qtQzY/HrXiDnbSURsagPsdNVMn1Tw3UiY=";
          };
        };

        format-pkgs = with pkgs; [
          nixpkgs-fmt
          alejandra
          taplo
        ];

        darwin-installs = with pkgs.darwin.apple_sdk.frameworks; [
          Security
          CoreFoundation
          Foundation
        ];

        cargo-installs = with pkgs; [
          cargo-criterion
          unstable.cargo-deny
          cargo-expand
          cargo-nextest
          cargo-outdated
          cargo-sort
          cargo-udeps
          cargo-watch
          # llvmPackages.bintools
          twiggy
          wasm-bindgen-cli
          wasm-tools
        ];

        cargo = "${pkgs.cargo}/bin/cargo";
        deno = "${unstable.deno}/bin/deno";
        node = "${unstable.nodejs_20}/bin/node";
        wasm-opt = "${pkgs.binaryen}/bin/wasm-opt";
        wasm-pack = "${unstable.wasm-pack}/bin/wasm-pack";
        npm = "${nodejs}/bin/npm";

        cmd = command-utils.cmd.${system};

        js-dir = "./javascript";
        rust-dir = "--manifest-path ./rust/Cargo.toml";
        wasm-dir = "./rust/automerge-wasm";

        release = {
          "release:host" =
            cmd "Build release for the current host (${system})"
            "${cargo} build ${rust-dir} --release";

          "release:wasm:web" =
            cmd "Build release for wasm32-unknown-unknown with web bindings"
            "${wasm-pack} build ${wasm-dir} --release --target=web";

          "release:wasm:nodejs" =
            cmd "Build release for wasm32-unknown-unknown with Node.js bindgings"
            "${wasm-pack} build ${wasm-dir} --release --target=nodejs";
        };

        build = {
          "build:host" =
            cmd "Build for ${system}"
            "${cargo} build ${rust-dir}";

          "build:wasm:web" =
            cmd "Build for wasm32-unknown-unknown with web bindings"
            "${wasm-pack} build ${wasm-dir} --dev --target=web";

          "build:wasm:nodejs" =
            cmd "Build for wasm32-unknown-unknown with Node.js bindgings"
            "${wasm-pack} build ${wasm-dir} --dev --target=nodejs";

          "build:node" =
            cmd "Build JS-wrapped Wasm library"
            "${npm} --prefix ${js-dir} install && ${npm} --prefix ${js-dir} run build";

          "build:deno" =
            cmd "Build Deno-wrapped Wasm library"
            "cd ${js-dir} && ${deno} install && ${deno} run build && cd ..";

          "build:wasi" =
            cmd "Build for Wasm32-WASI"
            "${cargo} build ${wasm-dir} --target wasm32-wasi";
        };

        bench = {
          "bench" =
            cmd "Run benchmarks, including test utils"
            "${cargo} bench ${rust-dir}";

          "bench:host:open" =
            cmd "Open host Criterion benchmarks in browser"
            "${pkgs.xdg-utils}/bin/xdg-open ./rust/target/criterion/report/index.html";
        };

        lint = {
          "lint" =
            cmd "Run Clippy"
            "${cargo} clippy ${rust-dir}";

          "lint:pedantic" =
            cmd "Run Clippy pedantically"
            "${cargo} clippy ${rust-dir} -- -W clippy::pedantic";

          "lint:fix" =
            cmd "Apply non-pendantic Clippy suggestions"
            "${cargo} clippy ${rust-dir} --fix";
        };

        watch = {
          "watch:build:host" =
            cmd "Rebuild host target on save"
            "${cargo} watch ${rust-dir} --clear";

          "watch:build:wasm" =
            cmd "Rebuild Wasm target on save"
            "${cargo} watch ${wasm-dir} --clear -- cargo build --target=wasm32-unknown-unknown";

          "watch:lint" =
            cmd "Lint on save"
            "${cargo} watch ${rust-dir} --clear --exec clippy";

          "watch:lint:pedantic" =
            cmd "Pedantic lint on save"
            "${cargo} watch ${rust-dir} --clear --exec 'clippy -- -W clippy::pedantic'";

          "watch:test:host" =
            cmd "Run all host tests on save"
            "${cargo} watch ${rust-dir} --clear --exec 'test && test --doc'";

          "watch:test:wasm" =
            cmd "Run all Wasm tests on save"
            "${cargo} watch ${wasm-dir} --clear --exec 'test --target=wasm32-unknown-unknown && test --doc --target=wasm32-unknown-unknown'";
        };

        test = {
          "test:all" =
            cmd "Run Cargo tests"
            "test:host && test:docs && test:wasm";

          "test:host" =
            cmd "Run Cargo tests for host target"
            "${cargo} test ${rust-dir} && ${cargo} test ${rust-dir} --doc";

          "test:wasm" =
            cmd "Run wasm-pack tests on all targets"
            "test:wasm:node && test:wasm:chrome";

          "test:wasm:node" =
            cmd "Run wasm-pack tests in Node.js"
            "${wasm-pack} test ${wasm-dir} --node";

          "test:wasm:chrome" =
            cmd "Run wasm-pack tests in headless Chrome"
            "${wasm-pack} test ${wasm-dir} --headless --chrome";

          "test:docs" =
            cmd "Run Cargo doctests"
            "${cargo} test ${rust-dir} --doc";
        };

        docs = {
          "docs:build:host" =
            cmd "Refresh the docs"
            "${cargo} doc ${rust-dir}";

          "docs:build:wasm" =
            cmd "Refresh the docs with the wasm32-unknown-unknown target"
            "${cargo} doc ${wasm-dir} --target=wasm32-unknown-unknown";

          "docs:open:host" =
            cmd "Open refreshed docs"
            "${cargo} doc ${rust-dir} --open";

          "docs:open:wasm" =
            cmd "Open refreshed docs"
            "${cargo} doc ${wasm-dir} --open --target=wasm32-unknown-unknown";
        };

        command_menu =
          command-utils.commands.${system}
          (release // build // bench // lint // watch // test // docs);
      in rec {
        devShells.default = pkgs.mkShell {
          name = "automerge";

          nativeBuildInputs = with pkgs;
            [
              # Rust
              (lib.hiPrio pkgs.rust-bin.nightly.latest.rustfmt)
              cargo-criterion
              cargo-deny
              cargo-edit
              cargo-flamegraph
              cargo-fuzz
              cargo-watch
              rust-toolchain
              wasm-cargo
              unstable.irust

              # Wasm
              unstable.binaryen
              unstable.wasm-pack

              # JS
              chromedriver
              chromium
              unstable.deno
              nodejs # Current LTS

              # Clang
              cmake
              cmocka
              doxygen

              # Nix
              direnv
              nixpkgs-fmt

              # External Libraries
              gnuplot
              openssl
            ]
            # Commands
            ++ command_menu
            ++ format-pkgs
            ++ cargo-installs
            ++ lib.optionals stdenv.isDarwin darwin-installs;

          WASM_CARGO = "wasm-cargo";

          # Use the Nix-provided Chromium for the JS packaging tests; the
          # Chrome that Puppeteer downloads does not run on NixOS.
          PUPPETEER_SKIP_DOWNLOAD = "1";
          PUPPETEER_EXECUTABLE_PATH = "${pkgs.chromium}/bin/chromium";

          shellHook = "menu";
        };

        formatter = pkgs.alejandra;
      }
    );
}
