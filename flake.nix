{
  description = "automerge";

  inputs = {
    nixpkgs.url = "nixpkgs/nixos-25.05";
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
  }: flake-utils.lib.eachDefaultSystem (system:
    let
      overlays = [
        (import rust-overlay)
      ];

      pkgs = import nixpkgs { inherit system overlays; };
      unstable = import nixos-unstable { inherit system overlays; };

      rustVersion = "1.86.0";

      rust-toolchain = pkgs.rust-bin.stable.${rustVersion}.default.override {
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

      format-pkgs = with pkgs; [
        nixpkgs-fmt
        alejandra
        taplo
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
        unstable.wasm-bindgen-cli
        wasm-tools
      ];

      cargo = "${pkgs.cargo}/bin/cargo";
      deno = "${unstable.deno}/bin/deno";
      node = "${unstable.nodejs_22}/bin/node";
      wasm-opt = "${pkgs.binaryen}/bin/wasm-opt";
      wasm-pack = "${unstable.wasm-pack}/bin/wasm-pack";
      yarn = "${pkgs.yarn}/bin/yarn";

      cmd = command-utils.cmd.${system};

      js-dir = "./javascript";
      rust-dir = "--manifest-path ./rust/Cargo.toml";
      wasm-dir = "./rust/automerge-wasm";

      release = {
        "release:host" = cmd "Build release for the current host (${system})"
          "${cargo} build ${rust-dir} --release";

        "release:wasm:web" = cmd "Build release for wasm32-unknown-unknown with web bindings"
          "${wasm-pack} build ${wasm-dir} --release --target=web";

        "release:wasm:nodejs" = cmd "Build release for wasm32-unknown-unknown with Node.js bindgings"
          "${wasm-pack} build ${wasm-dir} --release --target=nodejs";
      };

      build = {
        "build:host" = cmd "Build for ${system}"
          "${cargo} build ${rust-dir}";

        "build:wasm:web" = cmd "Build for wasm32-unknown-unknown with web bindings"
          "${wasm-pack} build ${wasm-dir} --dev --target=web";
        
        "build:wasm:nodejs" = cmd "Build for wasm32-unknown-unknown with Node.js bindgings"
          "${wasm-pack} build ${wasm-dir} --dev --target=nodejs";

        "build:node" = cmd "Build JS-wrapped Wasm library"
          "${yarn} install --cwd ${js-dir} && ${yarn} --cwd ${js-dir} run build";

        "build:deno" = cmd "Build Deno-wrapped Wasm library"
          "cd ${js-dir} && ${deno} install && ${deno} run build && cd ..";

        "build:wasi" = cmd "Build for Wasm32-WASI"
          "${cargo} build ${wasm-dir} --target wasm32-wasi";
      };

      bench = {
        "bench" = cmd "Run benchmarks, including test utils"
          "${cargo} bench ${rust-dir}";

        "bench:host:open" = cmd "Open host Criterion benchmarks in browser"
          "${pkgs.xdg-utils}/bin/xdg-open ./rust/target/criterion/report/index.html";
      };

      lint = {
        "lint" = cmd "Run Clippy"
          "${cargo} clippy ${rust-dir}";

        "lint:pedantic" = cmd "Run Clippy pedantically"
          "${cargo} clippy ${rust-dir} -- -W clippy::pedantic";

        "lint:fix" = cmd "Apply non-pendantic Clippy suggestions"
          "${cargo} clippy ${rust-dir} --fix";
      };

      watch = {
        "watch:build:host" = cmd "Rebuild host target on save"
          "${cargo} watch ${rust-dir} --clear";

        "watch:build:wasm" = cmd "Rebuild Wasm target on save"
          "${cargo} watch ${wasm-dir} --clear -- cargo build --target=wasm32-unknown-unknown";

        "watch:lint" = cmd "Lint on save"
          "${cargo} watch ${rust-dir} --clear --exec clippy";

        "watch:lint:pedantic" = cmd "Pedantic lint on save"
          "${cargo} watch ${rust-dir} --clear --exec 'clippy -- -W clippy::pedantic'";

        "watch:test:host" = cmd "Run all host tests on save"
          "${cargo} watch ${rust-dir} --clear --exec 'test && test --doc'";

        "watch:test:wasm" = cmd "Run all Wasm tests on save"
          "${cargo} watch ${wasm-dir} --clear --exec 'test --target=wasm32-unknown-unknown && test --doc --target=wasm32-unknown-unknown'";
      };

      test = {
        "test:all" = cmd "Run Cargo tests"
          "test:host && test:docs && test:wasm";

        "test:host" = cmd "Run Cargo tests for host target"
          "${cargo} test ${rust-dir} && ${cargo} test ${rust-dir} --doc";

        "test:wasm" = cmd "Run wasm-pack tests on all targets"
          "test:wasm:node && test:wasm:chrome";

        "test:wasm:node" = cmd "Run wasm-pack tests in Node.js"
          "${wasm-pack} test ${wasm-dir} --node";

        "test:wasm:chrome" = cmd "Run wasm-pack tests in headless Chrome"
          "${wasm-pack} test ${wasm-dir} --headless --chrome";

        "test:docs" = cmd "Run Cargo doctests"
          "${cargo} test ${rust-dir} --doc";
      };

      docs = {
        "docs:build:host" = cmd "Refresh the docs"
          "${cargo} doc ${rust-dir}";

        "docs:build:wasm" = cmd "Refresh the docs with the wasm32-unknown-unknown target"
          "${cargo} doc ${wasm-dir} --target=wasm32-unknown-unknown";

        "docs:open:host" = cmd "Open refreshed docs"
          "${cargo} doc ${rust-dir} --open";

        "docs:open:wasm" = cmd "Open refreshed docs"
          "${cargo} doc ${wasm-dir} --open --target=wasm32-unknown-unknown";
      };

      command_menu = command-utils.commands.${system}
        (release // build // bench // lint // watch // test // docs);
    in rec {
      devShells.default = pkgs.mkShell {
        name = "automerge";

        nativeBuildInputs = with pkgs;
          [
            # Rust
            (pkgs.hiPrio pkgs.rust-bin.nightly.latest.rustfmt)
            cargo-criterion
            cargo-deny
            cargo-edit
            cargo-flamegraph
            cargo-fuzz
            cargo-watch
            rust-toolchain
            unstable.irust

            # Wasm
            unstable.binaryen
            unstable.wasm-pack

            # JS
            chromedriver
            unstable.deno
            nodejs_22 # Current LTS
            pkgs.yarn

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
            
            # Commands
            command_menu
          ]
          ++ format-pkgs
          ++ cargo-installs;

        shellHook = "menu";
      };

      formatter = pkgs.alejandra;
    }
  );
}
