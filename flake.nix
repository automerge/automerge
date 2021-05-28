{
  description = "automerge-rs";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils = {
      url = "github:numtide/flake-utils";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay }:
    flake-utils.lib.eachDefaultSystem
      (system:
        let
          pkgs = import nixpkgs {
            overlays = [ rust-overlay.overlay ];
            inherit system;
          };
          rust = pkgs.rust-bin.nightly.latest.rust;
          cargoNix = pkgs.callPackage ./Cargo.nix { };
        in
        {
          packages = {
            automerge = cargoNix.workspaceMembers.automerge.build;
            automerge-protocol = cargoNix.workspaceMembers.automerge-protocol.build;
            automerge-backend = cargoNix.workspaceMembers.automerge-backend.build;
            automerge-backend-wasm = cargoNix.workspaceMembers.automerge-backend-wasm.build;
            automerge-frontend = cargoNix.workspaceMembers.automerge-frontend.build;
            automerge-c = cargoNix.workspaceMembers.automerge-c.build;
            automerge-cli = cargoNix.workspaceMembers.automerge-cli.build;
          };

          defaultPackage = self.packages.${system}.automerge;

          apps = {
            automerge-cli = flake-utils.lib.mkApp {
              name = "automerge";
              drv = self.packages.${system}.automerge-cli;
            };
          };

          checks = {
            automerge = cargoNix.workspaceMembers.automerge.build.override {
              runTests = true;
            };
            automerge-protocol = cargoNix.workspaceMembers.automerge-protocol.build.override {
              runTests = true;
            };
            automerge-backend = cargoNix.workspaceMembers.automerge-backend.build.override {
              runTests = true;
            };
            automerge-backend-wasm = cargoNix.workspaceMembers.automerge-backend-wasm.build.override {
              runTests = true;
            };
            automerge-frontend = cargoNix.workspaceMembers.automerge-frontend.build.override {
              runTests = true;
            };
            automerge-c = cargoNix.workspaceMembers.automerge-c.build.override {
              runTests = true;
            };
            automerge-cli = cargoNix.workspaceMembers.automerge-cli.build.override {
              # FIXME(jeffas): issues with 'environment variable `CARGO_BIN_EXE_automerge` not defined'
              runTests = false;
            };

            format = pkgs.runCommand "format"
              {
                src = ./.;
                buildInputs = [ rust ];
              } ''
              mkdir $out
              cd $src
              cargo fmt -- --check
            '';
          };

          devShell = pkgs.mkShell {
            buildInputs = with pkgs;
              [
                (rust.override {
                  extensions = [ "rust-src" ];
                  targets = [ "wasm32-unknown-unknown" ];
                })
                cargo-edit
                cargo-watch
                cargo-criterion
                cargo-fuzz
                cargo-flamegraph
                crate2nix
                wasm-pack
                pkgconfig
                openssl
                valgrind

                nodejs
                yarn

                rnix-lsp
                nixpkgs-fmt
              ];
          };
        });
}
