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
          lib = pkgs.lib;
          rust = pkgs.rust-bin.nightly.latest.rust;
          cargoNix = pkgs.callPackage ./Cargo.nix {
            inherit pkgs;
            release = true;
          };
          debugCargoNix = pkgs.callPackage ./Cargo.nix {
            inherit pkgs;
            release = false;
          };
        in
        {
          packages = lib.attrsets.mapAttrs
            (name: value: value.build)
            cargoNix.workspaceMembers;

          defaultPackage = self.packages.${system}.automerge;

          apps = {
            automerge-cli = flake-utils.lib.mkApp {
              name = "automerge";
              drv = self.packages.${system}.automerge-cli;
            };
          };

          checks = lib.attrsets.mapAttrs
            (name: value: value.build.override {
              runTests = true;
            })
            debugCargoNix.workspaceMembers //
          {
            automerge-cli =
              cargoNix.workspaceMembers.automerge-cli.build.override {
                # FIXME(jeffas): issues with 'environment variable
                # `CARGO_BIN_EXE_automerge` not defined'
                runTests = false;
              };

            automerge-fuzz =
              cargoNix.workspaceMembers.automerge-fuzz.build.override {
                # FIXME(jeffas): tests shouldn't be run directly but invoked
                # with cargo fuzz, I thought test=false in the Cargo.toml for
                # this would have some effect but clearly not. Ideally fix this
                # in the Cargo.toml or in crate2nix.
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
                mdbook

                nodejs
                yarn

                rnix-lsp
                nixpkgs-fmt
              ];
          };
        });
}
