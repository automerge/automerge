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
    flake-utils.lib.eachDefaultSystem (system:
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
        };

        defaultPackage = self.packages.${system}.automerge;

        devShell = pkgs.mkShell {
          buildInputs = with pkgs; [
            (rust.override {
              extensions = [ "rust-src" ];
              targets = [ "wasm32-unknown-unknown" ];
            })
            cargo-edit
            cargo-watch
            cargo-criterion
            crate2nix
            wasm-pack
            pkgconfig
            openssl

            nodejs
            yarn

            rnix-lsp
            nixpkgs-fmt
          ];
        };
      });
}
