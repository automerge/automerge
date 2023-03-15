{
  description = "automerge";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem
    (system: let
      pkgs = import nixpkgs {
        overlays = [rust-overlay.overlays.default];
        inherit system;
      };
      rust = pkgs.rust-bin.stable.latest.default;
    in {
      formatter = pkgs.alejandra;

      packages = {
        deadnix = pkgs.runCommand "deadnix" {} ''
          ${pkgs.deadnix}/bin/deadnix --fail ${./.}
          mkdir $out
        '';
      };

      checks = {
        inherit (self.packages.${system}) deadnix;
      };

      devShells.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          (rust.override {
            extensions = ["rust-src"];
            targets = ["wasm32-unknown-unknown"];
          })
          cargo-edit
          cargo-watch
          cargo-criterion
          cargo-fuzz
          cargo-flamegraph
          cargo-deny
          crate2nix
          wasm-pack
          pkgconfig
          openssl
          gnuplot

          nodejs
          yarn
          deno

          # c deps
          cmake
          cmocka
          doxygen

          rnix-lsp
          nixpkgs-fmt
        ];
      };
    });
}
