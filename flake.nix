{
  description = "KONUS Hidden Markov Models";

  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs?ref=nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          packages = [
            pkgs.python313
            pkgs.uv
          ];

          shellHook = ''
            export UV_PYTHON_PREFERENCE=only-system
            export UV_PYTHON=$(which python3)
          '';
        };
      }
    );
}
