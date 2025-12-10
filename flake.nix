{
  description = "Description for the project";

  inputs = {
    flake-parts.url = "github:hercules-ci/flake-parts";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    devshell.url = "github:numtide/devshell";
  };

  outputs = inputs @ {flake-parts, ...}:
    flake-parts.lib.mkFlake {inherit inputs;} {
      imports = [inputs.devshell.flakeModule];
      systems = ["x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin"];
      perSystem = {
        config,
        self',
        inputs',
        pkgs,
        system,
        ...
      }: {
        devshells.default = {
          packages = with pkgs; [
            jdk17_headless
          ];
          env = [
            {
              name = "LD_LIBRARY_PATH";
              value = pkgs.lib.makeLibraryPath [pkgs.stdenv.cc.cc.lib];
            }
          ];
        };
      };
      flake = {};
    };
}
