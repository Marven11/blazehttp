{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { nixpkgs, ... }:
    let
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.buildGoModule {
            pname = "blazehttp";
            version = "0.3.1-dev";
            src = ./.;
            vendorHash = "sha256-HlAz+KNTdbyQ0n7ych1OpqTCxgeqTkJU9/MMYeGD0/Y=";
            subPackages = [ "cmd/blazehttp" ];
            env.CGO_ENABLED = "0";
            ldflags = [
              "-s"
              "-w"
            ];
            meta.mainProgram = "blazehttp";
          };
        });
    };
}
