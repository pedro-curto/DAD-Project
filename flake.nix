{
  inputs = {
    nixpkgs.url = "flake:nixpkgs";
    flake-parts = {
      url = "github:hercules-ci/flake-parts";
      inputs.nixpkgs-lib.follows = "nixpkgs";
    };
    pre-commit-hooks-nix = {
      url = "github:cachix/pre-commit-hooks.nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.nixpkgs-stable.follows = "nixpkgs";
    };
    treefmt-nix = {
      url = "github:numtide/treefmt-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };

  };
  outputs = inputs@{ flake-parts, ... }:
    flake-parts.lib.mkFlake { inherit inputs; } {
      imports = [
        # To import a flake module
        # 1. Add foo to inputs
        # 2. Add foo as a parameter to the outputs function
        # 3. Add here: foo.flakeModule
        inputs.pre-commit-hooks-nix.flakeModule
        # inputs.devenv.flakeModule
        inputs.treefmt-nix.flakeModule

      ];
      flake = {
        # Put your original flake attributes here.
      };
      #systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" "x86_64-darwin" ];
      systems = [
        # systems for which you want to build the `perSystem` attributes
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
      ];
      # perSystem = { config, self', inputs', pkgs, system, ... }: {
      perSystem = { config, pkgs, ... }: {
        # Per-system attributes can be defined here. The self' and inputs'
        # module parameters provide easy access to attributes of the same
        # system.

        # https://tecnico-distsys.github.io/software/index.html
        devShells.default =
          let
            mavenPkg = pkgs.maven;
            javaPkg = pkgs.jdk22;
            patchelf = "${pkgs.patchelf}/bin/patchelf";
            patchProtoc = pkgs.writeShellScriptBin "patch-Protoc" ''
              set -ux
              echo "Running mvn install..."
              mvn install > /dev/null
              ${patchelf} --set-interpreter "$(cat $NIX_CC/nix-support/dynamic-linker)" target/protoc-plugins/protoc-3.12.0-linux-x86_64.exe
              echo "Running mvn install..."
              mvn install > /dev/null
              ${patchelf} --set-interpreter "$(cat $NIX_CC/nix-support/dynamic-linker)" target/protoc-plugins/protoc-gen-grpc-java-1.36.0-linux-x86_64.exe
              echo "Running mvn install..."
              mvn install > /dev/null
            '';
          in
          pkgs.mkShell {
            #Add executable packages to the nix-shell environment.
            M2_HOME = "${mavenPkg}";
            JAVA_HOME = "${javaPkg}";
            packages = with pkgs; [
              javaPkg
              mavenPkg
              git

              #Utils
              patchProtoc
              commitizen

              grpc-tools
              protobuf
              (pkgs.python3.withPackages (python-pkgs: [
                python-pkgs.grpcio
                python-pkgs.grpcio-tools
              ]))
              ruff
              black
            ];

            shellHook = ''
              # export DEBUG=1
              ${config.pre-commit.installationScript}
            '';
          };
        pre-commit = {
          check.enable = false;
          settings.hooks = {
            actionlint.enable = true;
            treefmt.enable = false;
            commitizen = {
              enable = true;
              description = "Check whether the current commit message follows commiting rules. Allow empty commit messages by default, because they typically indicate to Git that the commit should be aborted.";
              entry = "${pkgs.commitizen}/bin/cz check --commit-msg-file";
              stages = [ "commit-msg" ];
            };
          };
        };
        treefmt.projectRootFile = ./flake.nix;
        treefmt.programs = {
          actionlint.enable = true;
          nixpkgs-fmt.enable = false;
          shfmt.enable = true;
          mdformat.enable = true;
          google-java-format.enable = true;
          deadnix.enable = true;
          statix.enable = true;
          ruff-check.enable = true;
          ruff-format.enable = true;
        };
        treefmt.settings.formatter.google-java-format.options = [ "--aosp" ];
      };
    };
}
