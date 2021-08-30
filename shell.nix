let
  pkgs = import ./nix;
  inherit (pkgs) lib;
  prettier = pkgs.writeShellScriptBin "prettier" ''
    ${pkgs.nodePackages.prettier}/bin/prettier \
    --plugin-search-dir "${pkgs.nodePackages.prettier-plugin-toml}/lib" \
    "$@"
  '';
  mkPoetryEnv = python: pkgs.poetry2nix.mkPoetryEnv {
    inherit python;
    projectDir = ./.;
    editablePackageSources = {
      ibis = ./ibis;
    };
    overrides = pkgs.poetry2nix.overrides.withDefaults (
      import ./poetry-overrides.nix {
        inherit pkgs;
      }
    );
  };
  shellHook = ''
    ${(import ./pre-commit.nix).pre-commit-check.shellHook}
  '';
  commonBuildInputs = with pkgs; [
    cachix
    commitizen
    commitlint
    git
    niv
    nix-linter
    nixpkgs-fmt
    poetry
    prettier
    rlwrap
    shellcheck
    shfmt
    sqlite
  ];

  ibisConfig = builtins.fromTOML (lib.readFile ./pyproject.toml);

  pythonVersions = ibisConfig.tool.ibis.supported_python_versions;

  conda-shell-run = pkgs.writeShellScriptBin "conda-shell-run" ''
    set -euo pipefail

    ${pkgs.conda}/bin/conda-shell -c "$*"
  '';
in
{
  commitlint = pkgs.mkShell {
    name = "commitlint";
    buildInputs = with pkgs; [ commitlint ];
  };

  build = pkgs.mkShell {
    name = "ibis-build";
    inherit shellHook;
    buildInputs = commonBuildInputs;
  };
} // pkgs.lib.listToAttrs (
  map
    (version:
      let
        name = "python${builtins.replaceStrings [ "." ] [ "" ] version}";
      in
      {
        inherit name;
        value = pkgs.mkShell {
          name = "ibis-${name}";
          inherit shellHook;
          PYTHONPATH = builtins.toPath ./.;
          buildInputs = commonBuildInputs ++ [
            (mkPoetryEnv pkgs.${name})
            conda-shell-run
          ] ++ (with pkgs; [
            cacert
            conda
            graphviz
            openjdk11
            pandoc
          ]);
        };
      })
    pythonVersions
)
