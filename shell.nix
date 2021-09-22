{ python ? "3.9" }:
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
        inherit (pkgs) lib stdenv;
      }
    );
  };
  shellHook = ''
    ${(import ./pre-commit.nix).pre-commit-check.shellHook}
    mkdir -p ci/ibis-testing-data
    chmod u+rwx ci/ibis-testing-data
    cp -rf ${pkgs.ibisTestingData}/* ci/ibis-testing-data
    chmod --recursive u+rw ci/ibis-testing-data
  '';

  devInputs = (with pkgs; [
    cacert
    cachix
    commitizen
    commitlint
    conda
    git
    niv
    nix-linter
    nixpkgs-fmt
    poetry
    shellcheck
    shfmt
  ]) ++ [ prettier ];

  libraryDevInputs = with pkgs; [
    boost
    clang_12
    cmake
    graphviz
    openjdk11
    pandoc
    postgresql
    sqlite-interactive
  ];

  commonBuildInputs = devInputs ++ libraryDevInputs;

  pythonName = "python${builtins.replaceStrings [ "." ] [ "" ] python}";
in
pkgs.mkShell {
  name = "ibis-${pythonName}";

  inherit shellHook;

  buildInputs = commonBuildInputs ++ [
    (mkPoetryEnv pkgs.${pythonName})
  ];

  PYTHONPATH = builtins.toPath ./.;
}
