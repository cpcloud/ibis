{ uv2nix, pyproject-nix }: pkgs: super:
let
  # Create package overlay from workspace.
  workspace = uv2nix.lib.workspace.loadWorkspace { workspaceRoot = ../.; };
  envOverlay = workspace.mkPyprojectOverlay {
    sourcePreference = "wheel";
  };
  project = pyproject-nix.lib.project.loadPyproject {
    # Read & unmarshal pyproject.toml relative to this project root.
    # projectRoot is also used to set `src` for renderers such as buildPythonPackage.
    projectRoot = ../.;
  };

  pyprojectOverrides = import ./pyproject-overrides.nix { inherit pkgs; };

  mkDevEnv = python: { groups, extras }:
    # This devShell uses uv2nix to construct a virtual environment purely from Nix, using the same dependency specification as the application.
    # The notable difference is that we also apply another overlay here enabling editable mode ( https://setuptools.pypa.io/en/latest/userguide/development_mode.html ).
    #
    # This means that any changes done to your local files do not require a rebuild.
    let
      # Construct package set
      pythonSet =
        # Use base package set from pyproject.nix builders
        (pkgs.callPackage pyproject-nix.build.packages {
          inherit python;
        }).overrideScope
          (pkgs.lib.composeExtensions envOverlay pyprojectOverrides);

      # Create an overlay enabling editable mode for all local dependencies.
      editableOverlay = workspace.mkEditablePyprojectOverlay {
        root = "$REPO_ROOT";
      };

      # Override previous set with our overridable overlay.
      editablePythonSet = pythonSet.overrideScope editableOverlay;
      allDeps = builtins.attrNames project.dependencies.groups
        ++ builtins.attrNames project.dependencies.extras;

      depFilter = set: if set == "*" then (_: true) else (name: pkgs.lib.elem name set);
      groupFilter = depFilter groups;
      extraFilter = depFilter extras;
    in

    # Build virtual environment
    editablePythonSet.mkVirtualEnv "ibis-${python.pythonVersion}" {
      ibis-framework = builtins.filter groupFilter allDeps
        ++ builtins.filter extraFilter allDeps;
    };
  inherit (pkgs) lib stdenv;
in
{
  ibisTestingData = pkgs.fetchFromGitHub {
    name = "ibis-testing-data";
    owner = "ibis-project";
    repo = "testing-data";
    rev = "b26bd40cf29004372319df620c4bbe41420bb6f8";
    sha256 = "sha256-1fenQNQB+Q0pbb0cbK2S/UIwZDE4PXXG15MH3aVbyLU=";
  };

  ibis310 = pkgs.callPackage ./ibis.nix {
    python = pkgs.python310;
    inherit mkDevEnv;
  };

  ibis311 = pkgs.callPackage ./ibis.nix {
    python = pkgs.python311;
    inherit mkDevEnv;
  };

  ibis312 = pkgs.callPackage ./ibis.nix {
    python = pkgs.python312;
    inherit mkDevEnv;
  };

  ibisDevEnv310 = mkDevEnv pkgs.python310 {
    groups = "*";
    extras = "*";
  };
  ibisDevEnv311 = mkDevEnv pkgs.python311 {
    groups = "*";
    extras = "*";
  };
  ibisDevEnv312 = mkDevEnv pkgs.python312 {
    groups = "*";
    extras = "*";
  };

  ibisSmallDevEnv = mkDevEnv pkgs.python312 { groups = [ "dev" ]; extras = [ ]; };

  duckdb = super.duckdb.overrideAttrs (
    _: lib.optionalAttrs (stdenv.isAarch64 && stdenv.isLinux) {
      doInstallCheck = false;
    }
  );

  quarto = pkgs.callPackage ./quarto { };

  changelog = pkgs.writeShellApplication {
    name = "changelog";
    runtimeInputs = [ pkgs.nodejs_20.pkgs.conventional-changelog-cli ];
    text = ''
      conventional-changelog --config ./.conventionalcommits.js "$@"
    '';
  };

  check-release-notes-spelling = pkgs.writeShellApplication {
    name = "check-release-notes-spelling";
    runtimeInputs = [ pkgs.changelog pkgs.coreutils pkgs.codespell ];
    text = ''
      tmp="$(mktemp)"
      changelog --release-count 1 --output-unreleased --outfile "$tmp"
      if ! codespell "$tmp"; then
        # cat -n to output line numbers
        cat -n "$tmp"
        exit 1
      fi
    '';
  };

  update-lock-files = pkgs.writeShellApplication {
    name = "update-lock-files";
    runtimeInputs = with pkgs; [ just uv ];
    text = "just lock";
  };

  gen-examples = pkgs.writeShellApplication {
    name = "gen-examples";
    runtimeInputs = [
      pkgs.ibisDevEnv312
      (pkgs.rWrapper.override {
        packages = with pkgs.rPackages; [
          Lahman
          janitor
          palmerpenguins
          stringr
          tidyverse
        ];
      })
      pkgs.google-cloud-sdk
    ];

    text = ''
      python "$PWD/ibis/examples/gen_registry.py" "''${@}"
    '';
  };
}
