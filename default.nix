{ python ? "python3.9" }:
let
  pkgs = import ./nix;
  drv =
    { poetry2nix
    , python
    , lib
    , fetchFromGitHub
    }:

    let
      testingData = fetchFromGitHub {
        owner = "ibis-project";
        repo = "testing-data";
        rev = "master";
        sha256 = "1lm66g5kvisxsjf1jwayiyxl2d3dhlmxj13ijrya3pfg07mq9r66";
      };
    in
    poetry2nix.mkPoetryApplication {
      inherit python;

      pyproject = ./pyproject.toml;
      poetrylock = ./poetry.lock;
      src = lib.cleanSource ./.;

      overrides = pkgs.poetry2nix.overrides.withDefaults (
        import ./poetry-overrides.nix { inherit pkgs; }
      );

      preConfigure = ''
        rm -f setup.py
      '';

      buildInputs = with pkgs; [ graphviz ];
      checkInputs = with pkgs; [ graphviz ];

      checkPhase = ''
        runHook preCheck

        function min() { [ "$1" -lt "$2" ] && echo "$1" || echo "$2"; }

        tempdir="$(mktemp -d)"

        cp -r ${testingData}/* "$tempdir"

        chmod -R u+rwx "$tempdir"

        ln -s "$tempdir" ci/ibis-testing-data

        for backend in csv dask hdf5 pandas parquet sqlite; do
          python ci/datamgr.py "$backend" &
        done
        wait

        nproc="$(min 8 "$(nproc)")"

        pytest ibis/{tests,backends/{csv,dask,hdf5,pandas,parquet,sqlite}} -n "$nproc" -k 'not test_import_time'
        pytest ibis/backends/tests -m 'csv or dask or hdf5 or pandas or parquet or sqlite' -n "$nproc"

        runHook postCheck
      '';

      pythonImportsCheck = [ "ibis" ];
    };
in
pkgs.callPackage drv {
  python = pkgs.${builtins.replaceStrings [ "." ] [ "" ] python};
}
