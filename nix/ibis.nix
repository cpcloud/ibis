{ pkgs, python, mkDevEnv, ... }:
let
  testEnv = mkDevEnv python {
    groups = [ "tests" ];
    extras = [
      "duckdb"
      "datafusion"
      "sqlite"
      "polars"
      "decompiler"
      "visualization"
    ];
  };
in
python.pkgs.buildPythonPackage {
  name = "ibis-framework";
  inherit (testEnv) buildInputs
    nativeBuildInputs
    propagatedBuildInputs
    propagatedNativeBuildInputs;
  nativeCheckInputs = testEnv.buildInputs ++ [ pkgs.graphviz-nox ];
  pyproject = true;
  src = ../.;
  preCheck = ''
    ln -s ${pkgs.ibisTestingData} $PWD/ci/ibis-testing-data
  '';
  checkPhase = ''
    runHook preCheck
    pytest -m datafusion
    pytest -m 'core or duckdb or sqlite or polars' --numprocesses $NIX_BUILD_CORES --dist loadgroup
    runHook postCheck
  '';
}
