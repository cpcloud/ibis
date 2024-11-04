{ pkgs }: final: prev:
let
  buildSystemOverrides = {
    tomli.flit-core = [ ];
    parsy.setuptools = [ ];
    atpublic.hatchling = [ ];
    toolz.setuptools = [ ];
    typing-extensions.flit-core = [ ];
    sqlglot = {
      setuptools = [ ];
      setuptools-scm = [ ];
    };
    pathspec.flit-core = [ ];
    packaging.flit-core = [ ];
    pluggy = {
      setuptools = [ ];
      setuptools-scm = [ ];
    };
    pydruid.setuptools = [ ];
    pytest-clarity.setuptools = [ ];
    pure-sasl.setuptools = [ ];
  } // pkgs.lib.optionalAttrs pkgs.stdenv.isDarwin {
    duckdb = {
      setuptools = [ ];
      setuptools-scm = [ ];
      pybind11 = [ ];
    };
  };
  inherit (final) resolveBuildSystem;
  addBuildSystems =
    pkg: spec:
    pkg.overrideAttrs (old: {
      nativeBuildInputs = old.nativeBuildInputs ++ resolveBuildSystem spec;
    });
in
pkgs.lib.mapAttrs (name: spec: addBuildSystems prev.${name} spec) buildSystemOverrides // {
  mysqlclient = prev.mysqlclient.overrideAttrs (attrs: {
    nativeBuildInputs = attrs.nativeBuildInputs or [ ] ++ [
      prev.setuptools
      pkgs.pkg-config
      pkgs.libmysqlclient
    ];
  });

  psycopg2 = prev.psycopg2.overrideAttrs (attrs: {
    nativeBuildInputs = attrs.nativeBuildInputs or [ ] ++ [
      prev.setuptools
      pkgs.postgresql
    ];
  });

  pyodbc = prev.pyodbc.overrideAttrs (attrs: {
    nativeBuildInputs = attrs.nativeBuildInputs or [ ] ++ [
      pkgs.unixODBC
    ];
  });

  pyspark = prev.pyspark.overrideAttrs (attrs:
    let
      pysparkVersion = pkgs.lib.versions.majorMinor attrs.version;
      jarHashes = {
        "3.5" = "sha256-h+cYTzHvDKrEFbvfzxvElDNGpYuY10fcg0NPcTnhKss=";
        "3.3" = "sha256-3D++9VCiLoMP7jPvdCtBn7xnxqHnyQowcqdGUe0M3mk=";
      };
      icebergVersion = "1.6.1";
      scalaVersion = "2.12";
      jarName = "iceberg-spark-runtime-${pysparkVersion}_${scalaVersion}-${icebergVersion}.jar";
      icebergJarUrl = "https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-${pysparkVersion}_${scalaVersion}/${icebergVersion}/${jarName}";
      icebergJar = pkgs.fetchurl {
        name = jarName;
        url = icebergJarUrl;
        sha256 = jarHashes."${pysparkVersion}";
      };
    in
    {
      nativeBuildInputs = attrs.nativeBuildInputs or [ ] ++ [ final.setuptools ];
      postInstall = attrs.postInstall or "" + ''
        cp ${icebergJar} $out/${final.python.sitePackages}/pyspark/jars/${icebergJar.name}
      '';
    });

  thrift = prev.thrift.overrideAttrs (attrs: {
    nativeBuildInputs = attrs.nativeBuildInputs or [ ] ++ [ final.setuptools ];
    # avoid extremely premature optimization so that we don't have to
    # deal with a useless dependency on distutils
    postPatch = ''
      substituteInPlace setup.cfg --replace 'optimize = 1' 'optimize = 0'
    '';
  });
}
