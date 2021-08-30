{ pkgs }:
self: super: {
  # see https://github.com/numpy/numpy/issues/19624 for details
  numpy = super.numpy.overridePythonAttrs (attrs: {
    patches = (attrs.patches or [ ])
      ++ pkgs.lib.optionals
      # this patch only applies to amcos and only with numpy versions >=1.21,<1.21.2
      (pkgs.stdenv.isDarwin && (self.numpy.version == "1.21.0" || self.numpy.version == "1.21.1"))
      [
        (pkgs.fetchpatch {
          url = "https://github.com/numpy/numpy/commit/8045183084042fbafc995dd26eb4d9ca45eb630e.patch";
          sha256 = "14g69vq7llkh6smpfrb50iqga7fd360dkcc0rlwb5k2cz8bsii5b";
        })
      ];
  });

  pyspark = super.pyspark.overridePythonAttrs (_: {
    postPatch = ''
      sed -i "s/'pypandoc'//" setup.py

      substituteInPlace setup.py --replace py4j==0.10.9 'py4j>=0.10.9,<0.11'
    '';
  });

  sphinx_rtd_theme = super.sphinx_rtd_theme.overridePythonAttrs (attrs: {
    nativeBuildInputs = (attrs.nativeBuildInputs or [ ])
      ++ [ pkgs.nodejs ];
    doCheck = false;
  });

  sphinx-rtd-theme = self.sphinx_rtd_theme;

  ipykernel = super.ipykernel.overridePythonAttrs (attrs: {
    propagatedBuildInputs = (attrs.propagatedBuildInputs or [ ])
      ++ [ self.ipython-genutils ];
  });

  watchdog = super.watchdog.overrideAttrs (attrs: {
    disabledTests = (attrs.disabledTests or [ ]) ++ [
      "test_move_to"
      "test_move_internal"
      "test_close_should_terminate_thread"
    ];
  });

  pykerberos = super.pykerberos.overridePythonAttrs (attrs: {
    nativeBuildInputs = (attrs.nativeBuildInputs or [ ])
      ++ [ pkgs.krb5 ];
  });

  kerberos = self.pykerberos;

  tables = super.tables.overridePythonAttrs (_: {
    HDF5_DIR = "${pkgs.hdf5}";
    format = "setuptools";

    buildInputs = with pkgs; [ bzip2 c-blosc lzo hdf5 ];
    nativeBuildInputs = [ self.cython pkgs.pkg-config ];
    propagatedBuildInputs = [ self.numpy self.numexpr self.setuptools pkgs.hdf5 ];

    preBuild = ''
      make distclean
    '';

    postPatch = ''
      substituteInPlace Makefile --replace "src doc" "src"
      # Force test suite to error when unittest runner fails
      substituteInPlace tables/tests/test_suite.py \
        --replace "return 0" "assert result.wasSuccessful(); return 0" \
        --replace "return 1" "assert result.wasSuccessful(); return 1"
    '';
    setupPyBuildFlags = with pkgs; [
      "--hdf5=${lib.getDev hdf5}"
      "--lzo=${lib.getDev lzo}"
      "--bzip2=${lib.getDev bzip2}"
      "--blosc=${lib.getDev c-blosc}"
    ];

    pythonImportsCheck = [ "tables" ];
  });
}
