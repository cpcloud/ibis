{
  stdenv,
  lib,
  esbuild,
  fetchurl,
  dart-sass,
  makeWrapper,
  rWrapper,
  rPackages,
  autoPatchelfHook,
  libgcc,
  which,
}:

let
  platforms = rec {
    x86_64-linux = "linux-amd64";
    aarch64-linux = "linux-arm64";
    aarch64-darwin = "macos";
    x86_64-darwin = aarch64-darwin;
  };
  shas = rec {
    x86_64-linux = "sha256-daXUWf7yRnzPMxgvparvox7aqqv+xTneEMSsL+oZTyM=";
    aarch64-linux = "sha256-cuyEMv40+99yNGjhjAASvWweaqhdmnaq9ycCIkg52Hw=";
    aarch64-darwin = "sha256-DPIBZDV0pkfpvKty+YjrGegf3p92Xk4huh66VVK2cws=";
    # hashes are the same for both macos architectures, because the packages
    # are identical
    x86_64-darwin = aarch64-darwin;
  };
  inherit (stdenv.hostPlatform) system;
in
stdenv.mkDerivation rec {
  pname = "quarto";
  version = "1.8.17";
  src = fetchurl {
    url = "https://github.com/quarto-dev/quarto-cli/releases/download/v${version}/quarto-${version}-${platforms.${system}}.tar.gz";
    sha256 = shas.${system};
  };

  preUnpack = lib.optionalString stdenv.isDarwin "mkdir ${sourceRoot}";
  sourceRoot = lib.optionalString stdenv.isDarwin "quarto-${version}";
  unpackCmd = lib.optionalString stdenv.isDarwin "tar xzf $curSrc --directory=$sourceRoot";

  nativeBuildInputs = lib.optionals stdenv.isLinux [ autoPatchelfHook ] ++ [
    makeWrapper
    libgcc
  ];

  preFixup =
    let
      rEnv = rWrapper.override {
        packages = with rPackages; [
          dplyr
          reticulate
          rmarkdown
          tidyr
        ];
      };
    in
    ''
      wrapProgram $out/bin/quarto \
        --prefix QUARTO_ESBUILD : ${lib.getExe esbuild} \
        --prefix QUARTO_R : ${lib.getExe' rEnv "R"} \
        --prefix QUARTO_DART_SASS : ${lib.getExe dart-sass} \
        --prefix PATH : ${lib.makeBinPath [ which ]}
    '';

  installPhase = ''
    runHook preInstall

    mkdir -p $out/bin $out/share

    mv bin/* $out/bin
    mv share/* $out/share

    runHook postInstall
  '';

  meta = with lib; {
    description = "Open-source scientific and technical publishing system built on Pandoc";
    longDescription = ''
      Quarto is an open-source scientific and technical publishing system built on Pandoc.
      Quarto documents are authored using markdown, an easy to write plain text format.
    '';
    homepage = "https://quarto.org/";
    changelog = "https://github.com/quarto-dev/quarto-cli/releases/tag/v${version}";
    license = licenses.gpl2Plus;
    platforms = builtins.attrNames platforms;
    sourceProvenance = with sourceTypes; [
      binaryNativeCode
      binaryBytecode
    ];
  };
}
