{ pkgs ? import <nixpkgs> {} }:
let
  nativeBuildInputs = with pkgs; [
    rustup openssl pkg-config libiconv
    libclang rustPlatform.bindgenHook gcc13
    libcxx gnumake autoconf automake libtool cmake
    stdenv.cc.cc.lib
    uv maturin
  ];

in
pkgs.mkShell {
  inherit nativeBuildInputs;

  preBuild = ''
    export BINDGEN_EXTRA_CLANG_ARGS=''${pkgs.lib.concatStringsSep " " [
      "$(cat ${pkgs.stdenv.cc}/nix-support/libc-crt1-cflags)"
      "$(cat ${pkgs.stdenv.cc}/nix-support/libc-cflags)"
      "$(cat ${pkgs.stdenv.cc}/nix-support/cc-cflags)"
      "$(cat ${pkgs.stdenv.cc}/nix-support/libcxx-cxxflags)"
      "${pkgs.lib.optionalString pkgs.stdenv.cc.isClang "-idirafter ${pkgs.stdenv.cc.cc}/lib/clang/${pkgs.lib.getVersion pkgs.stdenv.cc.cc}/include"}"
      "${pkgs.lib.optionalString pkgs.stdenv.cc.isGNU "-isystem ${pkgs.stdenv.cc.cc}/include/c++/${pkgs.lib.getVersion pkgs.stdenv.cc.cc} -isystem ${pkgs.stdenv.cc.cc}/include/c++/${pkgs.lib.getVersion pkgs.stdenv.cc.cc}/${pkgs.stdenv.hostPlatform.config} -idirafter ${pkgs.stdenv.cc.cc}/lib/gcc/${pkgs.stdenv.hostPlatform.config}/${pkgs.lib.getVersion pkgs.stdenv.cc.cc}/include"}"
    ]}
  '';

  LIBC_INCLUDE_DIR="${pkgs.libclang.lib}/lib/clang/${pkgs.lib.getVersion pkgs.libclang}/include";
  LIBCXX_INCLUDE_DIR="${pkgs.libcxx.dev}/include";
  LIBCLANG_PATH = "${pkgs.libclang.lib}/lib";
  RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
  LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath nativeBuildInputs;

  shellHook = ''
    set -e
    uv venv .venv --python=3.12
  '';
}
