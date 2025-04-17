{ pkgs ? import <nixpkgs> {} }:
let
  nativeBuildInputs = with pkgs; [
    psmisc
    openssl pkg-config
    stdenv.cc.cc.lib
    uv
  ];

in
pkgs.mkShell {
  inherit nativeBuildInputs;

  LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath nativeBuildInputs;
  TMPDIR = "/tmp";

  shellHook = ''
    set -e
    uv venv .venv --python=3.11
  '';
}
