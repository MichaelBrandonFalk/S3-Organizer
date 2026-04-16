#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$ROOT_DIR/.build-x86-tcltk"
PREFIX="$ROOT_DIR/.x86-tcltk"
TCL_VERSION="${TCL_VERSION:-8.6.15}"
TK_VERSION="${TK_VERSION:-8.6.15}"
TCL_ARCHIVE="tcl${TCL_VERSION}-src.tar.gz"
TK_ARCHIVE="tk${TK_VERSION}-src.tar.gz"
TCL_URL="https://prdownloads.sourceforge.net/tcl/${TCL_ARCHIVE}"
TK_URL="https://prdownloads.sourceforge.net/tcl/${TK_ARCHIVE}"

mkdir -p "$BUILD_DIR" "$PREFIX"
cd "$BUILD_DIR"

if [[ ! -f "$TCL_ARCHIVE" ]]; then
  curl -L -o "$TCL_ARCHIVE" "$TCL_URL"
fi

if [[ ! -f "$TK_ARCHIVE" ]]; then
  curl -L -o "$TK_ARCHIVE" "$TK_URL"
fi

rm -rf "tcl${TCL_VERSION}" "tk${TK_VERSION}"
tar -xzf "$TCL_ARCHIVE"
tar -xzf "$TK_ARCHIVE"

cd "$BUILD_DIR/tcl${TCL_VERSION}/unix"
arch -x86_64 ./configure \
  --prefix="$PREFIX" \
  --enable-shared \
  --disable-corefoundation
arch -x86_64 make -j4
arch -x86_64 make install

cd "$BUILD_DIR/tk${TK_VERSION}/macosx"
arch -x86_64 make -f GNUmakefile install-deploy \
  PREFIX="$PREFIX" \
  INSTALL_PATH="$PREFIX/Library/Frameworks" \
  APPLICATION_INSTALL_PATH="$PREFIX/Applications" \
  TCL_BUILD_DIR="$BUILD_DIR/tcl${TCL_VERSION}/unix" \
  TCL_INSTALL_PATH="$PREFIX/Library/Frameworks/Tcl.framework"

echo "Built x86 Tcl/Tk into $PREFIX"
