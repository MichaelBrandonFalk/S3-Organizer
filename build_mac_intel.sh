#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYENV_ROOT_X86="$ROOT_DIR/.pyenv-x86"
PYTHON_VERSION_X86="${PYTHON_VERSION_X86:-3.12.9}"
PYTHON_X86="$PYENV_ROOT_X86/versions/$PYTHON_VERSION_X86/bin/python3"
VENV_X86="$ROOT_DIR/.venv-x86"
PYINSTALLER="$VENV_X86/bin/pyinstaller"
ICON_PATH="$ROOT_DIR/s3organizer.icns"

if [[ ! -x "$PYTHON_X86" ]]; then
  echo "Intel Python not found at:"
  echo "  $PYTHON_X86"
  echo
  echo "Build it first with Rosetta, for example:"
  echo "  PYENV_ROOT=$PYENV_ROOT_X86 PYTHON_BUILD_SKIP_HOMEBREW=1 MAKE_INSTALL_OPTS=-j1 arch -x86_64 pyenv install $PYTHON_VERSION_X86"
  exit 1
fi

if [[ ! -d "$VENV_X86" ]]; then
  arch -x86_64 "$PYTHON_X86" -m venv "$VENV_X86"
fi

arch -x86_64 "$VENV_X86/bin/pip" install -r "$ROOT_DIR/s3_copy_desktop_app/requirements.txt" pyinstaller

cd "$ROOT_DIR"

ICON_ARGS=()
if [[ -f "$ICON_PATH" ]]; then
  ICON_ARGS=(--icon "$ICON_PATH")
fi

arch -x86_64 "$PYINSTALLER" \
  --noconfirm \
  --windowed \
  --name "s3Organizer" \
  --collect-submodules keyring.backends \
  --collect-data keyring \
  --collect-data certifi \
  --hidden-import tkinter \
  "${ICON_ARGS[@]}" \
  s3_copy_desktop_app_launcher.py

echo "Build complete: $ROOT_DIR/dist/s3Organizer.app"
file "$ROOT_DIR/dist/s3Organizer.app/Contents/MacOS/s3Organizer"
