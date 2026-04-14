#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_PY="$ROOT_DIR/s3_copy_desktop_app/.venv/bin/python3"
PYINSTALLER="$ROOT_DIR/s3_copy_desktop_app/.venv/bin/pyinstaller"
ICON_PATH="$ROOT_DIR/assets/s3organizer.icns"

if [[ ! -x "$PYINSTALLER" ]]; then
  echo "PyInstaller not found in venv. Install with:"
  echo "  $ROOT_DIR/s3_copy_desktop_app/.venv/bin/pip install pyinstaller"
  exit 1
fi

cd "$ROOT_DIR"

ICON_ARGS=()
if [[ -f "$ICON_PATH" ]]; then
  ICON_ARGS=(--icon "$ICON_PATH")
else
  echo "Warning: icon file not found at $ICON_PATH. Building without custom app icon."
fi

"$PYINSTALLER" \
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
echo "Architecture: $(uname -m)"
echo "Note: Build on Intel and Apple Silicon separately if you need native support on both."
