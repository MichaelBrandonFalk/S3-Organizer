#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYINSTALLER="$ROOT_DIR/s3_copy_desktop_app/.venv/bin/pyinstaller"
if [[ ! -x "$PYINSTALLER" ]]; then
  PYINSTALLER="$ROOT_DIR/../s3_copy_desktop_app/.venv/bin/pyinstaller"
fi
ICON_PATH="$ROOT_DIR/assets/s3organizer.icns"
if [[ ! -f "$ICON_PATH" ]]; then
  ICON_PATH="$ROOT_DIR/s3organizer.icns"
fi

if [[ ! -x "$PYINSTALLER" ]]; then
  echo "PyInstaller not found in venv. Install with:"
  echo "  $ROOT_DIR/../s3_copy_desktop_app/.venv/bin/pip install pyinstaller"
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
  --name "PowerS3Browser" \
  --collect-submodules keyring.backends \
  --collect-data keyring \
  --collect-data certifi \
  --hidden-import tkinter \
  "${ICON_ARGS[@]}" \
  power_s3_browser_launcher.py

echo "Build complete: $ROOT_DIR/dist/PowerS3Browser.app"
echo "Architecture: $(uname -m)"
echo "Note: Build on Intel and Apple Silicon separately if you need native support on both."
