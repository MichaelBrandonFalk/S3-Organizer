#!/bin/bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 3 ]]; then
  echo "Usage: $0 <app-name> <launcher.py> [native|x86_64]" >&2
  exit 2
fi

APP_NAME="$1"
LAUNCHER="$2"
TARGET_ARCH="${3:-native}"

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
BUILD_DIR="$ROOT_DIR/build"
DIST_DIR="$ROOT_DIR/dist"
APP_DIR="$DIST_DIR/$APP_NAME.app"
VENV_DIR="$ROOT_DIR/.venv-build-$APP_NAME-$TARGET_ARCH"
REQUIREMENTS="$ROOT_DIR/s3_copy_desktop_app/requirements.txt"
PYINSTALLER="$VENV_DIR/bin/pyinstaller"
ICON_PATH="$ROOT_DIR/assets/s3organizer.icns"
if [[ ! -f "$ICON_PATH" ]]; then
  ICON_PATH="$ROOT_DIR/s3organizer.icns"
fi

if [[ ! -f "$ROOT_DIR/$LAUNCHER" ]]; then
  echo "Launcher not found: $ROOT_DIR/$LAUNCHER" >&2
  exit 1
fi

if [[ "$TARGET_ARCH" == "x86_64" ]]; then
  PYENV_ROOT_X86="${PYENV_ROOT_X86:-$ROOT_DIR/.pyenv-x86}"
  PYTHON_VERSION_X86="${PYTHON_VERSION_X86:-3.12.9}"
  PYTHON_BIN="$PYENV_ROOT_X86/versions/$PYTHON_VERSION_X86/bin/python3"
  if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "Intel Python not found at: $PYTHON_BIN" >&2
    echo "Build it first with Rosetta, for example:" >&2
    echo "  PYENV_ROOT=$PYENV_ROOT_X86 PYTHON_BUILD_SKIP_HOMEBREW=1 MAKE_INSTALL_OPTS=-j1 arch -x86_64 pyenv install $PYTHON_VERSION_X86" >&2
    exit 1
  fi
elif [[ "$TARGET_ARCH" == "native" ]]; then
  PYENV_ROOT_NATIVE="${PYENV_ROOT_NATIVE:-$HOME/.pyenv}"
  PYTHON_VERSION_NATIVE="${PYTHON_VERSION_NATIVE:-3.13.1}"
  PYENV_NATIVE_PYTHON="$PYENV_ROOT_NATIVE/versions/$PYTHON_VERSION_NATIVE/bin/python3"
  if [[ -x "$PYENV_NATIVE_PYTHON" ]]; then
    PYTHON_BIN="$PYENV_NATIVE_PYTHON"
  else
    PYTHON_BIN="${PYTHON_BIN:-python3}"
  fi
else
  echo "Unsupported target arch: $TARGET_ARCH" >&2
  exit 2
fi

run_arch() {
  if [[ "$TARGET_ARCH" == "x86_64" ]]; then
    arch -x86_64 "$@"
  else
    "$@"
  fi
}

if [[ "$TARGET_ARCH" == "native" ]]; then
  PYTHON_COMMAND_PATH="$(command -v "$PYTHON_BIN" || true)"
  if [[ "$PYTHON_COMMAND_PATH" == /opt/homebrew/* && "${ALLOW_HOMEBREW_PYTHON:-0}" != "1" ]]; then
    echo "Refusing to build from Homebrew Python: $PYTHON_COMMAND_PATH" >&2
    echo "Install/use pyenv Python at $PYENV_NATIVE_PYTHON or set PYTHON_BIN to a non-Homebrew Python." >&2
    exit 1
  fi
fi

cd "$ROOT_DIR"

echo "Using Python: $PYTHON_BIN"
echo "Cleaning old build artifacts for $APP_NAME ($TARGET_ARCH)..."
rm -rf "$BUILD_DIR" "$DIST_DIR" "$VENV_DIR" "$ROOT_DIR/$APP_NAME.spec"

echo "Creating clean venv: $VENV_DIR"
run_arch "$PYTHON_BIN" -m venv "$VENV_DIR"
run_arch "$VENV_DIR/bin/python" -m pip install -r "$REQUIREMENTS" pyinstaller

ICON_ARGS=()
if [[ -f "$ICON_PATH" ]]; then
  ICON_ARGS=(--icon "$ICON_PATH")
else
  echo "Warning: icon file not found at $ICON_PATH. Building without custom app icon." >&2
fi

echo "Building $APP_NAME.app..."
run_arch "$PYINSTALLER" \
  --noconfirm \
  --clean \
  --windowed \
  --name "$APP_NAME" \
  --collect-submodules keyring.backends \
  --collect-data keyring \
  --collect-data certifi \
  --collect-all openpyxl \
  --hidden-import tkinter \
  --hidden-import certifi \
  --hidden-import openpyxl \
  "${ICON_ARGS[@]}" \
  "$LAUNCHER"

if [[ ! -d "$APP_DIR" ]]; then
  echo "Build failed: app bundle was not created at $APP_DIR" >&2
  exit 1
fi

if [[ "$TARGET_ARCH" == "x86_64" ]]; then
  TKINTER_EXT="$(find "$APP_DIR/Contents/Frameworks" -path '*lib-dynload/_tkinter.cpython-*-darwin.so' | head -n 1)"
  TK_BUILD_FRAMEWORK="$ROOT_DIR/.x86-tcltk/Library/Frameworks/Tk.framework/Versions/8.6/Tk"
  TK_BUNDLED_FRAMEWORK="@loader_path/../../Tk.framework/Versions/8.6/Tk"

  if [[ -n "$TKINTER_EXT" && -f "$TK_BUILD_FRAMEWORK" ]]; then
    install_name_tool -change "$TK_BUILD_FRAMEWORK" "$TK_BUNDLED_FRAMEWORK" "$TKINTER_EXT" || true
  fi
fi

echo "Signing final app bundle..."
codesign --force --deep --sign - "$APP_DIR"

echo "Verifying code signature..."
codesign --verify --deep --strict --verbose=4 "$APP_DIR"

echo "Running packaged dependency import smoke check..."
S3_APP_VERIFY_IMPORTS=1 "$APP_DIR/Contents/MacOS/$APP_NAME"

echo "Running packaged Tk startup smoke check..."
if [[ "$TARGET_ARCH" == "x86_64" ]]; then
  open -W -n "$APP_DIR" --args --s3-app-verify-tk-startup
else
  S3_APP_VERIFY_TK_STARTUP=1 "$APP_DIR/Contents/MacOS/$APP_NAME"
fi

echo "Checking Mach-O load commands for forbidden local development references..."
FORBIDDEN_PATTERN='(/opt/homebrew/Cellar/python@[^[:space:]]*|\.venv)'
FOUND_FORBIDDEN=0
while IFS= read -r -d '' candidate; do
  if ! file "$candidate" | grep -q "Mach-O"; then
    continue
  fi
  refs="$(otool -L "$candidate" 2>/dev/null || true; otool -l "$candidate" 2>/dev/null || true)"
  if printf '%s\n' "$refs" | grep -E "$FORBIDDEN_PATTERN" >/dev/null; then
    echo "Forbidden reference found in: $candidate" >&2
    printf '%s\n' "$refs" | grep -E "$FORBIDDEN_PATTERN" >&2
    FOUND_FORBIDDEN=1
  fi
done < <(find "$APP_DIR" -type f -print0)

if [[ "$FOUND_FORBIDDEN" -ne 0 ]]; then
  exit 1
fi

echo "Build complete: $APP_DIR"
file "$APP_DIR/Contents/MacOS/$APP_NAME"
