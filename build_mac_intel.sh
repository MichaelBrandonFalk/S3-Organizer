#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$ROOT_DIR/build_macos_app.sh" "s3Organizer" "s3_copy_desktop_app_launcher.py" "x86_64"
