#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$ROOT_DIR/build_macos_app.sh" "PowerS3Browser" "power_s3_browser_launcher.py" "native"
