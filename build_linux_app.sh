#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
APP_NAME="${1:-s3Organizer}"
VERSION="${2:-v2_38}"
case "$APP_NAME" in
  s3Organizer) LAUNCHER=s3_copy_desktop_app_launcher.py ;;
  PowerS3Browser) LAUNCHER=power_s3_browser_launcher.py ;;
  *) echo "Unknown app: $APP_NAME" >&2; exit 2 ;;
esac
[[ "$(uname -s)" == Linux ]] || { echo 'Build this package on Linux.' >&2; exit 1; }
python3 -m venv .venv-linux
.venv-linux/bin/python -m pip install -r s3_copy_desktop_app/requirements.txt 'pyinstaller>=6,<7' secretstorage jeepney
.venv-linux/bin/python -m unittest discover -s tests -v
.venv-linux/bin/pyinstaller --noconfirm --clean --onedir --name "$APP_NAME" \
  --collect-submodules keyring.backends --collect-all secretstorage --collect-all jeepney \
  --collect-data certifi --collect-all openpyxl "$LAUNCHER"
cp s3organizer_mango_1024.png "dist/$APP_NAME/icon.png"
cp packaging/linux/README.txt "dist/$APP_NAME/README.txt"
cp packaging/linux/install.sh "dist/$APP_NAME/install.sh"
chmod +x "dist/$APP_NAME/install.sh"
S3_APP_VERIFY_IMPORTS=1 "dist/$APP_NAME/$APP_NAME"
xvfb-run -a env S3_APP_VERIFY_TK_STARTUP=1 "dist/$APP_NAME/$APP_NAME"
tar -C dist -czf "dist/${APP_NAME}_${VERSION}-linux-x86_64.tar.gz" "$APP_NAME"
