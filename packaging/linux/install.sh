#!/usr/bin/env bash
set -euo pipefail
PACKAGE_DIR="$(cd "$(dirname "$0")" && pwd)"
if [[ -x "$PACKAGE_DIR/PowerS3Browser" ]]; then
  APP_NAME=PowerS3Browser
else
  APP_NAME=s3Organizer
fi
INSTALL_DIR="$HOME/.local/opt/$APP_NAME"
mkdir -p "$INSTALL_DIR" "$HOME/.local/share/applications"
cp -a "$PACKAGE_DIR/." "$INSTALL_DIR/"
cat > "$HOME/.local/share/applications/$APP_NAME.desktop" <<EOF
[Desktop Entry]
Type=Application
Name=$APP_NAME
Exec="$INSTALL_DIR/$APP_NAME"
Icon=$INSTALL_DIR/icon.png
Terminal=false
Categories=Utility;
EOF
echo "$APP_NAME installed. Open it from the application menu."
