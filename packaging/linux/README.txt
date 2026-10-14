s3Organizer / PowerS3Browser for Linux Mint

Target: Linux Mint 21 or newer, 64-bit Intel/AMD (x86_64).
Python, Tk, boto3 and spreadsheet dependencies are included. AWS CLI is not required.

Extract the tar.gz using Archive Manager. Open the extracted folder and double-click
the s3Organizer or PowerS3Browser executable. Alternatively, run ./install.sh in a
terminal in that folder to add the app to your application menu (no sudo needed).

Settings start blank. Enter your bucket configuration and your own AWS credentials.
Saved credentials use the encrypted login keyring via Secret Service. Unlock the
login keyring in Passwords and Keys if prompted, or use session-only mode.
No credentials or private settings are included in this package.

Reports open in your default spreadsheet application using xdg-open.
Progress checkpoints are saved under ~/.local/state/<app-name>/checkpoints,
or XDG_STATE_HOME if configured. Keep the whole extracted folder together.

If your minimal Linux installation is missing desktop libraries, install:
sudo apt install libx11-6 libxext6 libxrender1 libxft2 libfontconfig1 libsm6 libice6 xdg-utils gnome-keyring

This package is built and startup-tested on Ubuntu 22.04, the base of Mint 21.
An end-to-end AWS transfer still requires validation with your own account.
