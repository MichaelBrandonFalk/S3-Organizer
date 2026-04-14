# S3 Copy Desktop App (macOS, Tkinter)

A lightweight internal macOS desktop app for non-technical users to copy one S3 object to another S3 location.

This v1 is intentionally **non-destructive**:
- Copy only
- No delete of source
- No move
- Warn/confirm before overwrite when destination already exists

## What Users Enter

The main window only asks for:
1. Current File Name
2. Desired Move Folder
3. Desired Name

The app resolves paths as:
- `s3://SOURCE_BUCKET/SOURCE_PREFIX/Current File Name`
- `s3://DEST_BUCKET/DEST_PREFIX/Desired Move Folder/Desired Name`

## Hidden Settings

Open `App -> Settings...` from the menu bar to configure:
- `SOURCE_BUCKET`
- `SOURCE_PREFIX`
- `DEST_BUCKET`
- `DEST_PREFIX`
- optional AWS region
- AWS credentials

Credential options:
- save credentials in macOS Keychain via `keyring`
- session-only mode (do not save credentials; in-memory for current app run only)

## File Structure

- `app.py`: Tkinter UI, threading, workflow orchestration
- `validators.py`: sanitization, path building, validation
- `s3_service.py`: S3 checks/copy and AWS error mapping
- `config_store.py`: non-secret config persistence
- `credentials_store.py`: keychain credential storage
- `requirements.txt`: Python dependencies

## Run Instructions

From this repository root (`/Users/brandon.falk/ai-test-project`):

```bash
python3 -m venv s3_copy_desktop_app/.venv
source s3_copy_desktop_app/.venv/bin/activate
pip install -r s3_copy_desktop_app/requirements.txt
python3 -m s3_copy_desktop_app.app
```

## Validation and Safety Behavior

- Trims whitespace from all input fields
- Prevents accidental double slashes in S3 keys
- `Current File Name` must not include slashes
- `Desired Name` must not include slashes and must include an extension
- `Desired Move Folder` supports nested paths like `folder1/folder2/folder3`
- Prevents exact source=destination object matches
- Confirms resolved source/destination before execution
- Verifies source object exists before copy
- Checks destination object existence and asks before overwrite
- Disables Copy button during execution
- Uses worker thread so UI stays responsive
- Shows status log and plain-English success/error messages

## Notes for macOS Packaging Later

For a `.app` bundle, a straightforward approach is `py2app` or `PyInstaller`:

### Option A: PyInstaller

```bash
pip install pyinstaller
pyinstaller --windowed --name "s3Organizer" s3_copy_desktop_app_launcher.py
```

Expected output app:
- `dist/s3Organizer.app`

### Recommended build in this repo

Use the included build script from repository root:

```bash
./build_mac_app.sh
```

Output:
- `dist/s3Organizer.app`
- optional shareable zip (create with `ditto`): `dist/s3Organizer-macOS-arm64.zip`

### Option B: py2app

Create a minimal `setup.py` and build with:

```bash
pip install py2app
python setup.py py2app
```

## Operational Notes

- The app uses keychain credentials when saved in Settings.
- Settings includes a session-only credential mode to bypass Keychain writes.
- If keychain credentials are not present, boto3 can still use standard AWS credential/provider chain configured on the Mac.
- Replace placeholder bucket/prefix values in Settings before production use.
- Credentials are not embedded in the `.app` bundle; keychain items stay on each user machine.
- This build is architecture-specific. Build on Apple Silicon for arm64, and build on Intel for x86_64.
