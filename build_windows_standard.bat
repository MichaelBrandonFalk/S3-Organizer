@echo off
setlocal

set "ROOT_DIR=%~dp0"
set "PYINSTALLER=%ROOT_DIR%s3_copy_desktop_app\.venv\Scripts\pyinstaller.exe"
if not exist "%PYINSTALLER%" set "PYINSTALLER=%ROOT_DIR%..\s3_copy_desktop_app\.venv\Scripts\pyinstaller.exe"

set "ICON_PATH=%ROOT_DIR%s3organizer.ico"

if not exist "%PYINSTALLER%" (
  echo PyInstaller not found in venv.
  echo Install with:
  echo   %ROOT_DIR%..\s3_copy_desktop_app\.venv\Scripts\pip install pyinstaller
  exit /b 1
)

cd /d "%ROOT_DIR%"

set "ICON_ARGS="
if exist "%ICON_PATH%" set "ICON_ARGS=--icon %ICON_PATH%"

"%PYINSTALLER%" ^
  --noconfirm ^
  --windowed ^
  --name "s3Organizer" ^
  --collect-submodules keyring.backends ^
  --collect-data keyring ^
  --collect-data certifi ^
  --hidden-import tkinter ^
  %ICON_ARGS% ^
  s3_copy_desktop_app_launcher.py

echo Build complete: %ROOT_DIR%dist\s3Organizer.exe
