@echo off
setlocal

set "ROOT_DIR=%~dp0"
set "VENV_DIR=%ROOT_DIR%.venv-windows"
if defined S3ORG_VENV_DIR set "VENV_DIR=%S3ORG_VENV_DIR%"
set "PYINSTALLER=%VENV_DIR%\Scripts\pyinstaller.exe"
set "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"

set "ICON_PATH=%ROOT_DIR%s3organizer.ico"

if not exist "%PYINSTALLER%" (
  if not exist "%PYTHON_EXE%" (
    echo Creating Windows venv at %VENV_DIR%
    py -3.12 -m venv "%VENV_DIR%" || exit /b 1
  )
  echo Installing Windows build dependencies...
  "%PYTHON_EXE%" -m pip install --upgrade pip || exit /b 1
  "%PYTHON_EXE%" -m pip install -r "%ROOT_DIR%s3_copy_desktop_app\requirements.txt" pyinstaller pillow || exit /b 1
)

cd /d "%ROOT_DIR%"

if not exist "%ICON_PATH%" (
  echo Generating Windows icon...
  "%PYTHON_EXE%" "%ROOT_DIR%generate_windows_icon.py" || exit /b 1
)

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

if exist "%ROOT_DIR%dist\s3Organizer" (
  powershell -NoProfile -Command "Compress-Archive -Path '%ROOT_DIR%dist\s3Organizer\*' -DestinationPath '%ROOT_DIR%dist\s3Organizer-windows.zip' -Force" || exit /b 1
)

echo Build complete: %ROOT_DIR%dist\s3Organizer\s3Organizer.exe
if exist "%ROOT_DIR%dist\s3Organizer-windows.zip" echo Packaged zip: %ROOT_DIR%dist\s3Organizer-windows.zip
