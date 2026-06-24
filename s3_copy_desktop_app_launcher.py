"""Launcher script for PyInstaller packaging."""

from s3_copy_desktop_app.runtime_verify import run_import_smoke_if_requested

run_import_smoke_if_requested()

from s3_copy_desktop_app.app import main

if __name__ == "__main__":
    main()
