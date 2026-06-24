"""Launcher script for the optional-dry-run PowerS3Browser build."""

from __future__ import annotations

import os

os.environ.setdefault("S3_APP_TITLE", "PowerS3Browser")
os.environ.setdefault("S3_APP_FILE_SLUG", "PowerS3Browser")
os.environ.setdefault("S3_SIMPLIFIED_BULK_REQUIRE_DRY_RUN", "0")

from s3_copy_desktop_app.runtime_verify import run_import_smoke_if_requested

run_import_smoke_if_requested()

from s3_copy_desktop_app.app import main


if __name__ == "__main__":
    main()
