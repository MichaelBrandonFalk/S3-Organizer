"""Runtime verification hooks used by packaged app builds."""

from __future__ import annotations

import importlib
import os
import sys


SMOKE_IMPORTS = (
    "boto3",
    "botocore",
    "certifi",
    "keyring",
    "openpyxl",
    "s3_copy_desktop_app.app",
    "tkinter",
)


def run_import_smoke_if_requested() -> None:
    """Import package dependencies from the frozen app and exit when requested."""

    if os.getenv("S3_APP_VERIFY_IMPORTS") != "1":
        return

    for module_name in SMOKE_IMPORTS:
        importlib.import_module(module_name)

    print("Packaged import smoke check passed.", file=sys.stderr, flush=True)
    raise SystemExit(0)
