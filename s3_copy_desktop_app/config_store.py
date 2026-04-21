"""Load and save non-secret app configuration."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
import os
from pathlib import Path


def _default_app_dir() -> Path:
    appdata = os.getenv("APPDATA", "").strip()
    if appdata:
        return Path(appdata) / "s3_copy_desktop_app"
    return Path.home() / ".s3_copy_desktop_app"


APP_DIR = _default_app_dir()
CONFIG_PATH = APP_DIR / "config.json"


@dataclass
class AppConfig:
    """S3 routing configuration for the copy workflow.

    Defaults are intentionally blank. Configure these values in Settings for
    your environment.
    Do not store secrets in this file.
    """

    source_bucket: str = ""
    source_prefix: str = ""
    dest_bucket: str = ""
    dest_prefix: str = ""
    aws_region: str = ""
    credential_mode: str = "keychain"


DEFAULT_CONFIG = AppConfig()


def load_config() -> AppConfig:
    """Return saved config or defaults when no file exists."""
    if not CONFIG_PATH.exists():
        return DEFAULT_CONFIG

    with CONFIG_PATH.open("r", encoding="utf-8") as file_handle:
        data = json.load(file_handle)

    credential_mode = str(data.get("credential_mode", DEFAULT_CONFIG.credential_mode)).strip().lower()
    if credential_mode not in {"keychain", "session"}:
        credential_mode = DEFAULT_CONFIG.credential_mode

    return AppConfig(
        source_bucket=str(data.get("source_bucket", DEFAULT_CONFIG.source_bucket)).strip(),
        source_prefix=str(data.get("source_prefix", DEFAULT_CONFIG.source_prefix)).strip(),
        dest_bucket=str(data.get("dest_bucket", DEFAULT_CONFIG.dest_bucket)).strip(),
        dest_prefix=str(data.get("dest_prefix", DEFAULT_CONFIG.dest_prefix)).strip(),
        aws_region=str(data.get("aws_region", "")).strip(),
        credential_mode=credential_mode,
    )


def save_config(config: AppConfig) -> None:
    """Persist non-secret config to the app directory in the user profile."""
    APP_DIR.mkdir(parents=True, exist_ok=True)
    with CONFIG_PATH.open("w", encoding="utf-8") as file_handle:
        json.dump(asdict(config), file_handle, indent=2)
