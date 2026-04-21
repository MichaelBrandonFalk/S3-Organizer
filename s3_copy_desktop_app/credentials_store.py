"""Store and retrieve AWS credentials from the system credential store via keyring."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Optional

import keyring
from keyring.errors import KeyringError

LEGACY_SERVICE_NAME = "s3-copy-desktop-app"
SERVICE_NAME = "s3-copy-desktop-app-v2"
SERVICE_CANDIDATES = (SERVICE_NAME, LEGACY_SERVICE_NAME)
USERNAME_ACCESS_KEY = "aws_access_key_id"
USERNAME_SECRET_KEY = "aws_secret_access_key"
USERNAME_SESSION_TOKEN = "aws_session_token"
USERNAME_COMBINED = "aws_credentials_json"
_CACHE_INITIALIZED = False
_CACHED_CREDENTIALS: Optional["AwsCredentials"] = None


@dataclass
class AwsCredentials:
    access_key_id: str
    secret_access_key: str
    session_token: str = ""


class KeychainOwnerConflictError(RuntimeError):
    """Raised when the platform credential store rejects writes due to item ownership mismatch."""


def _set_cached_credentials(credentials: Optional["AwsCredentials"]) -> Optional["AwsCredentials"]:
    global _CACHE_INITIALIZED, _CACHED_CREDENTIALS
    _CACHE_INITIALIZED = True
    _CACHED_CREDENTIALS = credentials
    return credentials


def load_credentials(refresh: bool = False) -> Optional[AwsCredentials]:
    """Load credentials from the system credential store, returning None when missing."""
    if _CACHE_INITIALIZED and not refresh:
        return _CACHED_CREDENTIALS

    for service_name in SERVICE_CANDIDATES:
        try:
            combined_value = (keyring.get_password(service_name, USERNAME_COMBINED) or "").strip()
            if combined_value:
                payload = json.loads(combined_value)
                access_key = str(payload.get("access_key_id", "")).strip()
                secret_key = str(payload.get("secret_access_key", "")).strip()
                session_token = str(payload.get("session_token", "")).strip()
                if access_key and secret_key:
                    return _set_cached_credentials(
                        AwsCredentials(
                            access_key_id=access_key,
                            secret_access_key=secret_key,
                            session_token=session_token,
                        )
                    )
        except KeyringError as error:
            raise RuntimeError(f"Could not read saved credentials: {error}") from error
        except json.JSONDecodeError as error:
            raise RuntimeError(f"Could not read saved credentials: invalid stored credential payload ({error})") from error

    try:
        access_key = (keyring.get_password(LEGACY_SERVICE_NAME, USERNAME_ACCESS_KEY) or "").strip()
        secret_key = (keyring.get_password(LEGACY_SERVICE_NAME, USERNAME_SECRET_KEY) or "").strip()
        session_token = (keyring.get_password(LEGACY_SERVICE_NAME, USERNAME_SESSION_TOKEN) or "").strip()
    except KeyringError as error:
        raise RuntimeError(f"Could not read saved credentials: {error}") from error

    if access_key and secret_key:
        credentials = AwsCredentials(
            access_key_id=access_key,
            secret_access_key=secret_key,
            session_token=session_token,
        )
        try:
            save_credentials(credentials)
        except (RuntimeError, KeychainOwnerConflictError):
            pass
        return _set_cached_credentials(credentials)

    return _set_cached_credentials(None)


def save_credentials(credentials: AwsCredentials) -> None:
    """Write credentials to the system credential store."""
    try:
        payload = json.dumps(
            {
                "access_key_id": credentials.access_key_id,
                "secret_access_key": credentials.secret_access_key,
                "session_token": credentials.session_token,
            }
        )
        keyring.set_password(SERVICE_NAME, USERNAME_COMBINED, payload)
    except KeyringError as error:
        if "-25244" in str(error):
            raise KeychainOwnerConflictError(
                "Could not write saved credentials because an older credential-store item has an incompatible owner. "
                "Delete old 's3-copy-desktop-app' credential entries and try Save again."
            ) from error
        raise RuntimeError(f"Could not write saved credentials: {error}") from error
    _set_cached_credentials(credentials)


def clear_credentials() -> None:
    """Remove credentials from the system credential store if they exist."""
    for service_name in SERVICE_CANDIDATES:
        for username in (USERNAME_COMBINED, USERNAME_ACCESS_KEY, USERNAME_SECRET_KEY, USERNAME_SESSION_TOKEN):
            try:
                keyring.delete_password(service_name, username)
            except keyring.errors.PasswordDeleteError:
                pass
            except KeyringError as error:
                raise RuntimeError(f"Could not update saved credentials: {error}") from error
    _set_cached_credentials(None)
