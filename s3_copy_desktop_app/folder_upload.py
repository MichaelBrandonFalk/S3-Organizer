"""Read-only folder discovery and deterministic local-to-S3 upload mappings."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path


MEDIA_EXTENSIONS = {".mov", ".mp4", ".mxf", ".srt", ".vtt", ".png", ".jpg", ".jpeg", ".tif", ".tiff", ".webp", ".wav", ".mp3"}


@dataclass
class FolderUploadRow:
    local_path: str
    relative_path: str
    folder: str | None
    selected: bool = True


def parse_destination(value: str) -> tuple[str, str]:
    value = value.strip()
    if not value.startswith("s3://"):
        raise ValueError("Enter a full destination such as s3://bucket/series/title/s01/.")
    bucket, _, prefix = value[5:].partition("/")
    if not bucket or any(char.isspace() for char in bucket) or "\\" in value or "?" in bucket or "#" in bucket:
        raise ValueError("Enter a valid S3 bucket and prefix URI.")
    prefix = prefix.rstrip("/")
    if "//" in prefix or any(part in {".", ".."} for part in prefix.split("/")):
        raise ValueError("The destination contains an empty or relative path component.")
    return bucket, prefix


def scan_s3_folders(client, bucket: str, prefix: str, cancelled=None) -> list[str]:
    """Use CommonPrefixes to discover directories without downloading objects."""
    root = prefix.rstrip("/") + "/" if prefix else ""
    pending = [root]
    folders = {""}
    paginator = client.get_paginator("list_objects_v2")
    while pending:
        if cancelled and cancelled():
            raise InterruptedError("Folder scan cancelled. Nothing was uploaded.")
        current = pending.pop()
        for page in paginator.paginate(Bucket=bucket, Prefix=current, Delimiter="/"):
            if cancelled and cancelled():
                raise InterruptedError("Folder scan cancelled. Nothing was uploaded.")
            for entry in page.get("CommonPrefixes", []):
                child = entry["Prefix"]
                if not child.startswith(root) or not child.startswith(current) or child == current:
                    continue
                relative = child[len(root):].rstrip("/")
                if relative not in folders:
                    folders.add(relative)
                    pending.append(child)
    return sorted(folders)


def _canonical_folder(value: str) -> str:
    parts = []
    for part in value.split("/"):
        combined = re.fullmatch(r"s(\d+)_e(\d+)(_las)?", part, re.I)
        numbered = re.fullmatch(r"([se])(\d+)(_las)?", part, re.I)
        if combined:
            season, episode, suffix = combined.groups()
            parts.extend((f"s{int(season):02d}", f"e{int(episode):02d}{(suffix or '').lower()}"))
        elif numbered:
            letter, number, suffix = numbered.groups()
            parts.append(f"{letter.lower()}{int(number):02d}{(suffix or '').lower()}")
        else:
            parts.append(part)
    return "/".join(parts)


def map_local_folder(root: str, destination_prefix: str, folders: list[str], include_all=False) -> list[FolderUploadRow]:
    local_root = Path(root)
    if not local_root.is_dir():
        raise ValueError("Choose an existing local upload folder.")
    if local_root.is_symlink():
        raise ValueError("Choose the actual upload folder rather than a symbolic link.")
    aliases: dict[str, list[str]] = {}
    for folder in folders:
        aliases.setdefault(_canonical_folder(folder), []).append(folder)
    base_segment = _canonical_folder(destination_prefix.rstrip("/").rsplit("/", 1)[-1])
    rows = []
    for directory, subdirs, files in os.walk(local_root, followlinks=False):
        subdirs[:] = sorted(name for name in subdirs if not name.startswith(".") and not Path(directory, name).is_symlink())
        for filename in sorted(files):
            path = Path(directory, filename)
            if filename.startswith(".") or path.is_symlink() or not path.is_file():
                continue
            if not include_all and path.suffix.lower() not in MEDIA_EXTENSIONS:
                continue
            relative = path.relative_to(local_root)
            parent = relative.parent.as_posix()
            candidate = _canonical_folder("" if parent == "." else parent)
            if candidate == base_segment:
                candidate = ""
            elif candidate.startswith(base_segment + "/") and base_segment:
                candidate = candidate[len(base_segment) + 1:]
            matches = aliases.get(candidate, [])
            folder = matches[0] if len(matches) == 1 else None
            rows.append(FolderUploadRow(str(path), relative.as_posix(), folder, folder is not None))
    return rows


def destination_key(prefix: str, folder: str, filename: str) -> str:
    if folder and (folder.startswith("/") or any(part in {".", "..", ""} for part in folder.split("/"))):
        raise ValueError("Choose a folder from the scanned destination list.")
    return "/".join(part for part in (prefix.rstrip("/"), folder, filename) if part)


def validate_selection(rows: list[FolderUploadRow], prefix: str) -> list[FolderUploadRow]:
    selected = [row for row in rows if row.selected]
    if not selected:
        raise ValueError("Select at least one file to upload.")
    keys = set()
    for row in selected:
        if row.folder is None:
            raise ValueError(f"Choose a destination folder for {row.relative_path}, or exclude it.")
        if not Path(row.local_path).is_file() or Path(row.local_path).is_symlink():
            raise ValueError(f"The local file is missing or is a symbolic link: {row.local_path}")
        key = destination_key(prefix, row.folder, Path(row.local_path).name)
        if key in keys:
            raise ValueError(f"Multiple selected files target the same destination: {key}. Exclude one or change its folder.")
        keys.add(key)
    return selected
