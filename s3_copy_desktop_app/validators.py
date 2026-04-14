"""Input sanitization, path construction, and validation helpers."""

from __future__ import annotations

from dataclasses import dataclass

from .config_store import AppConfig


@dataclass
class UserInput:
    current_file_name: str
    desired_move_folder: str
    desired_name: str
    current_caption_name: str = ""
    desired_caption_name: str = ""


@dataclass
class ResolvedS3Paths:
    source_bucket: str
    source_key: str
    dest_bucket: str
    dest_key: str

    @property
    def source_uri(self) -> str:
        return f"s3://{self.source_bucket}/{self.source_key}"

    @property
    def dest_uri(self) -> str:
        return f"s3://{self.dest_bucket}/{self.dest_key}"


def sanitize_filename(value: str) -> str:
    return value.strip().replace("\\", "/")


def sanitize_folder_path(value: str) -> str:
    cleaned = value.strip().replace("\\", "/")
    while "//" in cleaned:
        cleaned = cleaned.replace("//", "/")
    return cleaned.strip("/")


def join_key_parts(*parts: str) -> str:
    normalized_parts = []
    for part in parts:
        cleaned = (part or "").strip().replace("\\", "/").strip("/")
        while "//" in cleaned:
            cleaned = cleaned.replace("//", "/")
        if cleaned:
            normalized_parts.append(cleaned)
    return "/".join(normalized_parts)


def sanitize_user_input(
    current_file_name: str,
    desired_move_folder: str,
    desired_name: str,
    current_caption_name: str = "",
    desired_caption_name: str = "",
) -> UserInput:
    return UserInput(
        current_file_name=sanitize_filename(current_file_name),
        desired_move_folder=sanitize_folder_path(desired_move_folder),
        desired_name=sanitize_filename(desired_name),
        current_caption_name=sanitize_filename(current_caption_name),
        desired_caption_name=sanitize_filename(desired_caption_name),
    )


def build_paths(config: AppConfig, user_input: UserInput) -> ResolvedS3Paths:
    source_key = join_key_parts(config.source_prefix, user_input.current_file_name)
    dest_key = join_key_parts(config.dest_prefix, user_input.desired_move_folder, user_input.desired_name)

    return ResolvedS3Paths(
        source_bucket=config.source_bucket.strip(),
        source_key=source_key,
        dest_bucket=config.dest_bucket.strip(),
        dest_key=dest_key,
    )


def validate_user_input(config: AppConfig, user_input: UserInput) -> list[str]:
    errors: list[str] = []

    if not user_input.current_file_name:
        errors.append("Current File Name cannot be blank.")
    if not user_input.desired_move_folder:
        errors.append("Desired Move Folder cannot be blank.")
    if not user_input.desired_name:
        errors.append("Desired Name cannot be blank.")

    if "/" in user_input.current_file_name:
        errors.append("Current File Name must be a file name only (no slashes).")
    if "/" in user_input.desired_name:
        errors.append("Desired Name must be a file name only (no slashes).")

    if user_input.desired_name and "." not in user_input.desired_name.strip("."):
        errors.append("Desired Name must include a file extension (example: report.pdf).")

    has_current_caption = bool(user_input.current_caption_name)
    has_desired_caption = bool(user_input.desired_caption_name)
    if has_current_caption != has_desired_caption:
        errors.append(
            "To copy a caption, provide both Current Caption Name and Desired Caption Name, or leave both blank."
        )

    if "/" in user_input.current_caption_name:
        errors.append("Current Caption Name must be a file name only (no slashes).")
    if "/" in user_input.desired_caption_name:
        errors.append("Desired Caption Name must be a file name only (no slashes).")

    if user_input.desired_caption_name and "." not in user_input.desired_caption_name.strip("."):
        errors.append("Desired Caption Name must include a file extension (example: trailer_en.vtt).")

    if not config.source_bucket.strip():
        errors.append("Source bucket is not configured. Open Settings.")
    if not config.dest_bucket.strip():
        errors.append("Destination bucket is not configured. Open Settings.")

    return errors


def validate_paths_not_identical(paths: ResolvedS3Paths) -> list[str]:
    if (
        paths.source_bucket.strip() == paths.dest_bucket.strip()
        and paths.source_key.strip() == paths.dest_key.strip()
    ):
        return ["Source and destination resolve to the exact same S3 object."]
    return []
