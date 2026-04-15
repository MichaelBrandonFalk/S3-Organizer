"""S3 operations for existence checks and copy-only behavior."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import math
import os
import time
from typing import Callable, Optional

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError, EndpointConnectionError, NoCredentialsError

from .config_store import AppConfig
from .credentials_store import AwsCredentials


class UserVisibleError(Exception):
    """Error message intended to be shown directly to end users."""


class DestinationExistsError(UserVisibleError):
    """Raised when copy is blocked because destination already exists."""


@dataclass
class S3ObjectRef:
    bucket: str
    key: str


COPY_OBJECT_MAX_BYTES = 5 * 1024**3
MIN_MULTIPART_PART_SIZE_BYTES = 5 * 1024**2
DEFAULT_MULTIPART_PART_SIZE_BYTES = 256 * 1024**2
MAX_MULTIPART_PARTS = 10_000
MAX_MULTIPART_WORKERS = 8
MAX_RETRY_ATTEMPTS = 4
INITIAL_RETRY_DELAY_SECONDS = 1.0
ProgressCallback = Callable[[str], None]


def create_s3_client(config: AppConfig, credentials: AwsCredentials | None):
    """Create an S3 client from keychain credentials or default AWS chain."""
    session_kwargs = {}
    if config.aws_region.strip():
        session_kwargs["region_name"] = config.aws_region.strip()

    if credentials:
        session_kwargs.update(
            aws_access_key_id=credentials.access_key_id,
            aws_secret_access_key=credentials.secret_access_key,
        )
        if credentials.session_token:
            session_kwargs["aws_session_token"] = credentials.session_token

    client_config = BotoConfig(
        max_pool_connections=32,
        retries={"mode": "standard", "max_attempts": 6},
    )
    return boto3.session.Session(**session_kwargs).client("s3", config=client_config)


def object_exists(s3_client, object_ref: S3ObjectRef) -> bool:
    try:
        s3_client.head_object(Bucket=object_ref.bucket, Key=object_ref.key)
        return True
    except ClientError as error:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return False
        if error_code in {"403", "AccessDenied"}:
            raise UserVisibleError(
                "Access denied while checking S3 objects. Confirm AWS permissions for source and destination buckets."
            ) from error
        raise map_aws_error(error) from error
    except (NoCredentialsError, EndpointConnectionError, BotoCoreError) as error:
        raise map_aws_error(error) from error


def prefix_exists(s3_client, bucket: str, prefix: str) -> bool:
    normalized_prefix = prefix.strip("/")
    if not normalized_prefix:
        return True

    search_prefix = normalized_prefix.rstrip("/") + "/"
    try:
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=search_prefix, MaxKeys=1)
        return bool(response.get("Contents"))
    except ClientError as error:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        if error_code in {"403", "AccessDenied"}:
            raise UserVisibleError(
                "Access denied while checking destination folder paths. Confirm AWS permissions for the destination bucket."
            ) from error
        raise map_aws_error(error) from error
    except (NoCredentialsError, EndpointConnectionError, BotoCoreError) as error:
        raise map_aws_error(error) from error


def list_objects_under_prefix(
    s3_client,
    bucket: str,
    prefix: str,
    progress_callback: Optional[ProgressCallback] = None,
) -> list[S3ObjectRef]:
    objects: list[S3ObjectRef] = []
    continuation_token: str | None = None

    while True:
        try:
            request_kwargs = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
            if continuation_token:
                request_kwargs["ContinuationToken"] = continuation_token

            response = _call_with_retries(
                lambda: s3_client.list_objects_v2(**request_kwargs),
                progress_callback=progress_callback,
                operation_name="list_objects_v2",
            )
        except (NoCredentialsError, EndpointConnectionError, BotoCoreError, ClientError) as error:
            raise map_aws_error(error) from error

        for entry in response.get("Contents", []):
            key = str(entry.get("Key", "")).strip()
            if not key or key.endswith("/"):
                continue
            objects.append(S3ObjectRef(bucket=bucket, key=key))

        _notify_progress(progress_callback, f"Scanned {len(objects)} object(s) so far...")

        if not response.get("IsTruncated"):
            break
        continuation_token = response.get("NextContinuationToken")

    return objects


def copy_object(
    s3_client,
    source: S3ObjectRef,
    destination: S3ObjectRef,
    allow_overwrite: bool,
    progress_callback: Optional[ProgressCallback] = None,
) -> None:
    source_size = get_object_size(s3_client, source)
    _notify_progress(progress_callback, f"Source size: {format_bytes(source_size)}")

    if source_size <= COPY_OBJECT_MAX_BYTES:
        _notify_progress(progress_callback, "Starting standard copy...")
        copy_single_part(s3_client, source, destination, allow_overwrite)
        return

    copy_multipart(
        s3_client,
        source,
        destination,
        source_size,
        allow_overwrite,
        progress_callback,
    )


def upload_local_file(
    s3_client,
    local_path: str,
    destination: S3ObjectRef,
    progress_callback: Optional[ProgressCallback] = None,
) -> None:
    try:
        file_size = int(os.path.getsize(local_path))
    except OSError as error:
        raise UserVisibleError(f"Local file not found or unreadable: {local_path}") from error

    _notify_progress(progress_callback, f"Local file size: {format_bytes(file_size)}")
    _notify_progress(progress_callback, "Starting upload...")

    transfer_config = TransferConfig(
        multipart_threshold=64 * 1024**2,
        multipart_chunksize=DEFAULT_MULTIPART_PART_SIZE_BYTES,
        max_concurrency=MAX_MULTIPART_WORKERS,
        use_threads=True,
    )

    try:
        _call_with_retries(
            lambda: s3_client.upload_file(
                local_path,
                destination.bucket,
                destination.key,
                Config=transfer_config,
            ),
            operation_name="upload_file",
            allow_retry=False,
        )
    except (NoCredentialsError, EndpointConnectionError, BotoCoreError, ClientError) as error:
        raise map_aws_error(error) from error


def delete_object(s3_client, object_ref: S3ObjectRef) -> None:
    try:
        _call_with_retries(
            lambda: s3_client.delete_object(Bucket=object_ref.bucket, Key=object_ref.key),
            operation_name="delete_object",
        )
    except (NoCredentialsError, EndpointConnectionError, BotoCoreError, ClientError) as error:
        raise map_aws_error(error) from error


def get_object_size(s3_client, object_ref: S3ObjectRef) -> int:
    try:
        response = s3_client.head_object(Bucket=object_ref.bucket, Key=object_ref.key)
        return int(response["ContentLength"])
    except (NoCredentialsError, EndpointConnectionError, BotoCoreError, ClientError) as error:
        raise map_aws_error(error) from error


def copy_single_part(
    s3_client,
    source: S3ObjectRef,
    destination: S3ObjectRef,
    allow_overwrite: bool,
) -> None:
    try:
        copy_kwargs = {
            "Bucket": destination.bucket,
            "Key": destination.key,
            "CopySource": {"Bucket": source.bucket, "Key": source.key},
            "MetadataDirective": "COPY",
        }
        if not allow_overwrite:
            # Atomic no-overwrite guard. If destination appears between check and copy,
            # S3 returns PreconditionFailed and no overwrite occurs.
            copy_kwargs["IfNoneMatch"] = "*"

        _call_with_retries(
            lambda: s3_client.copy_object(**copy_kwargs),
            operation_name="copy_object",
            allow_retry=False,
        )
    except ClientError as error:
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        if error_code in {"EntityTooLarge"}:
            # Safety fallback in case source size changed between checks.
            source_size = get_object_size(s3_client, source)
            copy_multipart(s3_client, source, destination, source_size, allow_overwrite)
            return

        raise _map_copy_client_error(error) from error
    except (NoCredentialsError, EndpointConnectionError, BotoCoreError) as error:
        raise map_aws_error(error) from error


def copy_multipart(
    s3_client,
    source: S3ObjectRef,
    destination: S3ObjectRef,
    source_size: int,
    allow_overwrite: bool,
    progress_callback: Optional[ProgressCallback] = None,
) -> None:
    upload_id = None
    completed = False

    try:
        create_response = _call_with_retries(
            lambda: s3_client.create_multipart_upload(
                Bucket=destination.bucket,
                Key=destination.key,
            ),
            progress_callback=progress_callback,
            operation_name="create_multipart_upload",
            allow_retry=False,
        )
        upload_id = create_response["UploadId"]

        part_size = calculate_multipart_part_size(source_size)
        part_count = math.ceil(source_size / part_size)
        worker_count = min(MAX_MULTIPART_WORKERS, part_count)
        completed_parts_map: dict[int, str] = {}
        _notify_progress(
            progress_callback,
            (
                f"Large file detected. Multipart copy with {part_count} parts "
                f"of up to {format_bytes(part_size)} each using {worker_count} workers."
            ),
        )

        progress_interval = max(1, part_count // 20)
        completed_count = 0

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = []
            for part_index in range(part_count):
                part_number = part_index + 1
                start = part_index * part_size
                end = min(source_size - 1, start + part_size - 1)
                futures.append(
                    executor.submit(
                        _upload_copy_part,
                        s3_client,
                        source,
                        destination,
                        upload_id,
                        part_number,
                        start,
                        end,
                    )
                )

            for future in as_completed(futures):
                part_number, etag = future.result()
                completed_parts_map[part_number] = etag
                completed_count += 1

                if (
                    completed_count == 1
                    or completed_count == part_count
                    or completed_count % progress_interval == 0
                ):
                    percent = int((completed_count / part_count) * 100)
                    _notify_progress(
                        progress_callback,
                        f"Copy progress: {percent}% ({completed_count}/{part_count} parts)",
                    )

        complete_kwargs = {
            "Bucket": destination.bucket,
            "Key": destination.key,
            "UploadId": upload_id,
            "MultipartUpload": {
                "Parts": [
                    {"ETag": completed_parts_map[part_number], "PartNumber": part_number}
                    for part_number in range(1, part_count + 1)
                ]
            },
        }
        if not allow_overwrite:
            # Atomic no-overwrite guard for large objects.
            complete_kwargs["IfNoneMatch"] = "*"

        _call_with_retries(
            lambda: s3_client.complete_multipart_upload(**complete_kwargs),
            progress_callback=progress_callback,
            operation_name="complete_multipart_upload",
            allow_retry=False,
        )
        completed = True
    except ClientError as error:
        raise _map_copy_client_error(error) from error
    except (NoCredentialsError, EndpointConnectionError, BotoCoreError) as error:
        raise map_aws_error(error) from error
    finally:
        if upload_id and not completed:
            try:
                s3_client.abort_multipart_upload(
                    Bucket=destination.bucket,
                    Key=destination.key,
                    UploadId=upload_id,
                )
            except ClientError:
                # Best effort cleanup only.
                pass


def calculate_multipart_part_size(source_size: int) -> int:
    return max(
        MIN_MULTIPART_PART_SIZE_BYTES,
        DEFAULT_MULTIPART_PART_SIZE_BYTES,
        math.ceil(source_size / MAX_MULTIPART_PARTS),
    )


def _upload_copy_part(
    s3_client,
    source: S3ObjectRef,
    destination: S3ObjectRef,
    upload_id: str,
    part_number: int,
    start: int,
    end: int,
) -> tuple[int, str]:
    copy_source_range = f"bytes={start}-{end}"
    part_response = _call_with_retries(
        lambda: s3_client.upload_part_copy(
            Bucket=destination.bucket,
            Key=destination.key,
            UploadId=upload_id,
            PartNumber=part_number,
            CopySource={"Bucket": source.bucket, "Key": source.key},
            CopySourceRange=copy_source_range,
        ),
        operation_name=f"upload_part_copy(part={part_number})",
    )
    return part_number, part_response["CopyPartResult"]["ETag"]


def _call_with_retries(
    operation,
    progress_callback: Optional[ProgressCallback] = None,
    operation_name: str = "aws_operation",
    allow_retry: bool = True,
):
    delay = INITIAL_RETRY_DELAY_SECONDS
    for attempt in range(1, MAX_RETRY_ATTEMPTS + 1):
        try:
            return operation()
        except (EndpointConnectionError, BotoCoreError, ClientError) as error:
            if (
                not allow_retry
                or attempt >= MAX_RETRY_ATTEMPTS
                or not _is_retryable_exception(error)
            ):
                raise

            _notify_progress(
                progress_callback,
                f"Transient {operation_name} error. Retrying ({attempt + 1}/{MAX_RETRY_ATTEMPTS})...",
            )
            time.sleep(delay)
            delay *= 2


def _is_retryable_exception(error: Exception) -> bool:
    if isinstance(error, EndpointConnectionError):
        return True

    if isinstance(error, BotoCoreError):
        return True

    if isinstance(error, ClientError):
        error_code = str(error.response.get("Error", {}).get("Code", ""))
        return error_code in {
            "RequestTimeout",
            "RequestTimeoutException",
            "Throttling",
            "ThrottlingException",
            "SlowDown",
            "InternalError",
            "ServiceUnavailable",
            "500",
            "503",
        }

    return False


def _notify_progress(callback: Optional[ProgressCallback], message: str) -> None:
    if callback is None:
        return
    callback(message)


def format_bytes(size_bytes: int) -> str:
    if size_bytes < 1024:
        return f"{size_bytes} B"
    units = ["KB", "MB", "GB", "TB", "PB"]
    value = float(size_bytes)
    for unit in units:
        value /= 1024.0
        if value < 1024.0:
            return f"{value:.1f} {unit}"
    return f"{value:.1f} PB"


def _map_copy_client_error(error: ClientError) -> UserVisibleError:
    error_code = str(error.response.get("Error", {}).get("Code", ""))
    if error_code in {"PreconditionFailed", "412"}:
        return DestinationExistsError(
            "Destination object already exists. Overwrite confirmation is required."
        )

    return map_aws_error(error)


def map_aws_error(error: Exception) -> UserVisibleError:
    if isinstance(error, NoCredentialsError):
        return UserVisibleError(
            "AWS credentials are missing. Open Settings to save credentials in Keychain or configure an AWS profile on this Mac."
        )

    if isinstance(error, EndpointConnectionError):
        return UserVisibleError(
            "Network issue: unable to reach AWS endpoint. Check internet/VPN connection and try again."
        )

    if isinstance(error, BotoCoreError):
        return UserVisibleError(
            "Network/transport error while communicating with AWS. Please retry. "
            "If this keeps happening, check VPN/proxy/network stability."
        )

    if isinstance(error, ClientError):
        error_code = str(error.response.get("Error", {}).get("Code", ""))

        if error_code in {"NoSuchKey", "404", "NotFound"}:
            return UserVisibleError("Source file not found in S3.")
        if error_code in {"403", "AccessDenied"}:
            return UserVisibleError(
                "Access denied by AWS. Verify credentials and S3 permissions for read/write access."
            )
        if error_code in {"InvalidAccessKeyId", "SignatureDoesNotMatch", "AuthFailure"}:
            return UserVisibleError(
                "AWS credentials look invalid. Update credentials in Settings and try again."
            )

        message = str(error.response.get("Error", {}).get("Message", "")).strip()
        if message:
            return UserVisibleError(f"AWS error ({error_code}): {message}")
        return UserVisibleError(f"AWS error ({error_code}).")

    return UserVisibleError(f"Unexpected error: {error}")
