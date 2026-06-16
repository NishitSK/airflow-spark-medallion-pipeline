"""
S3 Client Utility
=================
All Amazon S3 operations for the Medallion Data Pipeline.

Authentication:
  Uses boto3 default credential chain — resolves through the EC2 IAM Role
  (MedallionPipelineRole) via instance metadata.
  DO NOT add AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY anywhere.

Bucket:
  Reads S3_BUCKET from environment variable. All functions are no-ops and
  return safe defaults if S3_BUCKET is not set.

Fail-safe Design:
  Every public function catches all exceptions and returns None/False instead
  of raising. S3 failures NEVER block local pipeline execution.
"""

import os
import io
import logging
import time
from typing import Optional

logger = logging.getLogger("s3_client")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


def _get_bucket() -> Optional[str]:
    """Return the configured S3 bucket name, or None if not set."""
    return os.environ.get("S3_BUCKET", "").strip() or None


def get_s3_client():
    """
    Return a boto3 S3 client using the EC2 IAM Role (instance metadata).
    Uses boto3's default credential resolution — no static keys.
    Returns None if boto3 is unavailable or credentials cannot be resolved.
    """
    try:
        import boto3
        from botocore.config import Config
        fast_config = Config(
            connect_timeout=0.5,
            read_timeout=1.0,
            retries={'max_attempts': 0}
        )
        # Default credential chain: instance metadata → env vars → ~/.aws
        # On EC2 with MedallionPipelineRole, instance metadata is used automatically.
        client = boto3.client(
            "s3",
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            config=fast_config
        )
        return client
    except Exception as e:
        logger.error(f"[S3] Failed to create S3 client: {e}")
        return None


def upload_file(local_path: str, s3_key: str, metadata: dict = None) -> bool:
    """
    Upload a local file to S3.

    Args:
        local_path: Absolute path to the local file.
        s3_key:     S3 object key (e.g. 'raw/uploads/dataset.csv').
        metadata:   Optional dict of S3 object metadata tags.

    Returns:
        True on success, False on failure.
    """
    bucket = _get_bucket()
    if not bucket:
        logger.warning("[S3] S3_BUCKET not configured — skipping upload_file.")
        return False

    if not os.path.exists(local_path):
        logger.warning(f"[S3] Local file not found, cannot upload: {local_path}")
        return False

    try:
        client = get_s3_client()
        if client is None:
            return False

        extra_args = {}
        if metadata:
            extra_args["Metadata"] = {str(k): str(v) for k, v in metadata.items()}

        file_size = os.path.getsize(local_path)
        client.upload_file(local_path, bucket, s3_key, ExtraArgs=extra_args if extra_args else None)
        logger.info(f"[S3] ✅ upload_file: s3://{bucket}/{s3_key} ({file_size:,} bytes)")
        return True

    except Exception as e:
        logger.error(f"[S3] ❌ upload_file failed: s3://{bucket}/{s3_key} — {type(e).__name__}: {e}")
        return False


def upload_bytes(data: bytes, s3_key: str, content_type: str = "application/octet-stream",
                 metadata: dict = None) -> bool:
    """
    Upload bytes (in-memory data) to S3.

    Args:
        data:         Raw bytes to upload.
        s3_key:       S3 object key.
        content_type: MIME type for the object.
        metadata:     Optional dict of S3 object metadata tags.

    Returns:
        True on success, False on failure.
    """
    bucket = _get_bucket()
    if not bucket:
        logger.warning("[S3] S3_BUCKET not configured — skipping upload_bytes.")
        return False

    if not data:
        logger.warning(f"[S3] Empty data provided, skipping upload: {s3_key}")
        return False

    try:
        client = get_s3_client()
        if client is None:
            return False

        put_kwargs = {
            "Bucket": bucket,
            "Key": s3_key,
            "Body": io.BytesIO(data),
            "ContentType": content_type,
        }
        if metadata:
            put_kwargs["Metadata"] = {str(k): str(v) for k, v in metadata.items()}

        client.put_object(**put_kwargs)
        logger.info(f"[S3] ✅ upload_bytes: s3://{bucket}/{s3_key} ({len(data):,} bytes, {content_type})")
        return True

    except Exception as e:
        logger.error(f"[S3] ❌ upload_bytes failed: s3://{bucket}/{s3_key} — {type(e).__name__}: {e}")
        return False


def generate_download_url(s3_key: str, expiry_seconds: int = 3600) -> Optional[str]:
    """
    Generate a presigned HTTPS GET URL for an S3 object.

    Args:
        s3_key:         S3 object key.
        expiry_seconds: URL validity duration (default: 1 hour).

    Returns:
        Presigned URL string, or None on failure.
    """
    bucket = _get_bucket()
    if not bucket:
        logger.warning("[S3] S3_BUCKET not configured — cannot generate presigned URL.")
        return None

    try:
        client = get_s3_client()
        if client is None:
            return None

        url = client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": s3_key},
            ExpiresIn=expiry_seconds,
        )
        logger.info(f"[S3] ✅ presigned URL generated: s3://{bucket}/{s3_key} (expires in {expiry_seconds}s)")
        return url

    except Exception as e:
        logger.error(f"[S3] ❌ generate_download_url failed: s3://{bucket}/{s3_key} — {type(e).__name__}: {e}")
        return None


def object_exists(s3_key: str) -> bool:
    """
    Check if an S3 object exists using a HEAD request.

    Returns:
        True if the object exists, False otherwise.
    """
    bucket = _get_bucket()
    if not bucket:
        return False

    try:
        client = get_s3_client()
        if client is None:
            return False

        client.head_object(Bucket=bucket, Key=s3_key)
        return True

    except Exception as e:
        # 404 raises ClientError with code '404' or 'NoSuchKey'
        err_code = getattr(getattr(e, "response", {}), "get", lambda k, d=None: d)("Error", {}).get("Code", "")
        if err_code in ("404", "NoSuchKey"):
            return False
        # Other errors (permissions, etc.) — log and return False
        logger.warning(f"[S3] object_exists check failed: s3://{bucket}/{s3_key} — {type(e).__name__}: {e}")
        return False


def get_object_metadata(s3_key: str) -> Optional[dict]:
    """
    Retrieve metadata (size, last_modified, etag) for an S3 object.

    Returns:
        Dict with keys: size_bytes, size_mb, last_modified, etag — or None on failure.
    """
    bucket = _get_bucket()
    if not bucket:
        return None

    try:
        client = get_s3_client()
        if client is None:
            return None

        response = client.head_object(Bucket=bucket, Key=s3_key)
        size_bytes = response.get("ContentLength", 0)
        return {
            "bucket":        bucket,
            "key":           s3_key,
            "s3_uri":        f"s3://{bucket}/{s3_key}",
            "size_bytes":    size_bytes,
            "size_mb":       round(size_bytes / (1024 * 1024), 4),
            "last_modified": response.get("LastModified"),
            "etag":          response.get("ETag", "").strip('"'),
            "content_type":  response.get("ContentType", ""),
        }

    except Exception as e:
        logger.debug(f"[S3] get_object_metadata: {s3_key} not found or error — {e}")
        return None


def list_prefix(prefix: str) -> list:
    """
    List all S3 objects under a given prefix.

    Returns:
        List of dicts with key, size_bytes, last_modified — or empty list.
    """
    bucket = _get_bucket()
    if not bucket:
        return []

    try:
        client = get_s3_client()
        if client is None:
            return []

        paginator = client.get_paginator("list_objects_v2")
        results = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                results.append({
                    "key":           obj["Key"],
                    "size_bytes":    obj["Size"],
                    "last_modified": obj["LastModified"],
                })
        return results

    except Exception as e:
        logger.error(f"[S3] list_prefix failed: s3://{bucket}/{prefix} — {type(e).__name__}: {e}")
        return []


# ------------------------------------------------------------------ #
# Convenience composite operations
# ------------------------------------------------------------------ #

def export_csv_bytes_to_s3(csv_bytes: bytes, s3_key: str, run_id: str = "", row_count: int = 0) -> bool:
    """Upload a CSV payload to S3 with standard metadata tags."""
    return upload_bytes(
        data=csv_bytes,
        s3_key=s3_key,
        content_type="text/csv",
        metadata={
            "run_id":    run_id,
            "row_count": str(row_count),
            "exported_at": str(int(time.time())),
        }
    )


def export_text_to_s3(text: str, s3_key: str, run_id: str = "") -> bool:
    """Upload a plain text payload to S3."""
    return upload_bytes(
        data=text.encode("utf-8") if isinstance(text, str) else text,
        s3_key=s3_key,
        content_type="text/plain",
        metadata={"run_id": run_id, "exported_at": str(int(time.time()))},
    )


def download_object_bytes(s3_key: str) -> Optional[bytes]:
    """
    Download an object from S3 and return its body as bytes.
    """
    bucket = _get_bucket()
    if not bucket:
        logger.warning("[S3] S3_BUCKET not configured — cannot download object.")
        return None

    try:
        client = get_s3_client()
        if client is None:
            return None

        response = client.get_object(Bucket=bucket, Key=s3_key)
        data = response["Body"].read()
        logger.info(f"[S3] ✅ download_object_bytes: s3://{bucket}/{s3_key} ({len(data):,} bytes downloaded)")
        return data

    except Exception as e:
        logger.error(f"[S3] ❌ download_object_bytes failed: s3://{bucket}/{s3_key} — {type(e).__name__}: {e}")
        return None

