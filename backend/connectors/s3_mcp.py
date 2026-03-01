"""
FlowForge — Amazon S3 MCP Connector
Wraps boto3 for S3 operations with assertions.
"""

import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.exceptions import ClientError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False
    logger.warning("boto3 not installed. Run: pip install boto3")


class S3Connector:
    """MCP connector for Amazon S3."""

    def __init__(
        self,
        access_key_id: str = None,
        secret_access_key: str = None,
        region: str = "us-east-1",
        endpoint_url: str = None,          # for MinIO / LocalStack
        credential_name: str = None,
    ):
        if not BOTO3_AVAILABLE:
            raise RuntimeError("boto3 not installed.")

        self.credential_name = credential_name
        self.region          = region

        kwargs = {"region_name": region}
        if access_key_id and secret_access_key:
            kwargs["aws_access_key_id"]     = access_key_id
            kwargs["aws_secret_access_key"] = secret_access_key
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url

        self._s3 = boto3.client("s3", **kwargs)
        logger.info(f"S3Connector initialized (region={region}, credential={credential_name})")

    # ── Object Operations ────────────────────────────────────────────────────

    def upload_file(self, local_path: str, bucket: str, key: str) -> str:
        """Upload a local file to S3. Returns s3:// URI."""
        self._s3.upload_file(local_path, bucket, key)
        uri = f"s3://{bucket}/{key}"
        logger.info(f"Uploaded {local_path} → {uri}")
        return uri

    def upload_bytes(self, data: bytes, bucket: str, key: str, content_type: str = "application/octet-stream") -> str:
        import io
        self._s3.upload_fileobj(io.BytesIO(data), bucket, key, ExtraArgs={"ContentType": content_type})
        return f"s3://{bucket}/{key}"

    def download_file(self, bucket: str, key: str, local_path: str) -> str:
        """Download an S3 object to local_path. Returns local_path."""
        os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
        self._s3.download_file(bucket, key, local_path)
        logger.info(f"Downloaded s3://{bucket}/{key} → {local_path}")
        return local_path

    def read_object(self, bucket: str, key: str) -> bytes:
        """Return raw bytes of S3 object."""
        resp = self._s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()

    def list_objects(self, bucket: str, prefix: str = "", max_results: int = 1000) -> List[dict]:
        """List objects in bucket (paginated)."""
        paginator = self._s3.get_paginator("list_objects_v2")
        items = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                items.append({"key": obj["Key"], "size": obj["Size"], "last_modified": str(obj["LastModified"])})
        return items[:max_results]

    def object_exists(self, bucket: str, key: str) -> bool:
        try:
            self._s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def delete_object(self, bucket: str, key: str) -> bool:
        self._s3.delete_object(Bucket=bucket, Key=key)
        return True

    def get_object_metadata(self, bucket: str, key: str) -> dict:
        resp = self._s3.head_object(Bucket=bucket, Key=key)
        return {"size": resp["ContentLength"], "last_modified": str(resp["LastModified"]), "etag": resp["ETag"]}

    # ── Assertions ────────────────────────────────────────────────────────────

    def assert_object_exists(self, bucket: str, key: str):
        if not self.object_exists(bucket, key):
            raise AssertionError(f"S3 object not found: s3://{bucket}/{key}")

    def assert_object_size_gt(self, bucket: str, key: str, min_bytes: int):
        meta = self.get_object_metadata(bucket, key)
        if meta["size"] <= min_bytes:
            raise AssertionError(f"S3 object size {meta['size']} not > {min_bytes}")

    def assert_object_count(self, bucket: str, prefix: str, expected: int):
        items = self.list_objects(bucket, prefix)
        if len(items) != expected:
            raise AssertionError(f"Expected {expected} objects at s3://{bucket}/{prefix}, found {len(items)}")

    # ── Health Check ─────────────────────────────────────────────────────────

    def health_check(self) -> dict:
        try:
            self._s3.list_buckets()
            return {"status": "ok"}
        except Exception as e:
            return {"status": "error", "error": str(e)}
