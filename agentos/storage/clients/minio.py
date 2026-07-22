from __future__ import annotations

import asyncio
import hashlib
import io
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any

from minio import Minio
from minio.error import S3Error
from minio.versioningconfig import ENABLED, VersioningConfig

from agentos.config.settings import Settings


@dataclass(frozen=True)
class ObjectMetadata:
    bucket: str
    object_name: str
    etag: str
    version_id: str | None
    size: int
    sha256: str

    @property
    def uri(self) -> str:
        suffix = f"?versionId={self.version_id}" if self.version_id else ""
        return f"minio://{self.bucket}/{self.object_name}{suffix}"


class MinioObjectClient:
    """Async facade over the official thread-safe MinIO Python client."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key.get_secret_value(),
            secret_key=settings.minio_secret_key.get_secret_value(),
            secure=settings.minio_secure,
            region=settings.minio_region,
        )

    @staticmethod
    def _validate_object_name(object_name: str) -> str:
        normalized = object_name.replace("\\", "/").lstrip("/")
        path = PurePosixPath(normalized)
        if not normalized or any(part in {"", ".", ".."} for part in path.parts):
            raise ValueError("unsafe object name")
        return path.as_posix()

    async def ensure_bucket(self, bucket: str) -> None:
        exists = await asyncio.to_thread(self.client.bucket_exists, bucket)
        if not exists:
            try:
                await asyncio.to_thread(self.client.make_bucket, bucket, self.settings.minio_region)
            except S3Error as error:
                if error.code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    raise
        await asyncio.to_thread(
            self.client.set_bucket_versioning,
            bucket,
            VersioningConfig(ENABLED),
        )

    async def initialize(self) -> None:
        await self.ensure_bucket(self.settings.minio_artifacts_bucket)
        await self.ensure_bucket(self.settings.minio_memory_bucket)

    async def healthcheck(self) -> dict[str, Any]:
        buckets = await asyncio.to_thread(self.client.list_buckets)
        return {
            "service": "minio",
            "healthy": True,
            "buckets": sorted(bucket.name for bucket in buckets),
        }

    async def put_bytes(
        self,
        *,
        bucket: str,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
        metadata: dict[str, str] | None = None,
    ) -> ObjectMetadata:
        await self.ensure_bucket(bucket)
        safe_name = self._validate_object_name(object_name)
        digest = hashlib.sha256(data).hexdigest()
        object_metadata: dict[str, str | list[str] | tuple[str]] = {
            "sha256": digest,
            **(metadata or {}),
        }
        result = await asyncio.to_thread(
            self.client.put_object,
            bucket,
            safe_name,
            io.BytesIO(data),
            len(data),
            content_type,
            object_metadata,
        )
        return ObjectMetadata(
            bucket=bucket,
            object_name=safe_name,
            etag=result.etag or "",
            version_id=result.version_id,
            size=len(data),
            sha256=digest,
        )

    async def get_bytes(
        self, *, bucket: str, object_name: str, version_id: str | None = None
    ) -> bytes:
        safe_name = self._validate_object_name(object_name)

        def read() -> bytes:
            response = self.client.get_object(bucket, safe_name, version_id=version_id)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        return await asyncio.to_thread(read)

    async def stat(
        self, *, bucket: str, object_name: str, version_id: str | None = None
    ) -> ObjectMetadata:
        safe_name = self._validate_object_name(object_name)
        result = await asyncio.to_thread(
            self.client.stat_object, bucket, safe_name, version_id=version_id
        )
        metadata = result.metadata or {}
        digest = metadata.get("x-amz-meta-sha256") or metadata.get("sha256") or ""
        return ObjectMetadata(
            bucket=bucket,
            object_name=safe_name,
            etag=result.etag or "",
            version_id=result.version_id,
            size=int(result.size or 0),
            sha256=digest,
        )
