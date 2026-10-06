# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Asynchronous Google Cloud Storage implementation of Orbit Storage's contract."""

from __future__ import annotations

import asyncio
import math
from collections.abc import AsyncIterable, Callable, Mapping
from contextlib import suppress
from datetime import datetime
from typing import Any, Protocol, cast

from gcloud.aio.storage import Storage
from orbit_storage import (
    ListObjectsRequest,
    ObjectInfo,
    ObjectPage,
    ObjectStore,
    StorageConfigurationError,
    StorageOperationError,
    StoredObject,
    validate_object_key,
)

from orbit_storage_gcs.config import GCSConfig


class _GCSClient(Protocol):
    """Narrow gcloud-aio interface used by this adapter and its deterministic tests."""

    async def upload(self, bucket: str, object_name: str, file_data: bytes, **kwargs: Any) -> Any:
        """Upload one bounded payload and return the provider object resource."""

    async def download_metadata(self, bucket: str, object_name: str, **kwargs: Any) -> Any:
        """Fetch provider metadata without downloading the object body."""

    async def download_stream(self, bucket: str, object_name: str, **kwargs: Any) -> Any:
        """Open a streamed object response."""

    async def delete(self, bucket: str, object_name: str, **kwargs: Any) -> Any:
        """Delete an object, raising an HTTP error when the request fails."""

    async def list_objects(self, bucket: str, **kwargs: Any) -> Any:
        """Fetch one bounded JSON API page of object metadata."""

    async def close(self) -> None:
        """Close the owned HTTP session and authentication resources."""


class _StreamResponse(Protocol):
    """Subset of gcloud-aio's managed streaming response used by the reader."""

    async def read(self, size: int = -1) -> bytes:
        """Read at most ``size`` response bytes."""

    async def __aenter__(self) -> Any: ...

    async def __aexit__(self, *args: object) -> None: ...


class _GCSBodyReader:
    """Adapt the SDK's managed response stream to Orbit's explicitly closeable reader."""

    def __init__(self, stream: _StreamResponse, chunk_size: int) -> None:
        """Take ownership of the entered provider response until close or EOF."""
        self._stream = stream
        self._chunk_size = chunk_size
        self._closed = False

    def __aiter__(self) -> _GCSBodyReader:
        """Return this asynchronous byte reader."""
        return self

    async def __anext__(self) -> bytes:
        """Read a bounded chunk and release the response automatically at EOF."""
        if self._closed:
            raise StopAsyncIteration
        try:
            chunk = await self._stream.read(self._chunk_size)
        except asyncio.CancelledError:
            raise
        except Exception:
            await self.aclose()
            raise StorageOperationError(
                "GCS object download failed while reading the body."
            ) from None
        if not chunk:
            await self.aclose()
            raise StopAsyncIteration
        if not isinstance(chunk, bytes):
            await self.aclose()
            raise StorageOperationError("GCS response returned a non-byte object body.")
        return chunk

    async def aclose(self) -> None:
        """Close the SDK response once, returning the connection to its pool."""
        if not self._closed:
            try:
                await self._stream.__aexit__(None, None, None)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise StorageOperationError("GCS response stream close failed.") from None
            self._closed = True


class GCSObjectStore(ObjectStore):
    """Lifecycle-managed GCS adapter with bounded async upload buffering."""

    def __init__(
        self,
        config: GCSConfig,
        *,
        client_factory: Callable[..., _GCSClient] | None = None,
    ) -> None:
        """Bind settings and lazily construct the SDK client on first use."""
        if not isinstance(config, GCSConfig):
            raise TypeError("GCSObjectStore requires a GCSConfig instance.")
        self.config = config
        self._client_factory = Storage if client_factory is None else client_factory
        self._client: _GCSClient | None = None
        self._client_lock = asyncio.Lock()
        self._upload_slots = asyncio.Semaphore(config.max_concurrent_uploads)
        self._closed = False

    async def _get_client(self) -> _GCSClient:
        """Create one SDK client lazily and retain it until adapter shutdown."""
        async with self._client_lock:
            if self._closed:
                raise StorageOperationError("GCS object store is closed.")
            if self._client is None:
                try:
                    options: dict[str, Any] = {}
                    if self.config.service_file is not None:
                        options["service_file"] = self.config.service_file
                    self._client = cast(_GCSClient, self._client_factory(**options))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise StorageOperationError("GCS client initialization failed.") from None
            return self._client

    def _key(self, key: str) -> str:
        """Apply the capability's opaque, bounded key policy before provider calls."""
        try:
            return validate_object_key(key)
        except ValueError:
            raise StorageConfigurationError("GCS object key is invalid.") from None

    @staticmethod
    def _status(error: Exception) -> int | None:
        """Extract only an HTTP status from an SDK exception, never its credentialed URL."""
        status = getattr(error, "status", None)
        if status is None:
            status = getattr(error, "status_code", None)
        return status if isinstance(status, int) and not isinstance(status, bool) else None

    @classmethod
    def _not_found(cls, error: Exception) -> bool:
        """Recognize only the provider's explicit HTTP 404 response."""
        return cls._status(error) == 404

    @staticmethod
    def _timestamp(value: object) -> datetime | None:
        """Parse GCS's RFC 3339 timestamp without accepting a timezone-free value."""
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("GCS updated timestamp must be text.")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError("GCS updated timestamp must include a timezone.")
        return parsed

    @classmethod
    def _info(
        cls,
        key: str,
        raw: Mapping[str, Any],
        *,
        fallback_size: int | None = None,
        fallback_content_type: str | None = None,
        fallback_metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo:
        """Normalize a GCS JSON object resource through the capability's model."""
        try:
            raw_size = raw.get("size", fallback_size)
            if isinstance(raw_size, bool):
                raise ValueError("GCS size cannot be boolean.")
            if isinstance(raw_size, str):
                if not raw_size.isdecimal():
                    raise ValueError("GCS size must be a non-negative integer.")
                size = int(raw_size)
            else:
                size = raw_size
            metadata = raw.get("metadata", fallback_metadata or {})
            if not isinstance(metadata, Mapping):
                raise ValueError
            return ObjectInfo(
                key=key,
                size_bytes=size,
                content_type=raw.get("contentType", fallback_content_type),
                etag=raw.get("etag"),
                modified_at=cls._timestamp(raw.get("updated")),
                metadata=metadata,
            )
        except Exception:
            raise StorageOperationError("GCS returned invalid object metadata.") from None

    async def _buffer(self, body: AsyncIterable[bytes]) -> bytes:
        """Collect an async upload within its configured per-operation memory ceiling."""
        data = bytearray()
        try:
            async for chunk in body:
                if not isinstance(chunk, bytes):
                    raise StorageConfigurationError("GCS upload streams must yield byte chunks.")
                if len(data) + len(chunk) > self.config.max_upload_bytes:
                    raise StorageConfigurationError(
                        "GCS upload exceeds the configured maximum payload size."
                    )
                data.extend(chunk)
        except asyncio.CancelledError:
            raise
        except StorageConfigurationError:
            raise
        except Exception:
            raise StorageOperationError("GCS upload stream could not be read.") from None
        return bytes(data)

    async def put(
        self,
        key: str,
        body: bytes | AsyncIterable[bytes],
        *,
        content_type: str | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> ObjectInfo:
        """Upload bytes or a bounded async stream and return normalized object metadata."""
        object_key = self._key(key)
        try:
            validated = ObjectInfo(
                key=object_key,
                size_bytes=0,
                content_type=content_type,
                metadata=metadata or {},
            )
        except Exception:
            raise StorageConfigurationError("GCS upload metadata is invalid.") from None
        async with self._upload_slots:
            if isinstance(body, bytes):
                payload = body
            elif hasattr(body, "__aiter__"):
                payload = await self._buffer(body)
            else:
                raise StorageConfigurationError(
                    "GCS upload body must be bytes or an async byte stream."
                )
            if len(payload) > self.config.max_upload_bytes:
                raise StorageConfigurationError(
                    "GCS upload exceeds the configured maximum payload size."
                )
            client = await self._get_client()
            try:
                response = await client.upload(
                    self.config.bucket,
                    object_key,
                    payload,
                    content_type=validated.content_type,
                    metadata=dict(validated.metadata),
                    timeout=math.ceil(self.config.request_timeout_seconds),
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                raise StorageOperationError("GCS upload operation failed.") from None
        if not isinstance(response, Mapping):
            raise StorageOperationError("GCS upload returned an invalid object response.")
        return self._info(
            object_key,
            response,
            fallback_size=len(payload),
            fallback_content_type=validated.content_type,
            fallback_metadata=validated.metadata,
        )

    async def get(self, key: str) -> StoredObject | None:
        """Return metadata with a generation-pinned, closeable GCS download stream."""
        object_key = self._key(key)
        client = await self._get_client()
        try:
            raw = await client.download_metadata(
                self.config.bucket,
                object_key,
                timeout=math.ceil(self.config.request_timeout_seconds),
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._not_found(error):
                return None
            raise StorageOperationError("GCS object metadata request failed.") from None
        if not isinstance(raw, Mapping):
            raise StorageOperationError("GCS returned invalid object metadata.")
        info = self._info(object_key, raw)
        generation = raw.get("generation")
        headers: dict[str, str] = {}
        if generation is not None:
            if not isinstance(generation, (str, int)) or isinstance(generation, bool):
                raise StorageOperationError("GCS returned invalid object generation metadata.")
            generation_value = str(generation)
            if not generation_value.isdecimal():
                raise StorageOperationError("GCS returned invalid object generation metadata.")
            headers["x-goog-if-generation-match"] = generation_value
        stream: _StreamResponse | None = None
        try:
            stream = cast(
                _StreamResponse,
                await client.download_stream(
                    self.config.bucket,
                    object_key,
                    headers=headers,
                    timeout=math.ceil(self.config.request_timeout_seconds),
                ),
            )
            await stream.__aenter__()
        except asyncio.CancelledError:
            if stream is not None:
                with suppress(Exception):
                    await stream.__aexit__(None, None, None)
            raise
        except Exception as error:
            if stream is not None:
                with suppress(Exception):
                    await stream.__aexit__(None, None, None)
            if self._not_found(error):
                return None
            raise StorageOperationError("GCS object download could not be opened.") from None
        return StoredObject(info=info, body=_GCSBodyReader(stream, self.config.read_chunk_size))

    async def delete(self, key: str) -> bool:
        """Delete an object and return false only when GCS reports it missing."""
        object_key = self._key(key)
        client = await self._get_client()
        try:
            await client.delete(
                self.config.bucket,
                object_key,
                timeout=math.ceil(self.config.request_timeout_seconds),
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if self._not_found(error):
                return False
            raise StorageOperationError("GCS delete operation failed.") from None
        return True

    async def list_page(
        self,
        prefix: str = "",
        *,
        limit: int = 100,
        continuation_token: str | None = None,
    ) -> ObjectPage:
        """Fetch one bounded GCS page using its opaque nextPageToken."""
        try:
            request = ListObjectsRequest(
                prefix=prefix, limit=limit, continuation_token=continuation_token
            )
        except Exception:
            raise StorageConfigurationError("GCS list request is invalid.") from None
        params = {"prefix": request.prefix, "maxResults": str(request.limit), "projection": "full"}
        if request.continuation_token is not None:
            params["pageToken"] = request.continuation_token
        client = await self._get_client()
        try:
            response = await client.list_objects(
                self.config.bucket,
                params=params,
                timeout=math.ceil(self.config.request_timeout_seconds),
            )
            if not isinstance(response, Mapping):
                raise ValueError
            raw_items = response.get("items", [])
            if not isinstance(raw_items, list):
                raise ValueError
            items: list[ObjectInfo] = []
            for raw in raw_items:
                if not isinstance(raw, Mapping) or not isinstance(raw.get("name"), str):
                    raise ValueError
                items.append(self._info(raw["name"], raw))
            token = response.get("nextPageToken")
            if token is not None and not isinstance(token, str):
                raise ValueError
            return ObjectPage(items=tuple(items), continuation_token=token)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise StorageOperationError(
                "GCS object listing failed or returned invalid data."
            ) from None

    async def aclose(self) -> None:
        """Close the owned Google client once; repeated shutdown calls are harmless."""
        async with self._client_lock:
            if self._closed:
                return
            self._closed = True
            if self._client is not None:
                try:
                    await self._client.close()
                except asyncio.CancelledError:
                    self._closed = False
                    raise
                except Exception:
                    raise StorageOperationError("GCS client shutdown failed.") from None


__all__ = ["GCSObjectStore"]
