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
"""Fake-client tests for GCS mapping, bounds, and Core lifecycle ownership."""

from __future__ import annotations

import asyncio
import tomllib
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, BinaryIO

import pytest
from orbit import Application, ApplicationConfig
from orbit_storage import (
    OBJECT_STORE_DEPENDENCY_KEY,
    StorageConfigurationError,
    StorageOperationError,
)

from orbit_storage_gcs import GCSConfig, GCSObjectStore
from orbit_storage_gcs.plugin import GCSObjectStorePlugin


def test_core_lifecycle_dependency_is_optional() -> None:
    """The object store can be used without Core unless its plugin extra is installed."""
    manifest = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text(encoding="utf-8")
    )["project"]
    assert not any(dep.startswith("orbit-core") for dep in manifest["dependencies"])
    assert manifest["optional-dependencies"]["core"] == ["orbit-core>=0.1.0a1,<0.2"]


class FakeHTTPError(Exception):
    """Provider-shaped exception used to exercise status-code translation."""

    def __init__(self, status: int) -> None:
        self.status = status
        super().__init__(f"HTTP {status}")


class FakeStream:
    """Small managed body stream with deterministic read and close behavior."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.entered = False
        self.closed = False

    async def __aenter__(self) -> FakeStream:
        self.entered = True
        return self

    async def __aexit__(self, *_: object) -> None:
        self.closed = True

    async def read(self, size: int = -1) -> bytes:
        chunk, self.payload = self.payload[:size], self.payload[size:]
        return chunk


class FakeClient:
    """In-memory asynchronous subset of the gcloud-aio Storage client."""

    def __init__(self) -> None:
        self.objects: dict[str, dict[str, Any]] = {}
        self.closed = False
        self.stream: FakeStream | None = None
        self.download_headers: dict[str, str] | None = None
        self.last_list_params: dict[str, str] | None = None
        self.last_upload_force_resumable: bool | None = None
        self.last_upload_spooled: bool = False
        self.last_upload_file: BinaryIO | None = None

    async def upload(
        self, bucket: str, object_name: str, file_data: bytes | BinaryIO, **kwargs: Any
    ) -> dict[str, Any]:
        self.last_upload_force_resumable = kwargs.get("force_resumable_upload")
        self.last_upload_spooled = bool(getattr(file_data, "_rolled", False))
        self.last_upload_file = file_data if hasattr(file_data, "read") else None
        payload = file_data.read() if hasattr(file_data, "read") else file_data
        assert isinstance(payload, bytes)
        resource = {
            "name": object_name,
            "size": str(len(payload)),
            "contentType": kwargs.get("content_type"),
            "etag": '"gcs-etag"',
            "updated": datetime.now(UTC).isoformat(),
            "generation": "123",
            "metadata": kwargs.get("metadata", {}),
        }
        self.objects[object_name] = {"body": payload, **resource}
        return resource

    async def download_metadata(self, bucket: str, object_name: str, **kwargs: Any) -> Any:
        if object_name not in self.objects:
            raise FakeHTTPError(404)
        return {key: value for key, value in self.objects[object_name].items() if key != "body"}

    async def download_stream(self, bucket: str, object_name: str, **kwargs: Any) -> FakeStream:
        if object_name not in self.objects:
            raise FakeHTTPError(404)
        self.download_headers = kwargs.get("headers")
        self.stream = FakeStream(self.objects[object_name]["body"])
        return self.stream

    async def delete(self, bucket: str, object_name: str, **kwargs: Any) -> str:
        if object_name not in self.objects:
            raise FakeHTTPError(404)
        del self.objects[object_name]
        return ""

    async def list_objects(self, bucket: str, **kwargs: Any) -> dict[str, Any]:
        params = kwargs.get("params", {})
        self.last_list_params = params
        names = sorted(name for name in self.objects if name.startswith(params.get("prefix", "")))
        offset = int(params.get("pageToken", "0"))
        page = names[offset : offset + int(params["maxResults"])]
        next_offset = offset + len(page)
        return {
            "items": [
                {key: value for key, value in self.objects[name].items() if key != "body"}
                for name in page
            ],
            "nextPageToken": str(next_offset) if next_offset < len(names) else None,
        }

    async def close(self) -> None:
        self.closed = True


def make_store(client: FakeClient, **config: Any) -> GCSObjectStore:
    """Build an adapter with a fake SDK client and validated test settings."""
    settings = {"bucket": "orbit-test", **config}
    return GCSObjectStore(GCSConfig(**settings), client_factory=lambda **_: client)


def test_config_rejects_invalid_transfer_limits() -> None:
    with pytest.raises(StorageConfigurationError, match="max_upload_bytes"):
        GCSConfig(bucket="bucket", max_upload_bytes=0)
    with pytest.raises(StorageConfigurationError, match="max_concurrent_downloads"):
        GCSConfig(bucket="bucket", max_concurrent_downloads=0)
    with pytest.raises(StorageConfigurationError, match="request_timeout_seconds"):
        GCSConfig(bucket="bucket", request_timeout_seconds=float("inf"))
    with pytest.raises(StorageConfigurationError, match="bucket"):
        GCSConfig(bucket="")
    with pytest.raises(StorageConfigurationError, match="aggregate in-memory"):
        GCSConfig(
            bucket="bucket",
            max_concurrent_uploads=128,
            upload_memory_threshold_bytes=1024 * 1024,
        )
    with pytest.raises(StorageConfigurationError, match="max_retry_attempts"):
        GCSConfig(bucket="bucket", max_retry_attempts=0)
    with pytest.raises(StorageConfigurationError, match="cannot exceed"):
        GCSConfig(
            bucket="bucket",
            retry_initial_delay_seconds=2,
            retry_max_delay_seconds=1,
        )


@pytest.mark.asyncio
async def test_idempotent_reads_retry_transient_failures_with_bounded_attempts() -> None:
    class FlakyClient(FakeClient):
        metadata_attempts = 0
        list_attempts = 0

        async def download_metadata(self, bucket: str, object_name: str, **kwargs: Any) -> Any:
            self.metadata_attempts += 1
            if self.metadata_attempts == 1:
                raise FakeHTTPError(503)
            return await super().download_metadata(bucket, object_name, **kwargs)

        async def list_objects(self, bucket: str, **kwargs: Any) -> dict[str, Any]:
            self.list_attempts += 1
            if self.list_attempts == 1:
                raise FakeHTTPError(429)
            return await super().list_objects(bucket, **kwargs)

    client = FlakyClient()
    store = make_store(
        client,
        retry_initial_delay_seconds=0,
        retry_max_delay_seconds=0,
    )
    await store.put("read-retry", b"ok")

    result = await store.get("read-retry")
    assert result is not None
    await result.aclose()
    page = await store.list_page()

    assert [item.key for item in page.items] == ["read-retry"]
    assert client.metadata_attempts == 2
    assert client.list_attempts == 2
    await store.aclose()


@pytest.mark.asyncio
async def test_read_retries_stop_at_configured_attempt_limit() -> None:
    class UnavailableClient(FakeClient):
        attempts = 0

        async def list_objects(self, bucket: str, **kwargs: Any) -> Any:
            self.attempts += 1
            raise FakeHTTPError(503)

    client = UnavailableClient()
    store = make_store(
        client,
        max_retry_attempts=2,
        retry_initial_delay_seconds=0,
        retry_max_delay_seconds=0,
    )
    with pytest.raises(StorageOperationError, match="listing failed"):
        await store.list_page()
    assert client.attempts == 2
    await store.aclose()


@pytest.mark.asyncio
async def test_put_get_delete_and_client_lifecycle() -> None:
    client = FakeClient()
    store = make_store(client)
    info = await store.put("reports/today.json", b"hello", content_type="application/json")
    assert info.size_bytes == 5
    assert info.etag == '"gcs-etag"'
    stored = await store.get("reports/today.json")
    assert stored is not None
    async with stored:
        assert [chunk async for chunk in stored.body] == [b"hello"]
    assert client.stream is not None and client.stream.closed
    assert client.download_headers == {"x-goog-if-generation-match": "123"}
    assert await store.delete("reports/today.json")
    assert not await store.delete("reports/today.json")
    assert await store.get("missing") is None
    await store.aclose()
    assert client.closed
    await store.aclose()


@pytest.mark.asyncio
async def test_download_slots_bound_open_stream_lifetimes() -> None:
    client = FakeClient()
    store = make_store(client, max_concurrent_downloads=1)
    await store.put("first", b"one")
    await store.put("second", b"two")

    first = await store.get("first")
    assert first is not None
    first_stream = client.stream
    second_task = asyncio.create_task(store.get("second"))
    await asyncio.sleep(0)
    assert not second_task.done()

    await first.aclose()
    second = await asyncio.wait_for(second_task, timeout=1)
    assert second is not None
    assert first_stream is not None and first_stream.closed
    await second.aclose()
    await store.aclose()


@pytest.mark.asyncio
async def test_missing_download_releases_its_slot() -> None:
    client = FakeClient()
    store = make_store(client, max_concurrent_downloads=1)
    assert await asyncio.wait_for(store.get("missing"), timeout=1) is None
    await store.put("present", b"ok")
    stored = await asyncio.wait_for(store.get("present"), timeout=1)
    assert stored is not None
    await stored.aclose()
    await store.aclose()


@pytest.mark.asyncio
async def test_cancelled_body_read_closes_stream_and_releases_slot() -> None:
    read_started = asyncio.Event()

    class WaitingStream(FakeStream):
        async def read(self, size: int = -1) -> bytes:
            read_started.set()
            await asyncio.Event().wait()
            return b""

    class WaitingClient(FakeClient):
        async def download_stream(
            self, bucket: str, object_name: str, **kwargs: Any
        ) -> WaitingStream:
            self.download_headers = kwargs.get("headers")
            stream = WaitingStream(self.objects[object_name]["body"])
            self.stream = stream
            return stream

    client = WaitingClient()
    store = make_store(client, max_concurrent_downloads=1)
    await store.put("waiting", b"data")
    stored = await store.get("waiting")
    assert stored is not None
    body_task = asyncio.create_task(stored.body.__anext__())
    await read_started.wait()
    body_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await body_task

    assert client.stream is not None and client.stream.closed
    again = await asyncio.wait_for(store.get("waiting"), timeout=1)
    assert again is not None
    await again.aclose()
    await store.aclose()


@pytest.mark.asyncio
async def test_failed_client_close_is_retryable_and_keeps_store_unavailable() -> None:
    client = FakeClient()
    store = make_store(client)
    await store._get_client()
    attempts = 0

    async def fail_once() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary close failure")
        client.closed = True

    client.close = fail_once
    with pytest.raises(StorageOperationError, match="shutdown failed"):
        await store.aclose()
    with pytest.raises(StorageOperationError, match="closed"):
        await store.put("after-close", b"value")

    await store.aclose()

    assert attempts == 2
    assert client.closed


@pytest.mark.asyncio
async def test_streamed_upload_respects_payload_ceiling() -> None:
    client = FakeClient()
    store = make_store(client, max_upload_bytes=5)

    async def chunks() -> AsyncIterator[bytes]:
        yield b"123"
        yield b"456"

    with pytest.raises(StorageConfigurationError, match="maximum payload"):
        await store.put("too-large", chunks())
    assert client.objects == {}
    await store.aclose()


@pytest.mark.asyncio
async def test_streamed_upload_spills_to_disk_and_forces_resumable_sdk_path() -> None:
    client = FakeClient()
    store = make_store(
        client,
        upload_memory_threshold_bytes=3,
        max_upload_bytes=32,
    )

    async def body() -> AsyncIterator[bytes]:
        yield b"ab"
        yield b"cde"
        yield b"fghi"

    result = await store.put("large-object", body(), metadata={"purpose": "test"})

    assert result.size_bytes == 9
    assert client.objects["large-object"]["body"] == b"abcdefghi"
    assert client.last_upload_spooled
    assert client.last_upload_force_resumable is True
    await store.aclose()


@pytest.mark.asyncio
async def test_cancelled_upload_closes_temporary_spool() -> None:
    entered = asyncio.Event()

    class WaitingClient(FakeClient):
        async def upload(
            self, bucket: str, object_name: str, file_data: bytes | BinaryIO, **kwargs: Any
        ) -> dict[str, Any]:
            self.last_upload_file = file_data if hasattr(file_data, "read") else None
            entered.set()
            await asyncio.Event().wait()

    client = WaitingClient()
    store = make_store(client, upload_memory_threshold_bytes=3)

    async def body() -> AsyncIterator[bytes]:
        yield b"abcdef"

    task = asyncio.create_task(store.put("cancelled", body()))
    await entered.wait()
    assert client.last_upload_file is not None
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert client.last_upload_file.closed
    await store.aclose()


@pytest.mark.asyncio
async def test_streamed_upload_maps_custom_metadata() -> None:
    client = FakeClient()
    store = make_store(client)

    async def chunks() -> AsyncIterator[bytes]:
        yield b"abc"
        yield b"def"

    info = await store.put("streamed", chunks(), metadata={"team": "runtime"})
    assert info.size_bytes == 6
    assert dict(info.metadata) == {"team": "runtime"}
    assert client.objects["streamed"]["body"] == b"abcdef"
    await store.aclose()


@pytest.mark.asyncio
async def test_list_page_maps_gcs_token_and_bounded_metadata() -> None:
    client = FakeClient()
    store = make_store(client)
    await store.put("docs/a", b"a")
    await store.put("docs/b", b"b")
    first = await store.list_page("docs/", limit=1)
    assert [item.key for item in first.items] == ["docs/a"]
    assert first.continuation_token == "1"
    second = await store.list_page("docs/", limit=1, continuation_token="1")
    assert [item.key for item in second.items] == ["docs/b"]
    assert client.last_list_params == {
        "prefix": "docs/",
        "maxResults": "1",
        "projection": "full",
        "pageToken": "1",
    }
    await store.aclose()


@pytest.mark.asyncio
async def test_provider_failures_are_sanitized() -> None:
    class BrokenClient(FakeClient):
        async def upload(
            self, bucket: str, object_name: str, file_data: bytes, **kwargs: Any
        ) -> Any:
            raise RuntimeError("https://user:secret@example.test/?token=secret")

    store = make_store(BrokenClient())
    with pytest.raises(StorageOperationError) as error:
        await store.put("valid", b"content")
    assert "secret" not in str(error.value)
    await store.aclose()


@pytest.mark.asyncio
async def test_plugin_registers_store_and_closes_client() -> None:
    client = FakeClient()
    store = make_store(client)
    app = Application(ApplicationConfig(name="gcs-plugin-test"))
    app.plugins.register(GCSObjectStorePlugin(store))
    await app.startup()
    assert await app.container.aresolve(OBJECT_STORE_DEPENDENCY_KEY) is store
    result = await app.plugins.plugins[0].control_actions[0].handler({})
    assert result is not None and result.summary == "GCS bucket listing is available."
    await store.put("lifecycle", b"initialized")
    await app.stop()
    assert client.closed
