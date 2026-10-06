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

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
from orbit import Application, ApplicationConfig
from orbit_storage import (
    OBJECT_STORE_DEPENDENCY_KEY,
    StorageConfigurationError,
    StorageOperationError,
)

from orbit_storage_gcs import GCSConfig, GCSObjectStore, GCSObjectStorePlugin


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

    async def upload(
        self, bucket: str, object_name: str, file_data: bytes, **kwargs: Any
    ) -> dict[str, Any]:
        resource = {
            "name": object_name,
            "size": str(len(file_data)),
            "contentType": kwargs.get("content_type"),
            "etag": '"gcs-etag"',
            "updated": datetime.now(UTC).isoformat(),
            "generation": "123",
            "metadata": kwargs.get("metadata", {}),
        }
        self.objects[object_name] = {"body": file_data, **resource}
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
    with pytest.raises(StorageConfigurationError, match="request_timeout_seconds"):
        GCSConfig(bucket="bucket", request_timeout_seconds=float("inf"))
    with pytest.raises(StorageConfigurationError, match="bucket"):
        GCSConfig(bucket="")


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
    await store.put("lifecycle", b"initialized")
    await app.stop()
    assert client.closed
