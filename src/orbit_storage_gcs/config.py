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
"""Validated bucket, authentication-file, and bounded transfer settings for GCS."""

from __future__ import annotations

import math
from dataclasses import dataclass

from orbit_storage import StorageConfigurationError


@dataclass(frozen=True, slots=True)
class GCSConfig:
    """Google Cloud Storage settings; credentials use Application Default Credentials by default.

    ``service_file`` is a path to a service-account JSON file, never the credential contents.
    Upload capacity is bounded per concurrent upload to keep async-stream buffering predictable.
    """

    bucket: str
    service_file: str | None = None
    request_timeout_seconds: float = 30.0
    read_chunk_size: int = 64 * 1024
    max_upload_bytes: int = 64 * 1024 * 1024
    max_concurrent_uploads: int = 4

    def __post_init__(self) -> None:
        """Reject unbounded and malformed options without exposing credential paths in errors."""
        if not isinstance(self.bucket, str) or not self.bucket.strip() or len(self.bucket) > 222:
            raise StorageConfigurationError(
                "GCS bucket must be non-empty and at most 222 characters."
            )
        if any(ord(character) < 32 or ord(character) == 127 for character in self.bucket):
            raise StorageConfigurationError("GCS bucket cannot contain control characters.")
        if self.service_file is not None and (
            not isinstance(self.service_file, str)
            or not self.service_file.strip()
            or len(self.service_file) > 4096
            or any(ord(character) < 32 or ord(character) == 127 for character in self.service_file)
        ):
            raise StorageConfigurationError(
                "GCS service_file must be a non-empty path when provided."
            )
        try:
            valid_timeout = (
                isinstance(self.request_timeout_seconds, (int, float))
                and not isinstance(self.request_timeout_seconds, bool)
                and math.isfinite(self.request_timeout_seconds)
                and 0 < self.request_timeout_seconds <= 300
            )
        except OverflowError:
            valid_timeout = False
        if not valid_timeout:
            raise StorageConfigurationError(
                "request_timeout_seconds must be finite and between 0 and 300."
            )
        if (
            isinstance(self.read_chunk_size, bool)
            or not isinstance(self.read_chunk_size, int)
            or not 1 <= self.read_chunk_size <= 16 * 1024 * 1024
        ):
            raise StorageConfigurationError("read_chunk_size must be between 1 byte and 16 MiB.")
        if (
            isinstance(self.max_upload_bytes, bool)
            or not isinstance(self.max_upload_bytes, int)
            or not 1 <= self.max_upload_bytes <= 1024 * 1024 * 1024
        ):
            raise StorageConfigurationError("max_upload_bytes must be between 1 byte and 1 GiB.")
        if (
            isinstance(self.max_concurrent_uploads, bool)
            or not isinstance(self.max_concurrent_uploads, int)
            or not 1 <= self.max_concurrent_uploads <= 256
        ):
            raise StorageConfigurationError("max_concurrent_uploads must be between 1 and 256.")


__all__ = ["GCSConfig"]
