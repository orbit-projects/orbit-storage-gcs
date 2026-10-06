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
"""Optional Core plugin that registers and owns the GCS object-store adapter."""

from orbit import Application
from orbit.plugins import Plugin, PluginMetadata
from orbit_storage import OBJECT_STORE_DEPENDENCY_KEY

from orbit_storage_gcs.store import GCSObjectStore


class GCSObjectStorePlugin(Plugin):
    """Register an object store using the shared capability key and close it on shutdown."""

    metadata = PluginMetadata(
        name="orbit-storage-gcs",
        version="0.1.0a1",
        capabilities=frozenset({"storage.gcs"}),
    )

    def __init__(self, store: GCSObjectStore) -> None:
        """Take lifecycle ownership of an explicitly configured GCS adapter."""
        if not isinstance(store, GCSObjectStore):
            raise TypeError("GCSObjectStorePlugin requires a GCSObjectStore instance.")
        self.store = store

    def setup(self, application: Application) -> None:
        """Expose the provider implementation through Orbit Storage's stable key."""
        if not isinstance(application, Application):
            raise TypeError("GCSObjectStorePlugin requires an Orbit Application.")
        application.container.register_instance(OBJECT_STORE_DEPENDENCY_KEY, self.store)

    async def deactivate(self) -> None:
        """Close the asynchronous Google client during shutdown or startup rollback."""
        await self.store.aclose()


__all__ = ["GCSObjectStorePlugin"]
