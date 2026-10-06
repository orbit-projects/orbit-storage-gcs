# Orbit Storage GCS: architecture and boundaries

## Responsibility

`orbit-storage-gcs` is an optional Google Cloud Storage adapter for the provider-neutral `orbit-storage`
capability. It is a separate package and is not installed with Orbit Core or `orbit-storage`.

```bash
pip install orbit-core orbit-storage orbit-storage-gcs
```

```python
from orbit import Application, ApplicationConfig
from orbit_storage_gcs import GCSConfig, GCSObjectStore, GCSObjectStorePlugin

application = Application(ApplicationConfig(name="orders"))
store = GCSObjectStore(GCSConfig(bucket="orders-data"))
application.plugins.register(GCSObjectStorePlugin(store))
```

The adapter uses `gcloud-aio-storage` for asynchronous GCS operations and relies on its Google
Application Default Credentials chain unless `service_file` is explicitly configured. The plugin
registers the store under `orbit-storage`'s shared dependency key and closes its client during
application shutdown. No GCS SDK or cloud credentials are added to Core or the capability package.

Byte uploads and async-iterable uploads are both supported. Streamed uploads are collected into a
bounded buffer before transfer because the selected SDK accepts a bytes or file-like payload rather
than Orbit's async iterator; configure `max_upload_bytes` and `max_concurrent_uploads` to bound
aggregate memory use. Downloads are streamed and the returned `StoredObject` must be used as an
async context manager or explicitly closed. Reads use the object's GCS generation as a precondition,
so an object replacement between metadata lookup and data transfer fails instead of silently
returning mismatched metadata and bytes.

Listing uses GCS page tokens, which are opaque and specific to this adapter and bucket. The
provider's metadata, versioning, retention, encryption, IAM, and consistency behavior remain GCS
responsibilities; the adapter does not provision buckets or change IAM policy. Unit tests use a fake
client. No live GCS project or emulator validation is claimed.

The async SDK API and stream lifecycle are documented by [gcloud-aio-storage](https://talkiq.github.io/gcloud-aio/autoapi/storage/index.html).
The generation precondition used to keep metadata and downloaded bytes aligned follows [Cloud Storage request preconditions](https://cloud.google.com/storage/docs/request-preconditions).

## Declared dependencies

The following dependency declarations come from the checked-in manifests. Optional groups and development dependencies are called out separately.

### `pyproject.toml`
- `gcloud-aio-storage>=9.6,<10`
- `orbit-storage>=0.1.0a1,<0.2`
- Optional `dev` group: `pytest>=8,<10`, `pytest-asyncio>=0.24,<2`, `ruff>=0.8,<1`, `mypy>=1.13,<2`, `orbit-core>=0.1.0a1,<0.2`.

Declared dependencies do not mean that optional providers or services are bundled with this package.

## Implementation layout

Representative implementation files in this checkout:

- `src/orbit_storage_gcs/__init__.py`
- `src/orbit_storage_gcs/config.py`
- `src/orbit_storage_gcs/plugin.py`
- `src/orbit_storage_gcs/store.py`

## Public contract and scope

## Status

This package is pre-alpha; its public API is not stable and it is not yet published. It supports
Python 3.11 through 3.14. Licensed under Apache-2.0.

## Boundary rules

Keep provider SDKs, credentials, transports, and provider-specific error translation in provider adapters. Keep reusable capability contracts in the matching capability package and lifecycle orchestration in Core. Apply the relevant layer for this repository and preserve the dependency direction shown above.
