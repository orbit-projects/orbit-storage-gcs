# Orbit Storage GCS

`orbit-storage-gcs` is an optional Google Cloud Storage adapter for the provider-neutral `orbit-storage`
capability. It is a separate package and is not installed with Orbit Core or `orbit-storage`.
`GCSObjectStore` works directly with `orbit-storage`; `GCSObjectStorePlugin` is an optional Core
integration from `orbit_storage_gcs.plugin`.

```bash
pip install orbit-storage orbit-storage-gcs
# Add the Core lifecycle plugin only when using it:
pip install 'orbit-storage-gcs[core]'
```

```python
from orbit import Application, ApplicationConfig
from orbit_storage_gcs import GCSConfig, GCSObjectStore
from orbit_storage_gcs.plugin import GCSObjectStorePlugin

application = Application(ApplicationConfig(name="orders"))
store = GCSObjectStore(GCSConfig(bucket="orders-data"))
application.plugins.register(GCSObjectStorePlugin(store))
```

The adapter uses `gcloud-aio-storage` for asynchronous GCS operations and relies on its Google
Application Default Credentials chain unless `service_file` is explicitly configured. The plugin
registers the store under `orbit-storage`'s shared dependency key and closes its client during
application shutdown. Orbit CLI can run **Check GCS bucket access**, a read-only request that lists
at most one object without displaying its name. The configured identity needs bucket list access.
No GCS SDK or cloud credentials are added to Core or the capability package.

Byte uploads and async-iterable uploads are both supported. The GCS SDK requires a seekable stream
to determine content length for resumable transfers, so async sources are written to a
spooled temporary file in bounded chunks. `upload_memory_threshold_bytes` controls the per-upload
memory threshold; its product with `max_concurrent_uploads` is capped at 64 MiB. Larger streams
spill to the process temporary directory and are sent through the SDK's resumable upload path.
`max_upload_bytes` bounds each stream, so maximum temporary disk use is bounded by that value
multiplied by concurrent uploads. Place the temporary directory on an ephemeral volume with
sufficient quota and appropriate encryption and retention controls. Downloads are streamed and the returned `StoredObject` must be used as an
async context manager or explicitly closed. Reads use the object's GCS generation as a precondition,
so an object replacement between metadata lookup and data transfer fails instead of silently
returning mismatched metadata and bytes.

Idempotent metadata reads, list-page reads, and download-stream opening use bounded retries for
timeouts, connection failures, throttling, and selected transient server statuses. Configure
`max_retry_attempts` (including the initial attempt), `retry_initial_delay_seconds`, and
`retry_max_delay_seconds` to tune full-jitter exponential backoff. Streaming body reads, uploads,
and deletes are not automatically retried: a mid-stream restart requires range and generation
coordination, while a lost mutation response can leave its outcome ambiguous. The underlying
`gcloud-aio-storage` SDK deliberately leaves retry policy to callers.

Listing uses GCS page tokens, which are opaque and specific to this adapter and bucket. The
provider's metadata, versioning, retention, encryption, IAM, and consistency behavior remain GCS
responsibilities; the adapter does not provision buckets or change IAM policy. Unit tests use a fake
client. No live GCS project or emulator validation is claimed.

The async SDK API and stream lifecycle are documented by [gcloud-aio-storage](https://talkiq.github.io/gcloud-aio/autoapi/storage/index.html).
The generation precondition used to keep metadata and downloaded bytes aligned follows [Cloud Storage request preconditions](https://cloud.google.com/storage/docs/request-preconditions).

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e ../orbit-core
python -m pip install -e ../orbit-storage
python -m pip install -e '.[dev]'
pytest
ruff check src tests
mypy
```

## Status

This package is alpha; its public API is not stable and it is not yet published. It supports
Python 3.11 through 3.14. Licensed under Apache-2.0.

