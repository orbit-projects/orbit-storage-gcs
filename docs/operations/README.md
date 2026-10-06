# Orbit Storage GCS: operations and security

This guide organizes runtime behavior documented by the package. It does not certify production readiness. Verify provider/client versions, permissions, transport security, limits, and failure behavior in the target environment before release.

## Configuration surface

Environment names found in the package README:

The package README does not name `ORBIT_*` variables. Use its typed constructors and application configuration, and confirm exact runtime inputs in the implementation before deployment.

Use the package README's constructor and deployment examples. Store credentials in a secret manager and avoid logging credentials, raw provider errors, request data, or opaque cursors.

## Lifecycle, failure behavior, and limits

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

## Status

This package is pre-alpha; its public API is not stable and it is not yet published. It supports
Python 3.11 through 3.14. Licensed under Apache-2.0.

## Production validation

Validate startup/shutdown cleanup, timeout and cancellation behavior, concurrency and payload bounds where applicable, secret rotation and least-privilege access, data durability, backup/restore, and failover against the selected provider. Do not infer distributed or durable guarantees from an in-process API or fake-client tests.
