# Changelog

## Unreleased

- Add the `:azure-blob` konserve backend.
- Support Azure Identity, connection strings, explicit credentials, and
  injected service clients.
- Add ETag/`If-Match` optimistic locking with HTTP 412 conflict handling.
- Add local and CircleCI integration tests against Azurite 3.36.0.
- Align git-derived versions and GitHub releases with the konserve cloud
  adapters.
- Stream binary values and internal blob copies instead of buffering complete
  blobs in memory.
- Validate metadata and EDN component lengths before allocation, with
  configurable size limits.
- Bound the shared Azure client cache and blocking-I/O executor, and report
  executor saturation to callers.
- Await asynchronous streaming callbacks, realize paged listings on the I/O
  executor, and avoid retaining stale ETags or payloads in exception data.
