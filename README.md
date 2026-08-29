# konserve-azure-blob

[![Slack](https://img.shields.io/badge/slack-join_chat-brightgreen.svg)](https://clojurians.slack.com/archives/CB7GJAN0L)
[![Clojars](https://img.shields.io/clojars/v/org.replikativ/konserve-azure-blob.svg)](https://clojars.org/org.replikativ/konserve-azure-blob)
[![CircleCI](https://circleci.com/gh/replikativ/konserve-azure-blob.svg?style=shield)](https://circleci.com/gh/replikativ/konserve-azure-blob)
[![Last commit](https://img.shields.io/github/last-commit/replikativ/konserve-azure-blob/main.svg)](https://github.com/replikativ/konserve-azure-blob/tree/main)

An [Azure Blob Storage](https://azure.microsoft.com/products/storage/blobs)
backend for [konserve](https://github.com/replikativ/konserve) and Datahike.

Azure Blob Storage is Azure's object storage service. This adapter maps each
konserve store to an isolated blob prefix within a container and uses blob ETags
for optimistic concurrency, providing durable shared storage for distributed
konserve and Datahike deployments.

## Usage

Add to your dependencies:

[![Clojars](https://img.shields.io/clojars/v/org.replikativ/konserve-azure-blob.svg)](https://clojars.org/org.replikativ/konserve-azure-blob)

### Configuration

```clojure
(require '[konserve-azure-blob.core]  ;; Registers the :azure-blob backend
         '[konserve.core :as k])

(def config
  {:backend :azure-blob
   :account-name "my-storage-account"
   :container "konserve"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"
   :config {:optimistic-locking-retries 10}})

(def store (k/create-store config {:sync? true}))
```

For API usage (`assoc-in`, `get-in`, `delete-store`, etc.), see the
[konserve documentation](https://github.com/replikativ/konserve).

The configuration alternatives, in precedence order, are:

- `:service-client` — an already configured `BlobServiceClient`
- `:connection-string` — useful for shared-key auth and Azurite
- `:endpoint` plus `:credential`
- `:account-name` plus `DefaultAzureCredential`

`:container` defaults to `"konserve"`. Each `:id` gets an isolated prefix in
the same container; `:store-path` can be used instead when a non-UUID path is
needed. Deleting a store removes only that prefix's konserve objects and marker,
and deliberately retains the container.

### Multiple stores in the same container

Different `:id` or `:store-path` values provide isolated stores in one Azure
container:

```clojure
(def store-1
  (k/create-store
   {:backend :azure-blob
    :account-name "my-storage-account"
    :container "konserve"
    :id #uuid "11111111-1111-1111-1111-111111111111"}
   {:sync? true}))

(def store-2
  (k/create-store
   {:backend :azure-blob
    :account-name "my-storage-account"
    :container "konserve"
    :id #uuid "22222222-2222-2222-2222-222222222222"}
   {:sync? true}))
```

### Datahike

Use the same backend map as the Datahike store configuration. The namespace must
be required before creating or connecting to the database so that the konserve
multimethods are registered:

```clojure
(require '[datahike.api :as d]
         '[konserve-azure-blob.core])

(def datahike-config
  {:store {:backend :azure-blob
           :account-name "my-storage-account"
           :container "datahike"
           :id #uuid "550e8400-e29b-41d4-a716-446655440000"}
   :schema-flexibility :write
   :keep-history? true})

(d/create-database datahike-config)
```

### Optimistic locking for distributed updates

Azure Blob Storage and Azurite both support the compare-and-swap behavior needed
by konserve. A read captures the blob ETag and the following write sends it as
an `If-Match` condition. Azure returns HTTP 412 if another writer changed the
blob; konserve then retries the read-modify-write when
`:optimistic-locking-retries` is positive.

Without optimistic locking, concurrent updates remain last-write-wins. With it,
independent application instances can safely update the same konserve value:

```clojure
(def config
  {:backend :azure-blob
   :account-name "my-storage-account"
   :container "konserve"
   :id #uuid "550e8400-e29b-41d4-a716-446655440000"
   :config {:optimistic-locking-retries 20}})

(def store (k/create-store config {:sync? true}))

;; Independent application instances can safely run this concurrently.
(k/update-in store [:counter] (fnil inc 0) {:sync? true})
```

How it works:

1. A read captures the blob's current ETag.
2. The following write sends that ETag in an `If-Match` condition.
3. Azure rejects a stale ETag with HTTP 412 (Precondition Failed).
4. Konserve reads the new value, reapplies the update function, and retries.
5. Retrying stops when the write succeeds or the configured limit is reached.

The client pins Azure Blob service API `2025-11-05`, which Azurite 3.36.0
implements. This backend does not depend on newer Blob features.

## Streaming and memory limits

Binary values are uploaded and downloaded as streams. The `InputStream` passed
to a streaming `bget` callback remains open until the callback, or the
core.async channel it returns, completes. Reads and writes therefore do not
need to materialize an entire binary blob in JVM memory. Blob copies used by
konserve's storage layout are streamed as well.

Metadata and ordinary EDN values must be materialized for deserialization, so
their declared lengths are checked against the blob size and configurable
limits before a byte array is allocated. The defaults are 16 MiB for metadata
and 256 MiB for an EDN value:

```clojure
{:backend :azure-blob
 :account-name "my-storage-account"
 :container "konserve"
 :id (random-uuid)
 :config {:max-metadata-size (* 16 1024 1024)
          :max-edn-value-size (* 256 1024 1024)}}
```

Choose limits appropriate for the application's JVM heap and expected value
sizes. Large payloads should use konserve's binary API.

Blocking Azure SDK calls run on a bounded shared executor (64 active requests
and a queue of 1,024). If both are exhausted, an asynchronous operation returns
an exception with type `:azure-blob-io-saturated`, allowing the application to
apply backpressure or retry instead of retaining work without limit.

Service clients are also kept in a bounded cache of 64 entries. Applications
with dynamic, tenant-specific credentials can pass a managed
`:service-client`; `clear-client-cache!` is available for credential rotation
and test isolation.

## Authentication and permissions

`DefaultAzureCredential` is used when `:account-name` is supplied. In Azure,
grant the application identity the `Storage Blob Data Contributor` role for the
container or account. This covers blob and container operations without granting
storage-account management rights.

For local development against Azure, `az login` can provide the developer
credential. Environment credentials, workload identity, and managed identity
are selected automatically where available. Secrets do not need to be stored in
the application configuration.

## Local development with Azurite

Docker Compose starts the same pinned Azurite image used in CI and runs the
complete sync, async, lifecycle, isolation, and concurrent-CAS tests:

```sh
./bin/run-tests
```

For a manually started emulator, use the standard development connection
string:

```clojure
{:backend :azure-blob
 :connection-string "UseDevelopmentStorage=true"
 :container "konserve"
 :id (random-uuid)}
```

Azurite listens on blob port 10000. Queue and table services are not required.

To manage the emulator separately:

```sh
docker compose up -d --wait
clojure -M:test -m konserve-azure-blob.test-runner
docker compose down --volumes
```

## Notes

The backend creates the configured container if necessary. `delete-store`
removes the store marker and konserve objects with `.ksv`, `.ksv.new`, and
`.ksv.backup` suffixes below that store's prefix, but it does not delete the
Azure container or unrelated blobs. An externally managed container therefore
only needs data-plane access for the application identity.

## Releases

The build follows the other konserve cloud adapters: the release version is
`0.1.<git-revision-count>`, and the GitHub release records the exact commit SHA.
CircleCI builds, formats, tests against Azurite, deploys to Clojars, and creates
the GitHub release from `main`.

## License

Copyright © 2026 Christian Weilbach

Licensed under the Apache License 2.0 (see [LICENSE](LICENSE)).
