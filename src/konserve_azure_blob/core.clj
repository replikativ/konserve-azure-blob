(ns konserve-azure-blob.core
  "Azure Blob Storage backend for konserve."
  (:require [clojure.core.async :refer [close! go promise-chan put!]]
            [konserve.impl.defaults :refer [connect-default-store normalize-store-config]]
            [konserve.impl.storage-layout :as layout
             :refer [PBackingBlob PBackingLock PBackingStore PReadMissSafe
                     -delete-store store-key-not-found-ex]]
            [konserve.store :as store]
            [konserve.utils :refer [*default-sync-translation* async+sync]]
            [replikativ.logging :as log]
            [superv.async :refer [<?- go-try-]])
  (:import [com.azure.core.util BinaryData Context]
           [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.storage.blob BlobClient BlobContainerClient BlobServiceClient
            BlobServiceClientBuilder BlobServiceVersion]
           [com.azure.storage.blob.models BlobRequestConditions BlobStorageException
            ListBlobsOptions]
           [com.azure.storage.blob.options BlobParallelUploadOptions]
           [java.io ByteArrayInputStream ByteArrayOutputStream]
           [java.util Arrays]
           [java.util.concurrent ConcurrentHashMap ExecutorService Executors ThreadFactory]))

(def ^:const default-container "konserve")
(def ^:const output-stream-buffer-size (* 1024 1024))

;; Azurite 3.36.0 implements this service version. The backend does not need any
;; operation introduced after it, so pinning the wire version keeps emulator and
;; production behavior aligned even when azure-storage-blob advances its default.
(def default-service-version BlobServiceVersion/V2025_11_05)

(defn- endpoint-for-account [account-name]
  (str "https://" account-name ".blob.core.windows.net"))

(defn build-service-client
  "Build a BlobServiceClient from a backend spec.

   Supported inputs, in precedence order:
   - :service-client
   - :connection-string
   - :endpoint plus :credential
   - :account-name plus DefaultAzureCredential"
  [{:keys [service-client connection-string endpoint account-name credential service-version]}]
  (or service-client
      (let [^BlobServiceClientBuilder builder (BlobServiceClientBuilder.)
            version (or service-version default-service-version)]
        (.serviceVersion builder version)
        (if connection-string
          (.connectionString builder connection-string)
          (let [endpoint (or endpoint
                             (when account-name (endpoint-for-account account-name))
                             (throw (ex-info "Azure Blob Storage requires :account-name, :endpoint, :connection-string, or :service-client."
                                             {:type :invalid-azure-blob-config})))
                credential (or credential (.build (DefaultAzureCredentialBuilder.)))]
            (.endpoint builder endpoint)
            (.credential builder credential)))
        (.buildClient builder))))

(def ^:private client-cache (ConcurrentHashMap.))

(defn- client-key [spec]
  (select-keys spec [:connection-string :endpoint :account-name :credential :service-version]))

(defn service-client
  "Return an injected client or a shared client for this endpoint and credential set."
  [spec]
  (or (:service-client spec)
      @(.computeIfAbsent
        client-cache
        (client-key spec)
        (reify java.util.function.Function
          (apply [_ _] (delay (build-service-client spec)))))))

(defn clear-client-cache!
  "Forget cached service clients. Azure SDK clients do not require closing."
  []
  (.clear client-cache))

(defn spec->store-path [{:keys [store-path id]}]
  (let [path (or store-path (some-> id str))]
    (when-not (and (string? path) (not-empty path))
      (throw (ex-info "Azure Blob Storage requires :id or a non-empty :store-path."
                      {:type :invalid-azure-blob-config})))
    (when (or (.startsWith ^String path "/") (.endsWith ^String path "/"))
      (throw (ex-info ":store-path must not start or end with '/'."
                      {:type :invalid-azure-blob-config :store-path path})))
    path))

(defn- blob-name [store-path key]
  (str store-path "/" key))

(def ^:private store-marker-key ".konserve-store-metadata")

(defn- marker-name [store-path]
  (blob-name store-path store-marker-key))

(defn- blob-client
  ^BlobClient [^BlobContainerClient container-client name]
  (.getBlobClient container-client name))

(defn- not-found? [^Throwable e]
  (and (instance? BlobStorageException e)
       (= 404 (.getStatusCode ^BlobStorageException e))))

(defn- precondition-failed? [^Throwable e]
  (and (instance? BlobStorageException e)
       (= 412 (.getStatusCode ^BlobStorageException e))))

(defn read-object
  "Read a blob in one request and return {:data byte-array :etag string}, or
   ::not-found. The download response carries the ETag, so CAS adds no HEAD."
  [^BlobContainerClient container-client name]
  (try
    (let [response (.downloadContentWithResponse
                    (blob-client container-client name)
                    nil nil nil Context/NONE)]
      {:data (.toBytes ^BinaryData (.getValue response))
       :etag (.getETag (.getDeserializedHeaders response))})
    (catch BlobStorageException e
      (if (not-found? e) ::not-found (throw e)))))

(defn- upload-options
  ^BlobParallelUploadOptions [^bytes bytes conditions]
  (cond-> (BlobParallelUploadOptions. (BinaryData/fromBytes bytes))
    conditions (.setRequestConditions ^BlobRequestConditions conditions)))

(defn write-object!
  "Create or overwrite a blob. When expected-etag is non-nil, use If-Match and
   return false on a CAS conflict; otherwise return true."
  [^BlobContainerClient container-client name ^bytes bytes expected-etag]
  (try
    (let [conditions (when expected-etag
                       (doto (BlobRequestConditions.)
                         (.setIfMatch expected-etag)))]
      (.uploadWithResponse
       (blob-client container-client name)
       (upload-options bytes conditions)
       nil
       Context/NONE)
      true)
    (catch BlobStorageException e
      (if (and expected-etag (precondition-failed? e))
        false
        (throw e)))))

(defn- write-object-if-absent!
  [^BlobContainerClient container-client name ^bytes bytes]
  (try
    (let [conditions (doto (BlobRequestConditions.) (.setIfNoneMatch "*"))]
      (.uploadWithResponse
       (blob-client container-client name)
       (upload-options bytes conditions)
       nil
       Context/NONE)
      true)
    (catch BlobStorageException e
      (if (precondition-failed? e) false (throw e)))))

(defn- object-exists? [^BlobContainerClient container-client name]
  (try
    (boolean (.exists (blob-client container-client name)))
    (catch BlobStorageException e
      (if (not-found? e) false (throw e)))))

(defn- delete-object! [^BlobContainerClient container-client name]
  (try
    (boolean (.deleteIfExists (blob-client container-client name)))
    (catch BlobStorageException e
      (if (not-found? e) false (throw e)))))

(defn- copy-object!
  "Copy through the client. This works with shared-key, SAS, and Entra-backed
   clients alike; server-side URL copy has different source-auth requirements."
  [^BlobContainerClient container-client from-name to-name]
  (let [source (read-object container-client from-name)]
    (when-not (= ::not-found source)
      (write-object! container-client to-name (:data source) nil))))

(defn- list-object-names [^BlobContainerClient container-client prefix]
  (let [options (doto (ListBlobsOptions.) (.setPrefix prefix))]
    (map #(.getName %) (iterator-seq (.iterator (.listBlobs container-client options nil))))))

;; The synchronous Azure SDK blocks. Never run it on core.async's dispatch pool:
;; asynchronous konserve operations execute on a virtual thread on JDK 21+, or
;; on a bounded daemon pool on older JDKs.
(def ^:private virtual-threads-available?
  (boolean
   (try
     (.getMethod Thread "startVirtualThread" (into-array Class [Runnable]))
     (catch Throwable _ false))))

(defonce ^:private fallback-io-executor
  (delay
    (let [counter (java.util.concurrent.atomic.AtomicInteger.)]
      (Executors/newFixedThreadPool
       64
       (reify ThreadFactory
         (newThread [_ runnable]
           (doto (Thread. ^Runnable runnable
                          (str "konserve-azure-blob-io-" (.incrementAndGet counter)))
             (.setDaemon true))))))))

(defn- run-io-task [^Runnable runnable]
  (if virtual-threads-available?
    (Thread/startVirtualThread runnable)
    (.execute ^ExecutorService @fallback-io-executor runnable)))

(defn- io-thread-ch [f]
  (let [result-ch (promise-chan)]
    (run-io-task
     (fn []
       (let [result (try
                      (f)
                      (catch Exception e e)
                      (catch Throwable t
                        (ex-info "Error in Azure Blob IO thread."
                                 {:type :azure-blob-io-error}
                                 t)))]
         (if (nil? result)
           (close! result-ch)
           (put! result-ch result)))))
    result-ch))

(defmacro ^:private io-try- [& body]
  `(io-thread-ch (fn [] ~@body)))

(def ^:private io-sync-translation
  (merge *default-sync-translation* '{io-try- try}))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defrecord AzureBlob [backing name data fetched-object etag]
  PBackingBlob
  (-sync [_ env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (let [{:keys [header meta value]} @data
            current-etag (get @(:etag-cache backing) name)
            retries (get-in env [:config :optimistic-locking-retries] 0)]
        (when-not (and header meta value)
          (throw (ex-info "Updating a row requires header, metadata, and value."
                          {:data @data})))
        (with-open [output (ByteArrayOutputStream. output-stream-buffer-size)]
          (.write output ^bytes header)
          (.write output ^bytes meta)
          (.write output ^bytes value)
          (when-not (write-object! (:container-client backing)
                                   name
                                   (.toByteArray output)
                                   (when (pos? retries) current-etag))
            (throw (ex-info "Azure Blob optimistic lock conflict."
                            {:type :optimistic-lock-conflict
                             :key name
                             :etag current-etag}))))
        (reset! data {})
        (reset! fetched-object nil)
        (reset! etag nil)
        (swap! (:etag-cache backing) dissoc name)))))
  (-close [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true)))
  (-read-header [_ env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (when-not @fetched-object
        (let [response (read-object (:container-client backing) name)]
          (when (= ::not-found response)
            (throw (store-key-not-found-ex name)))
          (reset! fetched-object (:data response))
          (reset! etag (:etag response))
          (swap! (:etag-cache backing) assoc name (:etag response))))
      (Arrays/copyOfRange ^bytes @fetched-object 0 layout/header-size))))
  (-read-meta [_ meta-size env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (Arrays/copyOfRange ^bytes @fetched-object
                          layout/header-size
                          (+ layout/header-size meta-size)))))
  (-read-value [_ meta-size env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (let [object ^bytes @fetched-object]
        (Arrays/copyOfRange object
                            (+ layout/header-size meta-size)
                            (alength object))))))
  (-read-binary [_ meta-size locked-cb env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (let [object ^bytes @fetched-object
            offset (+ layout/header-size meta-size)]
        (locked-cb {:input-stream (ByteArrayInputStream.
                                   (Arrays/copyOfRange object offset (alength object)))
                    :size (- (alength object) offset)})))))
  (-write-header [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))
  (-write-meta [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))
  (-write-value [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value))))
  (-write-binary [_ _meta-size value env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value)))))

(defn- konserve-object? [name]
  (or (.endsWith ^String name ".ksv")
      (.endsWith ^String name ".ksv.new")
      (.endsWith ^String name ".ksv.backup")))

(defrecord AzureBlobContainer [service-client container-client container store-path etag-cache]
  PBackingStore
  (-create-blob [this store-key env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (->AzureBlob this (blob-name store-path store-key)
                                      (atom {}) (atom nil) (atom nil)))))
  (-delete-blob [_ store-key env]
    (async+sync (:sync? env) io-sync-translation
                (io-try-
                 (let [name (blob-name store-path store-key)
                       deleted? (delete-object! container-client name)]
                   (swap! etag-cache dissoc name)
                   deleted?))))
  (-blob-exists? [_ store-key env]
    (async+sync (:sync? env) io-sync-translation
                (io-try- (object-exists? container-client (blob-name store-path store-key)))))
  (-copy [_ from to env]
    (async+sync (:sync? env) io-sync-translation
                (io-try- (copy-object! container-client
                                       (blob-name store-path from)
                                       (blob-name store-path to)))))
  (-atomic-move [_ from to env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (let [from-name (blob-name store-path from)
            to-name (blob-name store-path to)]
        (copy-object! container-client from-name to-name)
        (delete-object! container-client from-name)
        (swap! etag-cache dissoc from-name to-name)))))
  (-migratable [_ _key _store-key env]
    (if (:sync? env) nil (go-try- nil)))
  (-migrate [_ _migration-key _key-vec _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-handle-foreign-key [_ _migration-key _serializer _read-handlers _write-handlers env]
    (if (:sync? env) nil (go-try- nil)))
  (-create-store [_ env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (.createIfNotExists container-client)
      ;; Idempotent under concurrent first connects. If another creator wins,
      ;; its marker is equivalent to ours.
      (write-object-if-absent! container-client
                               (marker-name store-path)
                               (.getBytes "konserve" "UTF-8")))))
  (-store-exists? [_ env]
    (async+sync (:sync? env) io-sync-translation
                (io-try- (object-exists? container-client (marker-name store-path)))))
  (-sync-store [_ env]
    (if (:sync? env) nil (go-try- nil)))
  (-delete-store [_ env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (when (.exists container-client)
        (log/info :konserve.azure-blob/delete-store
                  "Deleting konserve blobs while retaining the Azure container.")
        (let [prefix (str store-path "/")]
          (doseq [name (list-object-names container-client prefix)
                  :when (or (= name (marker-name store-path))
                            (konserve-object? name))]
            (delete-object! container-client name)))
        (reset! etag-cache {})))))
  (-keys [_ env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (let [prefix (str store-path "/")
            prefix-length (count prefix)]
        (->> (list-object-names container-client prefix)
             (filter konserve-object?)
             (map #(subs % prefix-length))))))))

(extend-type AzureBlobContainer
  PReadMissSafe)

(defn- backing [spec]
  (let [client (service-client spec)
        container (or (:container spec) default-container)]
    (when-not (and (string? container) (not-empty container))
      (throw (ex-info "Azure Blob Storage requires a non-empty :container."
                      {:type :invalid-azure-blob-config})))
    (->AzureBlobContainer client
                          (.getBlobContainerClient client container)
                          container
                          (spec->store-path spec)
                          (atom {}))))

(defn connect-store
  "Connect a DefaultStore to Azure Blob Storage. The multimethod API should be
   preferred by applications; this lower-level entry point retains the sibling
   backends' direct API."
  [spec & {:keys [opts] :or {opts {:sync? true}}}]
  (let [merged-config (merge {:sync-blob? true
                              :in-place? true
                              :no-backup? true
                              :lock-blob? true}
                             (:config spec))
        store-config (-> (select-keys spec [:default-serializer :serializers
                                            :read-handlers :write-handlers
                                            :buffer-size])
                         (assoc :config merged-config)
                         normalize-store-config
                         (update-in [:config :encoding]
                                    #(merge {:serializer :FressianSerializer} %))
                         (update :buffer-size #(or % output-stream-buffer-size))
                         (assoc :opts opts))]
    (connect-default-store (backing spec) store-config)))

(defn delete-store [spec & {:keys [opts] :or {opts {:sync? true}}}]
  (-delete-store (backing spec) opts))

(defn release [_store env]
  (if (:sync? env) nil (go-try- nil)))

(defmethod store/-connect-store :azure-blob
  [{:keys [container id] :as config} opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (let [spec (dissoc config :backend)
          store-backing (backing spec)
          exists? (<?- (layout/-store-exists? store-backing opts))]
      (when-not exists?
        (throw (ex-info (str "Azure Blob konserve store does not exist: "
                             container "/" (spec->store-path spec))
                        {:type :store-not-found :container container :id id})))
      (<?- (connect-store spec :opts opts))))))

(defmethod store/-create-store :azure-blob
  [{:keys [container id] :as config} opts]
  (async+sync
   (:sync? opts) *default-sync-translation*
   (go-try-
    (let [spec (dissoc config :backend)
          store-backing (backing spec)
          exists? (<?- (layout/-store-exists? store-backing opts))]
      (when exists?
        (throw (ex-info (str "Azure Blob konserve store already exists: "
                             container "/" (spec->store-path spec))
                        {:type :store-already-exists :container container :id id})))
      (<?- (connect-store spec :opts opts))))))

(defmethod store/-store-exists? :azure-blob
  [config opts]
  (layout/-store-exists? (backing (dissoc config :backend)) opts))

(defmethod store/-delete-store :azure-blob
  [config opts]
  (delete-store (dissoc config :backend) :opts opts))

(defmethod store/-release-store :azure-blob
  [_config store-instance opts]
  (release store-instance opts))
