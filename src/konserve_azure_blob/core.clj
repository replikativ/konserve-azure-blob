(ns konserve-azure-blob.core
  "Azure Blob Storage backend for konserve."
  (:require [clojure.core.async :refer [<!! close! go promise-chan put!]]
            [clojure.core.async.impl.protocols :as async-proto]
            [konserve.impl.defaults :refer [connect-default-store normalize-store-config]]
            [konserve.impl.storage-layout :as layout
             :refer [PBackingBlob PBackingLock PBackingStore PReadMissSafe PStreamingBinaryWrite
                     -delete-store store-key-not-found-ex]]
            [konserve.store :as store]
            [konserve.utils :refer [*default-sync-translation* async+sync]]
            [replikativ.logging :as log]
            [superv.async :refer [<?- go-try-]])
  (:import [com.azure.core.util BinaryData Context]
           [com.azure.identity DefaultAzureCredentialBuilder]
           [com.azure.storage.blob BlobClient BlobContainerClient BlobServiceClient
            BlobServiceClientBuilder BlobServiceVersion]
           [com.azure.storage.blob.models BlobRange BlobRequestConditions BlobStorageException
            ListBlobsOptions]
           [com.azure.storage.blob.options BlobInputStreamOptions BlobParallelUploadOptions]
           [com.azure.storage.blob.specialized BlobInputStream]
           [java.io ByteArrayInputStream Closeable File FileInputStream InputStream
            PushbackReader Reader SequenceInputStream]
           [java.nio.charset StandardCharsets]
           [java.security MessageDigest]
           [java.util Arrays Base64 Collections LinkedHashMap]
           [java.util.concurrent ArrayBlockingQueue ExecutorService RejectedExecutionException
            ThreadFactory ThreadPoolExecutor ThreadPoolExecutor$AbortPolicy TimeUnit]))

(def ^:const default-container "konserve")
(def ^:const default-buffer-size (* 64 1024))
(def ^:const default-max-metadata-size (* 16 1024 1024))
(def ^:const default-max-edn-value-size (* 256 1024 1024))
(def ^:const max-cached-clients 64)
(def ^:const io-thread-count 64)
(def ^:const io-queue-capacity 1024)

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

(def ^:private client-cache
  (proxy [LinkedHashMap] [16 0.75 true]
    (removeEldestEntry [_]
      (> (.size ^LinkedHashMap this) max-cached-clients))))

(defn- sha-256 [^String value]
  (.encodeToString (Base64/getEncoder)
                   (.digest (MessageDigest/getInstance "SHA-256")
                            (.getBytes value StandardCharsets/UTF_8))))

(defn- client-key [spec]
  (cond-> (select-keys spec [:endpoint :account-name :credential :service-version])
    (:connection-string spec)
    (assoc :connection-string-sha-256 (sha-256 (:connection-string spec)))))

(defn service-client
  "Return an injected client or a shared client for this endpoint and credential set."
  [spec]
  (or (:service-client spec)
      (let [key (client-key spec)
            cached (locking client-cache
                     (or (.get client-cache key)
                         (let [candidate (delay (build-service-client spec))]
                           (.put client-cache key candidate)
                           candidate)))]
        (try
          @cached
          (catch Exception e
            ;; A failed delayed construction must not poison this key forever.
            (locking client-cache
              (when (identical? cached (.get client-cache key))
                (.remove client-cache key)))
            (throw e))))))

(defn clear-client-cache!
  "Forget cached service clients. Azure SDK clients do not require closing."
  []
  (locking client-cache
    (.clear client-cache)))

(defn client-cache-size
  "Number of shared clients currently retained by the bounded cache."
  []
  (locking client-cache
    (.size client-cache)))

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
  "Read a complete blob and return {:data byte-array :etag string}, or
   ::not-found. This low-level helper intentionally materializes its result;
   normal store reads use bounded ranges or streams instead."
  [^BlobContainerClient container-client name]
  (try
    (let [response (.downloadContentWithResponse
                    (blob-client container-client name)
                    nil nil nil Context/NONE)]
      {:data (.toBytes ^BinaryData (.getValue response))
       :etag (.getETag (.getDeserializedHeaders response))})
    (catch BlobStorageException e
      (if (not-found? e) ::not-found (throw e)))))

(defn- request-conditions [expected-etag]
  (when expected-etag
    (doto (BlobRequestConditions.)
      (.setIfMatch expected-etag))))

(defn- optimistic-conflict [name expected-etag cause]
  (ex-info "Azure Blob optimistic lock conflict."
           {:type :optimistic-lock-conflict
            :key name
            :etag expected-etag}
           cause))

(defn- content-range-total [headers]
  (when-let [content-range (.getContentRange headers)]
    (let [slash (.lastIndexOf ^String content-range "/")]
      (when (and (not (neg? slash)) (< slash (dec (count content-range))))
        (parse-long (subs content-range (inc slash)))))))

(defn- read-range
  [^BlobContainerClient container-client name offset length expected-etag]
  (if (zero? length)
    {:data (byte-array 0) :etag expected-etag}
    (try
      (let [client (blob-client container-client name)
            response (.downloadContentWithResponse
                      client
                      nil
                      (request-conditions expected-etag)
                      (BlobRange. (long offset) (Long/valueOf (long length)))
                      false
                      nil
                      Context/NONE)
            headers (.getDeserializedHeaders response)]
        {:data (.toBytes ^BinaryData (.getValue response))
         :etag (.getETag headers)
         :total-size (or (content-range-total headers)
                         (.getBlobSize (.getProperties client)))})
      (catch BlobStorageException e
        (cond
          (not-found? e) ::not-found
          (and expected-etag (precondition-failed? e))
          (throw (optimistic-conflict name expected-etag e))
          :else (throw e))))))

(defn- upload-options
  ^BlobParallelUploadOptions [^bytes bytes conditions]
  (cond-> (BlobParallelUploadOptions. (BinaryData/fromBytes bytes))
    conditions (.setRequestConditions ^BlobRequestConditions conditions)))

(defn- stream-upload-options
  ^BlobParallelUploadOptions [^InputStream input length conditions]
  (cond-> (if (some? length)
            (BlobParallelUploadOptions. input (long length))
            (BlobParallelUploadOptions. input))
    conditions (.setRequestConditions ^BlobRequestConditions conditions)))

(defn- write-stream!
  [^BlobContainerClient container-client name ^InputStream input length expected-etag]
  (try
    (.uploadWithResponse
     (blob-client container-client name)
     (stream-upload-options input length (request-conditions expected-etag))
     Context/NONE)
    true
    (catch BlobStorageException e
      (if (and expected-etag (precondition-failed? e))
        false
        (throw e)))))

(defn write-object!
  "Create or overwrite a blob. When expected-etag is non-nil, use If-Match and
   return false on a CAS conflict; otherwise return true."
  [^BlobContainerClient container-client name ^bytes bytes expected-etag]
  (write-stream! container-client name (ByteArrayInputStream. bytes)
                 (alength bytes) expected-etag))

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
  "Copy through a bounded stream. This works with shared-key, SAS, and
   Entra-backed clients alike; server-side URL copy has different source-auth
   requirements."
  [^BlobContainerClient container-client from-name to-name]
  (try
    (with-open [source (.openInputStream (blob-client container-client from-name))]
      (write-stream! container-client to-name source
                     (.getBlobSize (.getProperties ^BlobInputStream source)) nil))
    (catch BlobStorageException e
      (if (not-found? e) false (throw e)))))

(defn- list-object-names [^BlobContainerClient container-client prefix]
  (let [options (doto (ListBlobsOptions.) (.setPrefix prefix))]
    ;; Realize while still on the blocking-I/O executor. Returning the lazy
    ;; Azure iterator would fetch later pages on core.async's dispatch pool.
    (into [] (map #(.getName %))
          (iterator-seq (.iterator (.listBlobs container-client options nil))))))

(defn- io-thread-factory []
  (let [counter (java.util.concurrent.atomic.AtomicInteger.)]
    (reify ThreadFactory
      (newThread [_ runnable]
        (doto (Thread. ^Runnable runnable
                       (str "konserve-azure-blob-io-" (.incrementAndGet counter)))
          (.setDaemon true))))))

;; The synchronous Azure SDK blocks. A fixed pool plus bounded queue prevents
;; an outage from retaining an unlimited number of closures, payloads and
;; result channels. Saturation is reported to the caller explicitly.
(defonce ^:private io-executor
  (delay
    (ThreadPoolExecutor. io-thread-count
                         io-thread-count
                         0
                         TimeUnit/MILLISECONDS
                         (ArrayBlockingQueue. io-queue-capacity)
                         (io-thread-factory)
                         (ThreadPoolExecutor$AbortPolicy.))))

(defn- run-io-task [^Runnable runnable]
  (.execute ^ExecutorService @io-executor runnable))

(defn io-executor-stats
  "Current bounded I/O executor utilization, for monitoring and tests."
  []
  (if (realized? io-executor)
    (let [^ThreadPoolExecutor executor @io-executor]
      {:active (.getActiveCount executor)
       :queued (.size (.getQueue executor))
       :queue-capacity (+ (.size (.getQueue executor))
                          (.remainingCapacity (.getQueue executor)))})
    {:active 0 :queued 0 :queue-capacity io-queue-capacity}))

(defn shutdown-io-executor!
  "Stop the shared daemon I/O executor, primarily for application shutdown."
  []
  (when (realized? io-executor)
    (.shutdownNow ^ExecutorService @io-executor)))

(defn- deliver-result! [result-ch result]
  (if (nil? result)
    (close! result-ch)
    (put! result-ch result (fn [_] (close! result-ch)))))

(defn- io-thread-ch [f]
  (let [result-ch (promise-chan)]
    (try
      (run-io-task
       (fn []
         (deliver-result! result-ch (try (f) (catch Exception e e)))))
      (catch RejectedExecutionException e
        (deliver-result!
         result-ch
         (ex-info "Azure Blob I/O executor is saturated."
                  {:type :azure-blob-io-saturated
                   :queue-capacity io-queue-capacity}
                  e))))
    result-ch))

(defmacro ^:private io-try- [& body]
  `(io-thread-ch (fn [] ~@body)))

(def ^:private io-sync-translation
  (merge *default-sync-translation* '{io-try- try}))

(defn- reader-input-stream [^Reader reader]
  (let [reader (if (instance? PushbackReader reader)
                 reader
                 (PushbackReader. reader 1))
        pending (atom (byte-array 0))
        pending-index (atom 0)]
    (letfn [(refill! []
              (let [first-char (.read ^PushbackReader reader)]
                (if (= -1 first-char)
                  false
                  (let [first-char (char first-char)
                        chars (if (Character/isHighSurrogate first-char)
                                (let [second-char (.read ^PushbackReader reader)]
                                  (if (and (not= -1 second-char)
                                           (Character/isLowSurrogate (char second-char)))
                                    (char-array [first-char (char second-char)])
                                    (do
                                      (when-not (= -1 second-char)
                                        (.unread ^PushbackReader reader second-char))
                                      (char-array [first-char]))))
                                (char-array [first-char]))]
                    (reset! pending (.getBytes (String. chars) StandardCharsets/UTF_8))
                    (reset! pending-index 0)
                    true))))
            (read-byte! []
              (when (and (>= @pending-index (alength ^bytes @pending))
                         (not (refill!)))
                -1)
              (if (>= @pending-index (alength ^bytes @pending))
                -1
                (let [value (bit-and 0xff (aget ^bytes @pending @pending-index))]
                  (swap! pending-index inc)
                  value)))]
      (proxy [InputStream] []
        (read
          ([] (read-byte!))
          ([target offset length]
           (if (zero? length)
             0
             (loop [written 0]
               (let [value (read-byte!)]
                 (cond
                   (= -1 value) (if (zero? written) -1 written)
                   (= written length) written
                   :else (do
                           (aset-byte ^bytes target (+ offset written)
                                      (unchecked-byte value))
                           (if (= (inc written) length)
                             (inc written)
                             (recur (inc written))))))))))
        (close [] (.close ^PushbackReader reader))))))

(defn- binary-source [value]
  (cond
    (bytes? value)
    {:input (ByteArrayInputStream. ^bytes value) :length (alength ^bytes value)}

    (instance? File value)
    {:input (FileInputStream. ^File value) :length (.length ^File value)}

    (instance? InputStream value)
    {:input value :length nil}

    (instance? Reader value)
    {:input (reader-input-stream value) :length nil}

    (string? value)
    (let [bytes (.getBytes ^String value StandardCharsets/UTF_8)]
      {:input (ByteArrayInputStream. bytes) :length (alength bytes)})

    (and (some? value)
         (.isArray (class value))
         (= Character/TYPE (.getComponentType (class value))))
    (let [bytes (.getBytes (String. ^chars value) StandardCharsets/UTF_8)]
      {:input (ByteArrayInputStream. bytes) :length (alength bytes)})

    :else
    (throw (ex-info "Unsupported binary input."
                    {:type :unsupported-binary-input
                     :input-type (some-> value class str)}))))

(defn- combined-upload-source [^bytes header ^bytes meta value]
  (let [{:keys [input length]} (binary-source value)
        streams [(ByteArrayInputStream. header)
                 (ByteArrayInputStream. meta)
                 input]
        total-length (when (some? length)
                       (+ (long (alength header)) (long (alength meta)) (long length)))]
    {:input (SequenceInputStream. (Collections/enumeration streams))
     :length total-length}))

(defn- component-summary [data]
  (into {}
        (map (fn [[component value]]
               [component
                (cond
                  (bytes? value) {:present? true :bytes (alength ^bytes value)}
                  (nil? value) {:present? false}
                  :else {:present? true :type (str (class value))})]))
        data))

(defn- checked-range!
  [total-size offset length kind limit]
  (let [total-size (long total-size)
        offset (long offset)
        length (long length)
        end (try
              (Math/addExact offset length)
              (catch ArithmeticException _ Long/MAX_VALUE))]
    (when (or (neg? offset)
              (neg? length)
              (> end total-size)
              (and limit (> length (long limit))))
      (throw (ex-info "Invalid or unsafe Azure Blob layout."
                      {:type :invalid-blob-layout
                       :component kind
                       :total-size total-size
                       :offset offset
                       :length length
                       :limit limit})))
    length))

(defn- array-range-length!
  [total-size offset length kind limit]
  (let [length (checked-range! total-size offset length kind limit)]
    (when (> length Integer/MAX_VALUE)
      (throw (ex-info "Azure Blob component exceeds the JVM byte-array limit."
                      {:type :blob-component-too-large
                       :component kind
                       :length length
                       :limit Integer/MAX_VALUE})))
    (int length)))

(defn- await-locked-callback [locked-cb value _sync?]
  (let [result (locked-cb value)
        result (if (satisfies? async-proto/ReadPort result)
                 (<!! result)
                 result)]
    (if (instance? Throwable result)
      (throw result)
      result)))

(extend-protocol PBackingLock
  Boolean
  (-release [_ env]
    (if (:sync? env) nil (go-try- nil))))

(defn- open-blob-input-stream
  ^BlobInputStream [^BlobContainerClient container-client name offset expected-etag]
  (try
    (.openInputStream
     (blob-client container-client name)
     (doto (BlobInputStreamOptions.)
       (.setRange (BlobRange. (long offset)))
       (.setRequestConditions (request-conditions expected-etag)))
     Context/NONE)
    (catch BlobStorageException e
      (if (and expected-etag (precondition-failed? e))
        (throw (optimistic-conflict name expected-etag e))
        (throw e)))))

(defn- cleanup-blob! [backing name data total-size etag]
  (when-let [value (:value @data)]
    (when (instance? Closeable value)
      (try (.close ^Closeable value) (catch Exception _))))
  (reset! data {})
  (reset! total-size nil)
  (reset! etag nil)
  (swap! (:etag-cache backing) dissoc name)
  nil)

(defrecord AzureBlob [backing name data total-size etag]
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
                          {:components (component-summary @data)})))
        (let [{:keys [input length]} (combined-upload-source header meta value)]
          (with-open [input input]
            (when-not (write-stream! (:container-client backing)
                                     name
                                     input
                                     length
                                     (when (pos? retries) current-etag))
              (throw (optimistic-conflict name current-etag nil)))))
        (reset! data {})
        (reset! total-size nil)
        (reset! etag nil)
        (swap! (:etag-cache backing) dissoc name)))))
  (-close [_ env]
    (if (:sync? env)
      (cleanup-blob! backing name data total-size etag)
      (go-try- (cleanup-blob! backing name data total-size etag))))
  (-get-lock [_ env]
    (if (:sync? env) true (go-try- true)))
  (-read-header [_ env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (when-not @total-size
        (let [response (read-range (:container-client backing)
                                   name 0 layout/header-size nil)]
          (when (= ::not-found response)
            (throw (store-key-not-found-ex name)))
          (when (< (:total-size response) 8)
            (throw (ex-info "Azure Blob is too small to contain a konserve header."
                            {:type :invalid-blob-layout
                             :component :header
                             :total-size (:total-size response)})))
          (reset! total-size (:total-size response))
          (reset! etag (:etag response))
          (when (pos? (get-in env [:config :optimistic-locking-retries] 0))
            (swap! (:etag-cache backing) assoc name (:etag response)))
          (reset! data {:header (Arrays/copyOf ^bytes (:data response)
                                               layout/header-size)})))
      (:header @data))))
  (-read-meta [_ meta-size env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (let [offset (long (:header-size env layout/header-size))
            length (array-range-length!
                    @total-size offset meta-size :metadata
                    (get-in env [:config :max-metadata-size]
                            default-max-metadata-size))]
        (:data (read-range (:container-client backing)
                           name offset length @etag))))))
  (-read-value [_ meta-size env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (let [offset (+ (long (:header-size env layout/header-size)) (long meta-size))
            length (array-range-length!
                    @total-size offset (- (long @total-size) offset) :value
                    (get-in env [:config :max-edn-value-size]
                            default-max-edn-value-size))]
        (:data (read-range (:container-client backing)
                           name offset length @etag))))))
  (-read-binary [_ meta-size locked-cb env]
    (async+sync
     (:sync? env) io-sync-translation
     (io-try-
      (let [header-size (long (:header-size env layout/header-size))
            _ (checked-range! @total-size header-size meta-size :metadata
                              (get-in env [:config :max-metadata-size]
                                      default-max-metadata-size))
            offset (+ header-size (long meta-size))
            payload-size (checked-range! @total-size offset
                                         (- (long @total-size) offset)
                                         :binary-value nil)]
        (with-open [input (open-blob-input-stream (:container-client backing)
                                                  name offset @etag)]
          (await-locked-callback locked-cb
                                 {:input-stream input :size payload-size}
                                 (:sync? env)))))))
  (-write-header [_ header env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :header header))))
  (-write-meta [_ meta env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :meta meta))))
  (-write-value [_ value _meta-size env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value :binary? false))))
  (-write-binary [_ _meta-size value env]
    (async+sync (:sync? env) *default-sync-translation*
                (go-try- (swap! data assoc :value value :binary? true))))

  PStreamingBinaryWrite
  (-streaming-binary-write? [_] true))

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
        (into []
              (comp (filter konserve-object?)
                    (map #(subs % prefix-length)))
              (list-object-names container-client prefix)))))))

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
                         (update :buffer-size #(or % default-buffer-size))
                         (assoc :opts opts))]
    (connect-default-store (backing spec) store-config)))

(defn delete-store [spec & {:keys [opts] :or {opts {:sync? true}}}]
  (-delete-store (backing spec) opts))

(defn release [store-instance env]
  (let [release! #(when-let [etag-cache (some-> store-instance :backing :etag-cache)]
                    (reset! etag-cache {}))]
    (if (:sync? env)
      (release!)
      (go-try- (release!)))))

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
