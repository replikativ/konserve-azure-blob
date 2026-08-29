(ns konserve-azure-blob.core-test
  (:require [clojure.core.async :refer [<!! go promise-chan put! thread]]
            [clojure.test :refer [deftest is testing]]
            [konserve-azure-blob.core :as azure]
            [konserve.binary :as binary]
            [konserve.compliance-test :refer [compliance-test
                                              conditional-write-compliance-test]]
            [konserve.core :as k]
            [konserve.impl.storage-layout :as layout]
            [konserve.store :as store])
  (:import [com.azure.core.credential AzureSasCredential]
           [com.azure.storage.blob.models ListBlobsOptions]
           [java.io InputStream StringReader]
           [java.nio ByteBuffer]
           [java.nio.charset StandardCharsets]
           [java.util UUID]))

(def emulator-base
  {:backend :azure-blob
   :connection-string "UseDevelopmentStorage=true"
   :container "konserve-test"})

(defn- test-spec []
  (assoc emulator-base :id (UUID/randomUUID)))

(defn- utf8-bytes [value]
  (.getBytes ^String value StandardCharsets/UTF_8))

(defn- string [value]
  (String. ^bytes value StandardCharsets/UTF_8))

(defn- exception-type [value]
  (some-> value ex-data :type))

(defn- generated-input-stream [size]
  (let [position (atom 0)]
    (proxy [InputStream] []
      (read
        ([]
         (if (= @position size)
           -1
           (let [value (mod @position 251)]
             (swap! position inc)
             value)))
        ([target offset length]
         (if (= @position size)
           -1
           (let [count (int (min length (- size @position)))]
             (dotimes [index count]
               (aset-byte ^bytes target (+ offset index)
                          (unchecked-byte (mod (+ @position index) 251))))
             (swap! position + count)
             count)))))))

(defn- stream-summary [^InputStream input]
  (let [buffer (byte-array (* 64 1024))]
    (loop [size 0
           checksum 0]
      (let [read (.read input buffer)]
        (if (= -1 read)
          {:size size :checksum checksum}
          (recur (+ size read)
                 (loop [index 0
                        checksum checksum]
                   (if (= index read)
                     checksum
                     (recur (inc index)
                            (mod (+ checksum (bit-and 0xff (aget buffer index)))
                                 1000000007))))))))))

(defn- expected-summary [size]
  (let [period 251
        period-sum (quot (* period (dec period)) 2)
        complete-periods (quot size period)
        remainder (rem size period)]
    {:size size
     :checksum (mod (+ (* complete-periods period-sum)
                       (quot (* remainder (dec remainder)) 2))
                    1000000007)}))

(deftest azurite-etag-cas-test
  (testing "Azurite enforces ETag If-Match compare-and-swap"
    (let [spec (test-spec)
          service (azure/service-client spec)
          container (.getBlobContainerClient service (:container spec))
          name (str "cas/" (UUID/randomUUID))]
      (.createIfNotExists container)
      (try
        (is (true? (azure/write-object! container name (utf8-bytes "v1") nil)))
        (let [{:keys [etag]} (azure/read-object container name)]
          (is (string? etag))
          (is (true? (azure/write-object! container name (utf8-bytes "v2") etag)))
          (is (false? (azure/write-object! container name (utf8-bytes "stale") etag))
              "the stale ETag must lose with HTTP 412")
          (is (= "v2" (string (:data (azure/read-object container name))))))
        (finally
          (.deleteIfExists (.getBlobClient container name)))))))

(deftest compliance-sync-test
  (let [spec (test-spec)]
    (store/delete-store spec {:sync? true})
    (let [konserve-store (store/create-store spec {:sync? true})]
      (try
        (compliance-test konserve-store)
        (finally
          (store/release-store spec konserve-store {:sync? true})
          (store/delete-store spec {:sync? true}))))))

(deftest compliance-async-test
  (let [spec (test-spec)]
    (<!! (store/delete-store spec {:sync? false}))
    (let [konserve-store (<!! (store/create-store spec {:sync? false}))]
      (try
        (compliance-test konserve-store)
        (finally
          (<!! (store/release-store spec konserve-store {:sync? false}))
          (<!! (store/delete-store spec {:sync? false})))))))

(deftest store-lifecycle-test
  (let [spec (test-spec)]
    (store/delete-store spec {:sync? true})
    (is (false? (store/store-exists? spec {:sync? true})))
    (is (thrown-with-msg? Exception #"does not exist"
                          (store/connect-store spec {:sync? true})))
    (let [konserve-store (store/create-store spec {:sync? true})]
      (is (true? (store/store-exists? spec {:sync? true})))
      (is (thrown-with-msg? Exception #"already exists"
                            (store/create-store spec {:sync? true})))
      (store/release-store spec konserve-store {:sync? true}))
    (store/delete-store spec {:sync? true})
    (is (false? (store/store-exists? spec {:sync? true})))))

(deftest async-delete-reports-completion-test
  (let [spec (test-spec)
        konserve-store (store/create-store spec {:sync? true})]
    (k/assoc konserve-store :key :value {:sync? true})
    (store/release-store spec konserve-store {:sync? true})
    (<!! (store/delete-store spec))
    (is (false? (store/store-exists? spec {:sync? true})))))

(deftest multiple-stores-share-container-test
  (let [spec-a (test-spec)
        spec-b (test-spec)
        store-a (store/create-store spec-a {:sync? true})
        store-b (store/create-store spec-b {:sync? true})]
    (try
      (k/assoc store-a :key :a {:sync? true})
      (k/assoc store-b :key :b {:sync? true})
      (is (= :a (k/get store-a :key nil {:sync? true})))
      (is (= :b (k/get store-b :key nil {:sync? true})))
      (store/delete-store spec-a {:sync? true})
      (is (false? (store/store-exists? spec-a {:sync? true})))
      (is (true? (store/store-exists? spec-b {:sync? true})))
      (is (= :b (k/get store-b :key nil {:sync? true})))
      (finally
        (store/release-store spec-a store-a {:sync? true})
        (store/release-store spec-b store-b {:sync? true})
        (store/delete-store spec-a {:sync? true})
        (store/delete-store spec-b {:sync? true})))))

(deftest optimistic-locking-concurrent-test
  (let [spec (assoc (test-spec) :config {:optimistic-locking-retries 100})
        initial-store (store/create-store spec {:sync? true})
        workers 5
        increments 10]
    (k/assoc initial-store :counter 0 {:sync? true})
    (store/release-store spec initial-store {:sync? true})
    (try
      (let [jobs (doall
                  (for [_ (range workers)]
                    (future
                      (let [worker-store (store/connect-store spec {:sync? true})]
                        (try
                          (dotimes [_ increments]
                            (k/update-in worker-store [:counter] inc {:sync? true}))
                          (finally
                            (store/release-store spec worker-store {:sync? true})))))))]
        (doseq [job jobs] @job)
        (let [result-store (store/connect-store spec {:sync? true})]
          (try
            (is (= (* workers increments)
                   (k/get result-store :counter nil {:sync? true})))
            (finally
              (store/release-store spec result-store {:sync? true})))))
      (finally
        (store/delete-store spec {:sync? true})))))

(deftest read-miss-safe-test
  (let [spec (test-spec)
        konserve-store (azure/connect-store (dissoc spec :backend) :opts {:sync? true})]
    (try
      (is (satisfies? layout/PReadMissSafe (:backing konserve-store)))
      (is (= ::missing (k/get konserve-store :absent ::missing {:sync? true})))
      (finally
        (store/delete-store spec {:sync? true})))))

(deftest async-bget-awaits-callback-test
  (let [spec (test-spec)
        konserve-store (store/create-store spec {:sync? true})]
    (try
      (k/bassoc konserve-store :binary "callback-value" {:sync? true})
      (let [result (<!! (k/bget konserve-store
                                :binary
                                (fn [{:keys [input-stream]}]
                                  (go (slurp input-stream)))
                                {:sync? false :streaming? true}))]
        (is (= "callback-value" result))
        (is (string? result)
            "one take must return the callback value, not another channel"))
      (let [failure (<!! (k/bget konserve-store
                                 :binary
                                 (fn [_]
                                   (doto (promise-chan)
                                     (put! (ex-info "callback failed"
                                                    {:type :callback-failure}))))
                                 {:sync? false :streaming? true}))]
        (is (= :callback-failure (exception-type failure))))
      (finally
        (store/release-store spec konserve-store {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest bounded-streaming-binary-test
  (let [spec (test-spec)
        konserve-store (store/create-store spec {:sync? true})
        size (* 8 1024 1024)]
    (try
      (is (satisfies? layout/PStreamingBinaryWrite
                      (layout/-create-blob (:backing konserve-store)
                                           "streaming-probe.ksv"
                                           {:sync? true})))
      (is (true? (k/bassoc konserve-store :large-stream
                           (generated-input-stream size)
                           {:sync? true})))
      (is (= (expected-summary size)
             (<!! (k/bget konserve-store
                          :large-stream
                          (fn [{:keys [input-stream]}]
                            (thread (stream-summary input-stream)))
                          {:sync? false :streaming? true}))))
      (finally
        (store/release-store spec konserve-store {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest reader-input-stream-preserves-unicode-test
  (let [spec (test-spec)
        konserve-store (store/create-store spec {:sync? true})
        value "emoji: 😀 and ����"]
    (try
      (k/bassoc konserve-store :unicode-reader (StringReader. value) {:sync? true})
      (is (= value
             (String. ^bytes (k/bget konserve-store
                                     :unicode-reader
                                     (binary/to-bytes {:sync? true})
                                     {:sync? true})
                      StandardCharsets/UTF_8)))
      (finally
        (store/release-store spec konserve-store {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest corrupt-metadata-size-is-rejected-test
  (let [spec (test-spec)
        konserve-store (store/create-store spec {:sync? true})
        service (azure/service-client spec)
        container (.getBlobContainerClient service (:container spec))]
    (try
      (k/assoc konserve-store :victim {:small true} {:sync? true})
      (let [prefix (str (azure/spec->store-path spec) "/")
            options (doto (ListBlobsOptions.) (.setPrefix prefix))
            name (->> (.iterator (.listBlobs container options nil))
                      iterator-seq
                      (map #(.getName %))
                      (filter #(.endsWith ^String % ".ksv"))
                      first)
            object (azure/read-object container name)
            bytes ^bytes (:data object)]
        (.putInt (ByteBuffer/wrap bytes) 4 (* 1024 1024 1024))
        (azure/write-object! container name bytes nil)
        (let [failure (try
                        (k/get konserve-store :victim nil {:sync? true})
                        nil
                        (catch Exception e e))]
          (is (= :invalid-blob-layout (exception-type failure)))))
      (finally
        (store/release-store spec konserve-store {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest etag-cache-does-not-retain-read-keys-test
  (let [spec (assoc (test-spec) :config {:optimistic-locking-retries 10})
        konserve-store (store/create-store spec {:sync? true})]
    (try
      (doseq [key (range 50)]
        (k/assoc konserve-store key key {:sync? true}))
      (doseq [key (range 50)]
        (is (= key (k/get konserve-store key nil {:sync? true}))))
      (is (empty? @(:etag-cache (:backing konserve-store))))
      (finally
        (store/release-store spec konserve-store {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest stale-read-does-not-condition-full-overwrite-test
  (let [spec (assoc (test-spec) :config {:optimistic-locking-retries 3})
        store-a (store/create-store spec {:sync? true})
        store-b (store/connect-store spec {:sync? true})]
    (try
      (k/assoc store-a :key :v1 {:sync? true})
      (is (= :v1 (k/get store-a :key nil {:sync? true})))
      (k/assoc store-b :key :v2 {:sync? true})
      (is (= [nil :v3] (k/assoc store-a :key :v3 {:sync? true})))
      (is (= :v3 (k/get store-b :key nil {:sync? true})))
      (finally
        (store/release-store spec store-a {:sync? true})
        (store/release-store spec store-b {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest caches-and-queues-are-bounded-test
  (azure/clear-client-cache!)
  (try
    (is (thrown? Exception (azure/service-client {})))
    (is (zero? (azure/client-cache-size))
        "failed client construction must not poison the cache")
    (let [credential (AzureSasCredential. "sig=test")]
      (doseq [port (range 20000 (+ 20000 azure/max-cached-clients 10))]
        (azure/service-client {:endpoint (str "http://127.0.0.1:" port)
                               :credential credential}))
      (is (= azure/max-cached-clients (azure/client-cache-size))))
    (is (= azure/io-queue-capacity
           (:queue-capacity (azure/io-executor-stats))))
    (finally
      (azure/clear-client-cache!))))

(deftest backing-key-listing-is-realized-test
  (let [spec (test-spec)
        konserve-store (store/create-store spec {:sync? true})]
    (try
      (k/assoc konserve-store :key :value {:sync? true})
      (is (vector? (layout/-keys (:backing konserve-store) {:sync? true})))
      (finally
        (store/release-store spec konserve-store {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest incomplete-write-error-omits-payload-test
  (let [spec (test-spec)
        konserve-store (store/create-store spec {:sync? true})
        blob (layout/-create-blob (:backing konserve-store)
                                  "incomplete.ksv"
                                  {:sync? true})
        secret (utf8-bytes "payload-that-must-not-appear-in-errors")]
    (try
      (layout/-write-header blob secret {:sync? true})
      (let [failure (try
                      (layout/-sync blob {:sync? true :config {}})
                      nil
                      (catch Exception e e))
            error-data (ex-data failure)]
        (is (instance? Exception failure))
        (is (= {:header {:present? true :bytes (alength secret)}}
               (:components error-data)))
        (is (not (contains? error-data :data)))
        (is (not (.contains (pr-str error-data)
                            "payload-that-must-not-appear-in-errors"))))
      (finally
        (layout/-close blob {:sync? true})
        (store/release-store spec konserve-store {:sync? true})
        (store/delete-store spec {:sync? true})))))

(deftest azure-conditional-write-test
  (testing "the `:expected-revision` contract against a real Azure Blob endpoint.

            This backing answers `:global`, and it can: the comparison is Azure's
            own If-Match, evaluated by the service, not a lock local to a heap or
            a host. Running konserve's shared contract against a live endpoint is
            the only thing standing between that claim and a deployment trusting
            it -- a backing that declares a domain it cannot honour is worse than
            one that declares none, because the caller believes it is fenced."
    (let [spec (test-spec)
          _    (try (store/delete-store spec {:sync? true}) (catch Exception _ nil))
          s    (store/create-store spec {:sync? true})]
      (try
        (is (= :global (k/conditional-write-domain s))
            "Azure evaluates the precondition, so the domain reaches every writer")
        (conditional-write-compliance-test s)
        (finally
          (store/release-store spec s {:sync? true})
          (try (store/delete-store spec {:sync? true}) (catch Exception _ nil)))))))

(deftest azure-refuses-a-precondition-it-cannot-build-test
  (testing "an `:expected-revision` with no ETag to fence against must be REFUSED.

            Writing unconditionally there would hand back a knob that reads as
            handled: the caller asked for a compare-and-set and would get a plain
            overwrite. Asserted at the blob, not through the store, because
            konserve performs the write's own header read and so always supplies
            an ETag on the public path -- the guard is defensive, and the only
            honest way to cover it is to construct the state it defends against."
    (let [spec (test-spec)
          _    (try (store/delete-store spec {:sync? true}) (catch Exception _ nil))
          st   (store/create-store spec {:sync? true})]
      (try
        (k/assoc st :some-key {:v 1} {:sync? true})
        (let [backing (:backing st)
              ;; A blob that has read nothing: no ETag of its own, none cached.
              blob (layout/-create-blob backing :some-key {:sync? true})
              _    (reset! (:etag-cache backing) {})
              bytes (fn [n] (byte-array (repeat n (byte 1))))
              _    (reset! (:data blob) {:header (bytes 8) :meta (bytes 4) :value (bytes 4)})
              r    (try (layout/-sync blob {:sync? true :expected-revision "no-etag-was-read"})
                        (catch Exception e e))]
          (is (= :konserve/conditional-write-unsupported (:type (ex-data r)))
              "refuses rather than silently writing unconditionally"))
        (is (= {:v 1} (k/get st :some-key nil {:sync? true}))
            "and the refused write left the stored value untouched")
        (finally
          (store/release-store spec st {:sync? true})
          (try (store/delete-store spec {:sync? true}) (catch Exception _ nil)))))))
