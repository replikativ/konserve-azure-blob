(ns konserve-azure-blob.core-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [konserve-azure-blob.core :as azure]
            [konserve.compliance-test :refer [compliance-test]]
            [konserve.core :as k]
            [konserve.impl.storage-layout :as layout]
            [konserve.store :as store])
  (:import [java.nio.charset StandardCharsets]
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
