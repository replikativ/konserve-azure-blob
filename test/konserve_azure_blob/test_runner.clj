(ns konserve-azure-blob.test-runner
  (:require [clojure.test :as test]
            [konserve-azure-blob.core-test]))

(defn -main [& _]
  (let [{:keys [fail error]} (test/run-tests 'konserve-azure-blob.core-test)]
    (shutdown-agents)
    (System/exit (if (zero? (+ fail error)) 0 1))))

