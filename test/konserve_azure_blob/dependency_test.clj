(ns konserve-azure-blob.dependency-test
  "Keep the adapter's synchronous HTTP provider explicit and lightweight."
  (:require [clojure.test :refer [deftest is testing]]
            [konserve-azure-blob.core :as azure])
  (:import [com.azure.core.http HttpClient]))

(deftest service-client-uses-jdk-http-without-netty-runtime
  (testing "the synchronous service client still initializes"
    (is (some? (azure/build-service-client
                {:connection-string "UseDevelopmentStorage=true"}))))
  (testing "the Azure SDK selects the JDK provider"
    (is (= "com.azure.core.http.jdk.httpclient.JdkHttpClient"
           (.getName (class (HttpClient/createDefault))))))
  (testing "the unused Netty provider and native runtime are absent"
    (is (thrown? ClassNotFoundException
                 (Class/forName
                  "com.azure.core.http.netty.NettyAsyncHttpClientBuilder")))
    (is (thrown? ClassNotFoundException
                 (Class/forName "io.netty.channel.Channel")))))
