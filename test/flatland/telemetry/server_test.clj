(ns flatland.telemetry.server-test
  (:use clojure.test)
  (:require [flatland.telemetry.server :as telemetry]
            [flatland.telemetry.graphite :as graphite]
            [aleph.tcp :as tcp]
            [aleph.http :as http]
            [lamina.core :as lamina]
            [lamina.connections :as connection])
  (:use flatland.useful.debug))

(def telemetry-tcp-port 5001)
(def telemetry-http-port 5002)
(def graphite-test-port 5003)

(deftest test-graphite
  (let [graphite-inputs (lamina/channel)
        graphite-server (tcp/start-tcp-server (fn [ch _]
                                                (lamina/siphon ch graphite-inputs))
                                              {:port graphite-test-port
                                               :frame graphite/wire-format})
        server (telemetry/init {:modules [{:init graphite/init,
                                           :options {:port graphite-test-port}}]
                                :tcp-port telemetry-tcp-port
                                :http-port telemetry-http-port})
        tcp-client @(tcp/tcp-client (merge telemetry/tcp-options
                                           {:host "localhost" :port telemetry-tcp-port}))]
    (try
      (let [url-base (str "http://localhost:" telemetry-http-port)]
        (http/sync-http-request {:method :post :url (str url-base "/listen")
                                 :body "{\"name\": \"x\", \"query\": \"abc.x\", \"type\": \"graphite\"}"
                                 :headers {"content-type" "application/json"}})
        (lamina/enqueue tcp-client ["abc" "{\"x\":1}"])
        (let [[label value time] @(lamina/read-channel graphite-inputs)]
          (is (= "x" label))
          (is (= 1.0 value))
          (is time)))
      
      (finally (lamina/close tcp-client)
               (telemetry/destroy! server)
               (graphite-server)))))
