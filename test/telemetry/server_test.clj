(ns telemetry.server-test
  (:use clojure.test)
  (:require [telemetry.server :as telemetry]
            [telemetry.module.carbon :as carbon]
            [aleph.tcp :as tcp]
            [aleph.http :as http]
            [lamina.core :as lamina]
            [lamina.connections :as connection])
  (:use flatland.useful.debug))

(def carbon-test-port 4006)
(def telemetry-tcp-port 5001)
(def telemetry-http-port 5002)

(deftest test-carbon
  (let [carbon-inputs (lamina/channel)
        carbon-server (tcp/start-tcp-server (fn [ch _]
                                              (lamina/siphon ch carbon-inputs))
                                            {:port carbon-test-port
                                             :frame carbon/wire-format})
        server (telemetry/init {:modules [{:init carbon/init,
                                           :options {:port carbon-test-port}}]
                                :tcp-port telemetry-tcp-port
                                :http-port telemetry-http-port})
        tcp-client @(tcp/tcp-client (merge telemetry/tcp-options
                                           {:host "localhost" :port telemetry-tcp-port}))]
    (try
      (let [url-base (str "http://localhost:" telemetry-http-port)]
        (http/sync-http-request {:method :post :url (str url-base "/listen")
                                 :body "name=x&query=abc.x&type=carbon"
                                 :headers {"content-type" "application/x-www-form-urlencoded"}})
        (lamina/enqueue tcp-client ["abc" "{\"x\":1}"])
        (let [[label value time] @(lamina/read-channel carbon-inputs)]
          (is (= "x" label))
          (is (= 1.0 value))
          (is time)))

      (finally (lamina/close tcp-client)
               (telemetry/destroy! server)
               (carbon-server)))))
