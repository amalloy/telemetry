(ns flatland.telemetry.client
  (:require [clojure.java.io :as io]
            [cheshire.core :refer [encode]]
            [flatland.useful.map :refer [keyed]])
  (:import java.net.Socket))

(defn connect
  "Connect to a telemetry server. Takes :host and :port, which default to
   localhost and 1845 respectively."
  [& {:keys [host port] :or {host "localhost", port 1845}}]
  (let [socket (doto (Socket. host port)
                 (.setKeepAlive true))
        [reader writer] ((juxt io/reader io/writer) socket)]
    (keyed [socket reader writer])))

(defn close
  "Close the server connection."
  [server]
  (.close (:socket server)))

(defn log
  "Log data to a connected telemetry server."
  [server label data]
  (binding [*out* (:writer server)]
    (println (name label) (encode data))))