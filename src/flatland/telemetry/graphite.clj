(ns flatland.telemetry.graphite
  "Module for sending data to graphite."
  (:require [lamina.core :as lamina]
            [aleph.tcp :as tcp]
            [gloss.core :as gloss]
            [flatland.telemetry.sinks :as sinks]
            [lamina.connections :as connection]
            [flatland.telemetry.graphite.config :as config]
            [compojure.core :refer [GET]])
  (:use flatland.useful.debug))

(def wire-format
  (gloss/compile-frame [(gloss/string :utf-8 :delimiters [" "])             ;; message type
                        (gloss/string-float :utf-8 :delimiters [" "])       ;; count
                        (gloss/string-integer :utf-8 :delimiters ["\n"])])) ;; timestamp

(defn granularity-decider
  "Given a thunk that produces a reader over carbon's storage-schemas.conf file, returns a function
  from label (ie name) to granularity, in milliseconds. Note that this reloads the config file every
  time a granularity is requested, to allow for the possibility that the configuration has
  changed. This will only happen when a new listener is added, so it shouldn't be a big deal; if
  you'd rather never reload, you can provide a thunk that uses your own preferred caching method."
  [config-reader]
  (when config-reader
    (fn [label]
      (with-open [^java.io.Closeable reader (config-reader)]
        (let [lines (line-seq reader)
              rules (config/parse-carbon-config lines)]
          (first (for [{:keys [pattern retentions]} rules
                       :when (re-find pattern label)]
                   (* 1000 (config/as-seconds (:granularity (first retentions)))))))))))

(defn graphite-channel
  "Produce a channel that receives [name data time] tuples and sends them, formatted for graphite,
  to a server on the specified host and port."
  [host port]
  (tcp/tcp-client {:host host :port port :frame wire-format}))

(defn init-connection
  "Starts a channel that will siphon from the given nexus into a graphite server forever.
   Returns a thunk that will close the connection."
  [nexus host port]
  (let [graphite-connector (connection/persistent-connection
                            #(graphite-channel host port)
                            {:on-connected (fn [ch]
                                             (lamina/ground ch) ;; ignore input from server
                                             (lamina/siphon (lamina/map* #(s/replace % ":" ".")
                                                                         nexus)
                                                            ch))})]
    (graphite-connector)
    (fn []
      (connection/close-connection graphite-connector))))

(defn init [{:keys [host port config-reader] :or {host "localhost" port 2003}}]
  (let [nexus (lamina/channel* :permanent? true :grounded? true)
        stop-graphite (init-connection nexus host port)]
    {:name :graphite
     :shutdown stop-graphite
     :handler (GET "/render" []
                   {:status 200 :body "Graphite's sample handler."})
     :period (granularity-decider config-reader)
     :listen (fn listen [ch name]
               (-> ch
                   (->> (lamina/mapcat* (sinks/sink name)))
                   (lamina/siphon nexus)))}))
