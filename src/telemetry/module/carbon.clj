(ns telemetry.module.carbon
  (:require [lamina.core :as lamina]
            [aleph.tcp :as tcp]
            [gloss.core :as gloss]
            [telemetry.graphite.common :as graphite]
            [lamina.connections :as connection]
            [telemetry.module.carbon.config :as config]
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
      (with-open [reader (config-reader)]
        (let [lines (line-seq reader)
              rules (config/parse-carbon-config lines)]
          (first (for [{:keys [pattern retentions]} rules
                       :when (re-find pattern label)]
                   (* 1000 (config/as-seconds (:granularity (first retentions)))))))))))

(defn carbon-channel
  "Produce a channel that receives [name data time] tuples and sends them, formatted for carbon,
  to a server on the specified host and port."
  [host port]
  (tcp/tcp-client {:host host :port port :frame wire-format}))

(defn init-connection
  "Starts a channel that will siphon from the given nexus into a carbon server forever.
   Returns a thunk that will close the connection."
  [nexus host port]
  (let [carbon-connector (connection/persistent-connection
                          #(carbon-channel host port)
                          {:on-connected (fn [ch]
                                           (lamina/ground ch) ;; ignore input from server
                                           (lamina/siphon nexus ch))})]
    (carbon-connector)
    (fn []
      (connection/close-connection carbon-connector))))

(defn init [{:keys [host port config-reader] :or {host "localhost" port 2003}}]
  (let [nexus (lamina/channel* :permanent? true :grounded? true)
        stop-carbon (init-connection nexus host port)]

    {:name :carbon
     :shutdown stop-carbon
     :handler (GET "/render" []
                {:status 200 :body "Carbon's sample handler."})
     :period (granularity-decider config-reader)
     :listen (fn listen [ch name]
               (-> ch
                   (->> (lamina/mapcat* (graphite/graphite-sink name)))
                   (lamina/siphon nexus)))}))
