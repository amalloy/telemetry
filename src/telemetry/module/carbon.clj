(ns telemetry.module.carbon
  (:require [lamina.core :as lamina]
            [aleph.tcp :as tcp]
            [gloss.core :as gloss]
            [telemetry.graphite.common :as graphite]
            [lamina.connections :as connection]
            [compojure.core :refer [GET]])
  (:use flatland.useful.debug))

(def wire-format
  (gloss/compile-frame [(gloss/string :utf-8 :delimiters [" "])             ;; message type
                        (gloss/string-float :utf-8 :delimiters [" "])       ;; count
                        (gloss/string-integer :utf-8 :delimiters ["\n"])])) ;; timestamp

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

(defn init [{:keys [host port] :or {host "localhost" port 2003}}]
  (let [nexus (lamina/channel* :permanent? true :grounded? true)
        stop-carbon (init-connection nexus host port)]

    {:name :carbon
     :shutdown stop-carbon
     :handler (GET "/render" []
                {:status 200 :body "Carbon's sample handler."})
     :listen (fn listen [ch name]
               (-> ch
                   (->> (lamina/mapcat* (graphite/graphite-sink name)))
                   (lamina/siphon nexus)))}))
