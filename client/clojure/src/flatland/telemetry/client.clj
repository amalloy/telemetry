(ns flatland.telemetry.client
  (:require [aleph.tcp :as tcp]
            [aleph.formats :as formats]
            [gloss.core :as gloss]
            [lamina.core :as lamina]
            [flatland.laminate :as laminate]))

(defn channel
  "Creates a channel for writing to a telemetry server. It expects to receive messages of the form
   [\"topic:name:here\" {:foo {:whatever 1} :bar [10]}], and will send it to the server."
  [host port]
  (tcp/tcp-client {:host host :port port :delimiters ["\r\n" "\n"]
                   :frame [(gloss/string :utf-8 :delimiters [" "])
                           (gloss/compile-frame (gloss/string :utf-8)
                                                formats/encode-json->string
                                                identity)]}))

(defn client
  "Opens a persistent connection to a telemetry server. Returns a map of two functions, :send
  and :close. :send takes two arguments, a topic and a message; close takes none. Any messages sent
  via :send will be forwarded to the server, and will be queued if the server is unreachable.

  Optionally, a :queue-mode of :queue or :discard may be specified: in :discard mode, messages sent
  while the server is unreachable will be discarded instead of queued. :queue mode is the default."
  ([host port]
     (client host port {:queue-mode :queue}))
  ([host port {:keys [queue-mode]}]
     (let [nexus (lamina/channel* :permanent? true :grounded? (case queue-mode
                                                                :queue false
                                                                :discard true))]
       {:send (fn [topic message] (lamina/enqueue nexus [topic message]))
        :close (laminate/persistent-stream nexus #(channel host port))})))
