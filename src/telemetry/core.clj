(ns telemetry.core
  (:use [ring.middleware params keyword-params])
  (:require
   [clojure.string :as str]
   [swank.swank :as swank]
   (lamina [core :as lamina]
           [connections :as connection]
           trace)
   (gloss [core :as gloss])
   (aleph [trace :as trace]
          [formats :as formats]
          [http :as http]
          [tcp :as tcp])
   [compojure.core :refer [routes GET POST]]
   [flatland.useful.utils :refer [returning]]
   [flatland.useful.map :refer [keyed map-vals]])
  (:import java.util.Date)
  (:use flatland.useful.debug))

(def tcp-port 1845)
(def http-port 1846)
(def trace-port 1847)

(def trace-router
  "A server forwarding traces on the trace port to all subscribers.
   Start the server by forcing this delay object."
  (delay (trace/start-trace-router {:port trace-port})))

(def endpoint
  (delay
    (trace/trace-endpoint {:client-options {:host "localhost" :port trace-port}})))

(defn input-handler
  "Forwards each message it receives into the trace router."
  [ch _]
  (lamina/receive-all ch
    (fn [[probe data]]
      (let [probe (str/replace probe #"\." ":")]
        (lamina.trace/trace* probe
          (formats/decode-json data))))))

(defn graphite-channel
  "Produce a channel that receives [name data time] tuples and sends them, formatted for graphite,
  to a server on the specified host and port."
  [host port]
  (tcp/tcp-client {:host host :port port
                   :frame [(gloss/string :utf-8 :delimiters [" "]) ;; message type
                           (gloss/string-float :utf-8 :delimiters [" "]) ;; count
                           (gloss/string-integer :utf-8 :delimiters ["\n"])]})) ;; timestamp

(defn outgoing-channel-generator [host port]
  (connection/persistent-connection
   (fn []
     (doto @(graphite-channel host port)
       (lamina/ground))))) ;; we will only ever look at the write end of the channel

(def outgoing-channel*
  "Manages a persistent TCP connection to the graphite server;
   deref to get a function that produces a lamina connection."
  (promise))

(defn outgoing-channel
  "Produces the write half of a lamina connection to the graphite server; all
   incoming data from the server is discarded."
  []
  @(@outgoing-channel*))

(defn unix-time
  "Number of seconds since the unix epoch, as by Linux's time() system call."
  [^Date date]
  (-> date (.getTime) (quot 1000)))

(def listeners
  "A map whose keys are graphite names or patterns (eg, new.users or page.views.*) and whose values
   are objects with the information necessary to supply data for that name: the lamina query
   descriptor to send, the lamina connection itself, and a thunk to unsubscribe that query."
  (ref {}))

(defn readable-listeners
  "A view of the listeners map which can be printed and reread."
  [listeners]
  (map-vals listeners :query))

;;; functions for interpolating values into patterns

(defn rename-multiple
  "Takes a pattern with N wildcards like foo.*1.blah.*2 and a list of N values, and replaces each
   wildcard with the matching value from the list."
  [pattern keys]
  (reduce (fn [name [attr i]]
            (str/replace name (str "*" i)
                         (str attr)))
          pattern
          (map vector keys (iterate inc 1))))

(defn rename-one
  "Replaces all *s in the pattern with the value of key."
  [pattern key]
  (str/replace pattern #"\*" key))

;;; lamina channel transformers, to turn values from a probe descriptor into a sequence of tuples
;;; suitable for encoding and sending out to the graphite server.

(defn timed-sink
  "Returns a function which emits each datum decorated with the given label and the current time."
  [name]
  (fn [data]
    (let [now (unix-time (Date.))]
      [[name data now]])))

(defn sink-by-name
  "Returns a function which expects to receive a map of labels to values. Each label has its
   value(s) interpolated into the pattern, and a list of [name label time] tuples is returned."
  [pattern rename-fn]
  (fn [keyed-numbers]
    (let [now (unix-time (Date.))]
      (for [[k v] keyed-numbers]
        (let [name (rename-fn pattern k)]
          [name v now])))))

(defn graphite-sink
  "Determines what kind of pattern name is, and creates an appropriate transformer for its channel.
   name may be:
   - an ordinary name (in which case its values are emitted unchanged),
   - a pattern containing the * wildcard (in which case it is assumed to be grouped-by one field),
   - a pattern with *1, *2 wildcards (in which case it is assumed to be grouped-by a tuple)."
  [name]
  (if (re-find #"\*\d" name)
    (let [name (str/replace name #"\*(?!\d)" "*1")]
      (sink-by-name name rename-multiple))
    (if (re-find #"\*" name)
      (sink-by-name name rename-one)
      (timed-sink name))))

;;; functions that work on the listener list, and return Ring responses

(defn inspector
  "Inspect or debug a probe query without sending it to graphite, by returning an endless, streaming
   HTTP response of its values."
  [query]
  {:status 200
   :headers {"content-type" "application/json"}
   :body (->> (trace/subscribe @endpoint (formats/url-decode query))
              :messages
              (lamina/map* formats/encode-json->string)
              (lamina/map* #(str % "\n")))})

(defn remove-listener
  "Disconnects any channel writing to the given name/pattern."
  [name]
  (if-let [{:keys [unsubscribe query]} (dosync (returning (get @listeners name)
                                                 (alter listeners dissoc name)))]
    (do (unsubscribe)
        {:status 200 :headers {"content-type" "application/json"}
         :body (formats/encode-json->string {:query query})})
    {:status 404}))

(defn add-listener
  "Connects a channel from the queried probe descriptor to a graphite sink for the given name or
  pattern. Implicitly disconnects any existing writer from that sink first."
  [name query]
  (remove-listener name)
  (let [channel (doto (trace/subscribe @endpoint query)
                  (-> (:messages)
                      (->> (lamina/mapcat* (graphite-sink name)))
                      (lamina/siphon (outgoing-channel))))
        unsubscribe #(lamina/close (:messages channel))]
    (dosync
     (alter listeners assoc name (keyed [query channel unsubscribe])))
    {:status 204})) ;; no content

(defn get-listeners
  "Produces a summary of all probe queries and the graphite names they're writing to."
  []
  {:status 200 :headers {"content-type" "application/json"}
   :body (formats/encode-json->string (readable-listeners @listeners))})

(let [config-file "config.clj"]
  (defn save-listeners
    "Saves the current listeners to a file in the current directory."
    [listeners]
    (spit config-file (pr-str (readable-listeners listeners))))
  (defn restore-listeners
    "Restores listeners from a file in the current directory."
    []
    (try
      (doseq [[name query] (read-string (slurp config-file))]
        (add-listener name query))
      (catch Exception e nil))))

(defn wrap-saving-listeners
  "Wraps a ring handler such that, if the handler succeeds, the current listener set is saved before
  returning."
  [handler]
  (fn [request]
    (when-let [response (handler request)]
      (save-listeners @listeners)
      response)))

(def tcp-server "A tcp server accepting traces via a simple text protocol."
  (atom nil))
(def http-server "A basic HTTP API to configure what traces are passed on to graphite."
  (atom nil))

(defn init
  ([] (init "localhost" 2003))
  ([graphite-host graphite-port]
     (force trace-router)
     (deliver outgoing-channel* (outgoing-channel-generator graphite-host graphite-port))

     (reset! tcp-server
             (tcp/start-tcp-server
              input-handler
              {:port tcp-port
               :delimiters ["\r\n" "\n"]
               :frame [(gloss/string :utf-8 :delimiters [" "])
                       (gloss/string :utf-8)]}))

     (restore-listeners)

     (reset! http-server
             (http/start-http-server
              (let [writers (routes (POST "/listen" [name query]
                                      (add-listener name query))
                                    (POST "/forget" [name]
                                      (remove-listener name)))
                    readers (routes (GET "/inspect" [query]
                                      (inspector query))
                                    (GET "/listeners" []
                                      (get-listeners)))]
                (-> (routes (wrap-saving-listeners writers)
                            readers)
                    wrap-keyword-params
                    wrap-params
                    http/wrap-ring-handler))

              {:port http-port}))

     (let [host "localhost" port 4005]
       (printf "Starting swank on %s:%d\n" host port)
       (swank/start-server :host host :port port))
     nil))

(defn stop []
  (@tcp-server)
  (reset! tcp-server nil)

  (@http-server)
  (reset! http-server nil)

  (save-listeners (dosync (returning @listeners
                            (ref-set listeners {})))))

(defn -main [& args]
  (init))
