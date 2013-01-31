(ns telemetry.core
  (:use [ring.middleware params keyword-params])
  (:require
   [clojure.string :as str]
   [swank.swank :as swank]
   (lamina [core :as lamina]
           [connections :as connection]
           [trace :as trace]
           [cache :as cache])
   [lamina.trace.router.core :as router]
   (gloss [core :as gloss])
   (aleph [formats :as formats]
          [http :as http]
          [tcp :as tcp])
   [compojure.core :refer [routes GET POST]]
   [flatland.useful.utils :refer [returning]]
   [flatland.useful.map :refer [keyed map-vals]])
  (:import java.util.Date)
  (:use flatland.useful.debug))

(def tcp-port 1845)
(def http-port 1846)

(def aggregation-period
  "Number of milliseconds lamina should buffer up periodic data before flushing."
  30000)

(defn input-handler
  "Forwards each message it receives into the trace router."
  [ch _]
  (let [ch* (->> ch
                 (lamina/map* (fn [[probe data]]
                                [(str/replace probe #"\." ":")
                                 (formats/decode-json data)])))]
    (-> ch*
        (doto (lamina/on-error (fn [e]
                                 (spit "log.clj" (pr-str ["closing channel" e]) :append true))))
        (lamina/receive-all
         (fn [[probe data]]
           (trace/trace* probe [probe data]))))))

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
  (let [data-sink (if (re-find #"\*\d" name)
                    (let [name (str/replace name #"\*(?!\d)" "*1")]
                      (sink-by-name name rename-multiple))
                    (if (re-find #"\*" name)
                      (sink-by-name name rename-one)
                      (timed-sink name)))]
    (fn [[probe data]]
      (data-sink data))))

(defn subscribe [query]
  (cache/subscribe trace/local-trace-router query :period aggregation-period))

;;; functions that work on the listener list, and return Ring responses

(defn inspector
  "Inspect or debug a probe query without sending it to graphite, by returning an endless, streaming
   HTTP response of its values."
  [query]
  {:status 200
   :headers {"content-type" "application/json"}
   :body (->> (subscribe (formats/url-decode query))
              (lamina/map* (fn [[probe data]]
                             (str probe "\n" (pr-str data) "\n"))))})

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
  (let [channel (doto (subscribe query)
                  (-> (->> (lamina/mapcat* (graphite-sink name)))
                      (lamina/siphon graphite-nexus)))
        unsubscribe #(lamina/close channel)]
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

(defn wrap-debug [handler]
  (fn [req]
    (?! (handler (?! req)))))

(defn carbon-channel
  "Produce a channel that receives [name data time] tuples and sends them, formatted for carbon,
  to a server on the specified host and port."
  [host port]
  (tcp/tcp-client {:host host :port port
                   :frame [(gloss/string :utf-8 :delimiters [" "]) ;; message type
                           (gloss/string-float :utf-8 :delimiters [" "]) ;; count
                           (gloss/string-integer :utf-8 :delimiters ["\n"])]})) ;; timestamp

(defn init-carbon-connection
  "Starts a channel that will siphon from the carbon nexus into a carbon server forever.
   Returns a thunk that will close the connection."
  [nexus host port]
  (let [carbon-connector (connection/persistent-connection
                          #(carbon-channel host port)
                          {:on-connected (fn [ch]
                                           (lamina/ground ch)
                                           (lamina/siphon nexus ch))})]
    (carbon-connector)
    (fn []
      (connection/close-connection carbon-connector))))

(defn carbon-init [{:keys [host port] :or {host "localhost" port 2003}}]
  (let [nexus (lamina/channel* :permanent? true :grounded? true)
        stop-carbon (init-carbon-connection nexus host port)]

    {:name :carbon
     :shutdown stop-carbon
     :handler (constantly nil)
     :listen (fn listen [name ch]
               (-> ch
                   (->> (lamina/mapcat* (graphite-sink name)))
                   (lamina/siphon nexus)))}))

(defn telemetry-init [{:keys [modules aggregation-period] :as config}]
  (let [modules (into {} (for [{:keys [init options]} modules]
                           (let [module (init options)]
                             (when-not (:shutdown module)
                               (throw (Exception. (format "Module %s must provide a shutdown hook."
                                                          (:name module)))))
                             [(:name module) module])))
        config (doto (assoc config
                       :listeners (ref {})
                       :modules modules)
                 (restore-listeners))
        tcp (tcp/start-tcp-server
             input-handler
             {:port tcp-port
              :delimiters ["\r\n" "\n"]
              :frame [(gloss/string :utf-8 :delimiters [" "])
                      (gloss/string :utf-8)]})
        http (http/start-http-server
              (let [writers (routes (POST "/listen" [type name query]
                                      (add-listener config (keyword type) name query))
                                    (POST "/forget" [type name]
                                      (remove-listener config (keyword type) name)))
                    readers (routes (GET "/inspect" [query]
                                      (inspector query))
                                    (GET "/listeners" [type]
                                      (get-listeners listeners (keyword type))))]
                (-> (routes (wrap-saving-listeners writers config)
                            readers)
                    wrap-keyword-params
                    wrap-params
                    http/wrap-ring-handler))

              {:port http-port})]

    {:shutdown (fn []
                 (tcp)
                 (http)
                 (doseq [{:keys [shutdown]} (:modules config)]
                   (shutdown)))}))

(defn -main [& args]
  (when-let [[period] (seq args)]
    (alter-var-root #'aggregation-period (constantly (Long/parseLong period))))
  (init))
