(ns flatland.telemetry.server
  (:use [ring.middleware params keyword-params format-params format-response])
  (:require
   [clojure.string :as s]
   [clojure.java.io :as io]
   (lamina [core :as lamina]
           [connections :as connection]
           [query :as query]
           [trace :as trace])
   [lamina.trace.router :as router]
   lamina.query.struct
   (gloss [core :as gloss])
   (aleph [formats :as formats]
          [http :as http]
          [tcp :as tcp])
   [compojure.core :refer [routes GET POST ANY context]]
   [compojure.route :refer [resources]]
   [flatland.useful.utils :refer [returning]]
   [flatland.useful.map :refer [update keyed map-vals ordering-map]]
   [flatland.telemetry.util :refer [unix-time from-unix-time]]
   [flatland.telemetry.graphite :as graphite]
   [flatland.telemetry.phonograph :as phonograph]
   [flatland.telemetry.cassette :as cassette])
  (:import (java.io StringReader BufferedReader IOException)
           (java.util Date))
  (:use flatland.useful.debug))

(def default-tcp-port
  "The port on which telemetry listens for incoming traces."
  1845)
(def default-http-port
  "The port to run the telemetry administration webserver on."
  1846)

(def tcp-options
  "The gloss frame the telemetry tcp server expects to receive."
  {:delimiters ["\r\n" "\n"]
   :frame [(gloss/string :utf-8 :delimiters [" "])
           (gloss/string :utf-8)]})

(def default-aggregation-period
  "Number of milliseconds lamina should buffer up periodic data before flushing."
  30000)

(defn subscribe
  "Subscribe to the given trace descriptor, returning a channel of the resuts. Sets the
   period for all periodic operators before parsing."
  [query period]
  (trace/subscribe trace/local-trace-router (str "&" query)
                   {:period period}))

(defn try-subscribe [query period]
  (try
    {:success true, :value (subscribe query period)}
    (catch Exception e
      {:success false, :value (.getMessage e)})))

;;; functions that work on the query list, and return Ring responses

(defn inspector
  "Inspect or debug a probe query without sending it to any queries, by returning an endless,
   streaming HTTP response of its values."
  [config query period]
  (let [{:keys [success value]} (try-subscribe (formats/url-decode query)
                                               (or period (:period config)))]
    (if success
      {:status 200
       :headers {"content-type" "application/json"}
       :body (->> value
                  (lamina/map* pr-str)
                  (lamina/map* #(str % "\n")))}
      {:status 400
       :headers {"content-type" "text/plain"}
       :body value})))

(defn remove-query
  "Disconnects any channel writing to the given name/pattern."
  [config type name]
  (let [queries (:queries config)]
    (if-let [{:keys [unsubscribe query]} (dosync (returning (get-in @queries [type name])
                                                   (alter queries update type dissoc name)))]
      (do (unsubscribe)
          {:status 200 :headers {"content-type" "application/json"}
           :body (formats/encode-json->string {:query query})})
      {:status 404})))

(defn replay [config query period start-time]
  (let [replayer (some :replay (vals (:modules config)))
        data-seq (-> (query/query-seqs
                      {query nil}
                      {:timestamp #(* 1000 (:timestamp %)) :payload identity :period period
                       :seq-generator (fn [pattern]
                                        (replayer {:pattern pattern :start-time start-time}))})
                     (get query))]
    data-seq))

(defn stitch-replay [live-channel old-seq response-channel]
  (let [[old-channel combined] (repeatedly lamina/channel)
        latest-date (atom 0)
        message-count (atom 0)]
    (lamina/receive-all old-channel (fn [{:keys [timestamp]}]
                                      (swap! message-count inc)
                                      (swap! latest-date max timestamp)))
    (-> old-channel
        (->> (lamina/sample-every {:period 5000})
             (lamina/map* (fn [{:keys [timestamp]}]
                            (str @message-count " " (from-unix-time timestamp) "\n"))))
        (lamina/join response-channel))
    (lamina/siphon old-channel combined)
    (lamina/on-drained old-channel
                       (fn []
                         (let [switchover-time @latest-date]
                           (lamina/siphon (lamina/drop-while* (fn [{:keys [timestamp]}]
                                                                (<= timestamp switchover-time))
                                                              live-channel)
                                          combined))))
    (lamina/join (lamina/lazy-seq->channel old-seq) old-channel)
    combined))

(defn maybe-replay [config live-channel query period replay-since response-channel]
  (if-not replay-since
    (do
      (lamina/enqueue-and-close response-channel "OK\n")
      live-channel)
    (-> live-channel
        (stitch-replay (replay config query period replay-since) response-channel))))

(defn add-query
  "Connects a channel from the queried probe descriptor to a graphite sink for the given name or
  pattern. Implicitly disconnects any existing writer from that sink first."
  [config type label target query replay-since]
  (let [{:keys [listen period]} (get-in config [:modules type])]
    (if listen
      (let [period (period label)
            {:keys [success value]} (try-subscribe query period)]
        (if success
          (do
            (remove-query config type label)
            (let [response-channel (lamina/channel)
                  live-channel (->> value
                                    (lamina/map* (fn [obj]
                                                   {:timestamp (unix-time (Date.))
                                                    :value obj})))
                  channel (maybe-replay config live-channel query period replay-since response-channel)
                  unsubscribe #(lamina/close channel)]
              (dosync
               (alter (:queries config)
                      assoc-in [type label]
                      (keyed [query channel target unsubscribe])))
              (listen channel (or (not-empty target) label))
              {:status 200
               :body response-channel}))
          {:status 400 :content-type "text/plain"
           :body value}))
      {:status 404 :content-type "text/plain"
       :body (format "Unrecognized query type %s\n" (name (or type "nil")))})))

(defn readable-queries
  "A view of the queries map which can be printed and reread."
  [queries]
  (into {}
        (for [[type queries] queries]
          [type
           (into {}
                 (for [[name query] queries]
                   [name (select-keys query [:query :target])]))])))

(defn get-queries
  "Produces a summary of all probe queries and the queries they're writing to."
  [config]
  {:status 200 :headers {"content-type" "application/json"}
   :body (str (formats/encode-json->string (for [[type queries] @(:queries config)
                                                 [name {:keys [query target]}] queries]
                                             (keyed [type name query target])))
              "\n")})

(defn save-queries
  "Saves the current queries to a file in the current directory."
  [config]
  (spit (:config-path config) (pr-str (readable-queries @(:queries config)))))

(defn restore-queries
  "Restores queries from the designated file."
  [config]
  (doseq [[type queries] (try
                             (read-string (slurp (:config-path config)))
                             (catch Exception e nil))
          [name {:keys [query target]}] queries]
    (add-query config type name target query nil)))

(defn wrap-saving-queries
  "Wraps a ring handler such that, if the handler succeeds, the current query set is saved before
  returning."
  [handler config]
  (if (:config-path config)
    (fn [request]
      (when-let [response (handler request)]
        (save-queries config)
        response))
    handler))

(defn wrap-debug [handler]
  (fn [req]
    (?! (handler (?! req)))))

(defn wrap-404 [handler]
  (fn [req]
    (or (handler req)
        {:status 404})))

(defn try-adding-html [handler]
  (fn [req]
    (or (handler req)
        (handler (update-in req [:uri]
                            #(-> %
                                 (s/replace #"/$" "")
                                 (str ".html")))))))

(defn module-routes
  "Constructs routes for delegating to installed modules.
   A module named foo will be in charge of handling any request under /foo.

   For example, /foo/bar will be sent to the foo module's :handler as a request for /bar."
  [{:keys [modules] :as config}]
  (apply routes
         (for [[module-name {:keys [handler]}] modules
               :when handler]
           (context (str "/" (name module-name)) []
                    handler))))

(defn get-targets [{:keys [modules] :as config}]
  (into {}
        (for [[module-name {:keys [targets]}] modules
              :when targets]
          [module-name (targets)])))

(defn render-targets [targets]
  {:status 200 :headers {"content-type" "application/json"}
   :body (str (formats/encode-json->string targets)
              "\n")})

(defn render-schema [config]
  {:status 200 :headers {"content-type" "application/json"}
   :body (str (formats/encode-json->string {:type (map name (keys (:modules config)))})
              "\n")})

(defn parse-json [address probe data]
  (try
    (formats/decode-json data)
    (catch Exception e
      (throw (IllegalArgumentException.
              (format "Probe %s from %s contained invalid data %s"
                      probe address data)
              e)))))

(defn process-event [topic data]
  (assert (string? topic))
  (trace/trace* topic (assoc data :topic topic)))

(defn tcp-handler
  "Forwards each message it receives into the trace router."
  [config]
  (let [clients (:clients config)]
    (fn [ch {:keys [address] :as client-info}]
      (let [client-id (gensym)
            log-event (let [inc (fnil inc 0)]
                        (fn [type]
                          (send clients update-in [address type] inc)))
            ch* (->> (doto ch
                       (lamina/on-closed (fn []
                                           (log-event :drop)
                                           (send clients update-in [:channels]
                                                 dissoc client-id)))

                       (lamina/on-error (fn [e] (log-event :error))))
                     (lamina/map* (fn [line]
                                    (let [[probe data] (s/split line #" " 2)]
                                      [(s/replace probe #"\." ":")
                                       (parse-json address probe data)]))))]
        (log-event :connect)
        (send clients update-in [:channels] assoc client-id ch)
        (-> ch*
            (lamina/receive-all
             (fn [[probe data]]
               (log-event :trace)
               (process-event probe data))))))))

(defn ring-handler
  "Builds a telemetry ring handler from a config map."
  [config]
  (let [writers (routes (POST "/add-query" [type name target query replay-since]
                          (add-query config
                                     (keyword type) name (not-empty target) query
                                     (when (seq replay-since)
                                       (Long/parseLong replay-since))))
                        (POST "/remove-query" [type name]
                          (remove-query config (keyword type) name)))
        readers (routes (ANY "/inspect" [query period]
                          (inspector config query
                                     (when (seq period)
                                       (lamina.query.struct/parse-time-interval period))))
                        (ANY "/queries" []
                          (get-queries config))
                        (ANY "/targets" []
                          (render-targets (get-targets config)))
                        (GET "/schema" []
                          (render-schema config)))]
    (-> (routes (wrap-saving-queries writers config)
                readers
                (module-routes config)
                (try-adding-html (resources "/")))
        wrap-keyword-params
        wrap-params
        wrap-json-params
        wrap-404)))

(defn wrap-default
  "Returns a function which calls f, replacing nil with default."
  [f default]
  (if f
    (fn [x]
      (or (f x) default))
    (constantly default)))

(defn init-modules [modules default-period]
  (into {} (for [{:keys [init options]} modules]
             (let [module (init options)]
               (when-not (:shutdown module)
                 (throw (Exception. (format "Module %s must provide a shutdown hook."
                                            (:name module)))))
               [(:name module) (-> module
                                   (update-in [:period] wrap-default default-period)
                                   (update-in [:subscription-filter] #(or % identity)))]))))

(defn init
  "Starts the telemetry http and tcp servers, and registers any modules given. Returns a server
  handle which can be terminated via destroy!. The handle is just a map with some useful data, but
  should be considered opaque: tamper at your own risk."
  [{:keys [modules http-port tcp-port period module-order] :or {period 1000} :as config}]
  (let [modules (init-modules modules period)
        config (doto (assoc config
                       :queries (ref {})
                       :clients (agent {})
                       :modules (into (ordering-map module-order)
                                      modules))
                 (restore-queries))
        tcp (tcp/start-tcp-server (tcp-handler config)
                                  ; we override the frame with utf-8 because gloss has some bad
                                  ; error-handling code that breaks in some cases if we let it split
                                  ; up by space for us. the "published" tcp-options are indeed the
                                  ; real format we support, we just have to do it by hand for now.
                                  (merge tcp-options {:port (or tcp-port default-tcp-port)
                                                      :frame (gloss/string :utf-8)}))
        handler (ring-handler config)
        http (http/start-http-server (http/wrap-ring-handler handler)
                                     {:port (or http-port default-http-port)})]
    {:shutdown (fn shutdown []
                 (tcp)
                 (http)
                 (doseq [[module queries] @(:queries config)
                         [name query] queries]
                   ((:unsubscribe query)))
                 (doseq [[module-name {:keys [shutdown]}] (:modules config)]
                   (shutdown)))
     :config config
     :handler handler}))

(defn destroy!
  "Given a server handle returned by init, shuts down all running servers and modules."
  [server]
  ((:shutdown server)))

(defn -main [& args]
  (let [period (if-let [[period] (seq args)]
                 (Long/parseLong period)
                 default-aggregation-period)
        read-schema #(io/reader "/opt/graphite/conf/storage-schemas.conf")]
    (def server (init {:period period, :config-path "config.clj"
                       :modules [{:init graphite/init
                                  :options {:host "localhost" :port 2003
                                            :config-reader read-schema
                                            :storage-path "/opt/graphite/storage/whisper"}}
                                 {:init phonograph/init
                                  :options {:base-path "./storage/phonograph"}}
                                 {:init cassette/init
                                  :options {:base-path "./storage/cassette"}}]
                       :module-order [:graphite :phonograph :cassette]}))))
