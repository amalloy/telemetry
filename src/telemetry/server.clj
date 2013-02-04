(ns telemetry.server
  (:use [ring.middleware params keyword-params])
  (:require
   [clojure.string :as str]
   [clojure.java.io :as io]
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
   [compojure.core :refer [routes GET POST context]]
   [flatland.useful.utils :refer [returning]]
   [flatland.useful.map :refer [update keyed map-vals]]
   [telemetry.module.carbon :as carbon])
  (:import (java.io StringReader BufferedReader IOException))
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
  (cache/subscribe trace/local-trace-router query
                   :period period))

;;; functions that work on the listener list, and return Ring responses

(defn inspector
  "Inspect or debug a probe query without sending it to any listeners, by returning an endless,
   streaming HTTP response of its values."
  [config query]
  {:status 200
   :headers {"content-type" "application/json"}
   :body (->> (subscribe (formats/url-decode query) (:period config))
              (lamina/map* formats/encode-json->string)
              (lamina/map* #(str % "\n")))})

(defn remove-listener
  "Disconnects any channel writing to the given name/pattern."
  [config type name]
  (let [listeners (:listeners config)]
    (if-let [{:keys [unsubscribe query]} (dosync (returning (get-in @listeners [type name])
                                                   (alter listeners update type dissoc name)))]
      (do (unsubscribe)
          {:status 200 :headers {"content-type" "application/json"}
           :body (formats/encode-json->string {:query query})})
      {:status 404})))

(defn add-listener
  "Connects a channel from the queried probe descriptor to a graphite sink for the given name or
  pattern. Implicitly disconnects any existing writer from that sink first."
  [config type label query]
  (let [{:keys [listen period]} (get-in config [:modules type])]
    (if listen
      (do
        (remove-listener config type label)
        (let [channel (doto (subscribe query (period label))
                        (listen label))
              unsubscribe #(lamina/close channel)]
          (dosync
           (alter (:listeners config)
                  assoc-in [type label]
                  (keyed [query channel unsubscribe])))
          {:status 204})) ;; no content
      {:status 404 :body (format "Unrecognized listener type %s\n" (name (or type "nil")))})))

(defn readable-listeners
  "A view of the listeners map which can be printed and reread."
  [listeners]
  (into {}
        (for [[type listeners] listeners]
          [type
           (into {}
                 (for [[name listener] listeners]
                   [name (:query listener)]))])))

(defn get-listeners
  "Produces a summary of all probe queries and the listeners they're writing to."
  [config]
  {:status 200 :headers {"content-type" "application/json"}
   :body (str (formats/encode-json->string (readable-listeners @(:listeners config)))
              "\n")})

(defn save-listeners
  "Saves the current listeners to a file in the current directory."
  [config]
  (spit (:config-path config) (pr-str (readable-listeners @(:listeners config)))))

(defn restore-listeners
  "Restores listeners from the designated file."
  [config]
  (doseq [[type listeners] (try
                             (read-string (slurp (:config-path config)))
                             (catch Exception e nil))
          [name query] listeners]
    (add-listener config type name query)))

(defn wrap-saving-listeners
  "Wraps a ring handler such that, if the handler succeeds, the current listener set is saved before
  returning."
  [handler config]
  (if (:config-path config)
    (fn [request]
      (when-let [response (handler request)]
        (save-listeners config)
        response))
    handler))

(defn wrap-debug [handler]
  (fn [req]
    (?! (handler (?! req)))))

(defn wrap-404 [handler]
  (fn [req]
    (or (handler req)
        {:status 404})))

(defn module-routes
  "Constructs routes for delegating to installed modules.
   A module named foo will be in charge of handling any request under /foo.

   For example, /foo/bar will be sent to the foo module :hander as a request for /bar."
  [{:keys [modules] :as config}]
  (apply routes
         (for [[module-name {:keys [handler]}] modules
               :when handler]
           (context (str "/" (name module-name)) []
                    handler))))

(defn tcp-handler
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
           (trace/trace* probe data))))))

(defn ring-handler
  "Builds a telemetry ring handler from a config map."
  [config]
  (let [writers (routes (POST "/listen" [type name query]
                          (add-listener config (keyword type) name query))
                        (POST "/forget" [type name]
                          (remove-listener config (keyword type) name)))
        readers (routes (GET "/inspect" [query]
                          (inspector config query))
                        (GET "/listeners" []
                          (get-listeners config)))]
    (-> (routes (wrap-saving-listeners writers config)
                readers
                (module-routes config))
        wrap-keyword-params
        wrap-params
        wrap-404)))

(defn wrap-default
  "Returns a function which calls f, replacing nil with default."
  [f default]
  (if f
    (fn [x]
      (or (f x) default))
    (constantly default)))

(defn init
  "Starts the telemetry http and tcp servers, and registers any modules given. Returns a server
  handle which can be terminated via destroy!. The handle is just a map with some useful data, but
  should be considered opaque: tamper at your own risk."
  [{:keys [modules http-port tcp-port period] :or {period 1000} :as config}]
  (let [modules (into {} (for [{:keys [init options]} modules]
                           (let [module (init options)]
                             (when-not (:shutdown module)
                               (throw (Exception. (format "Module %s must provide a shutdown hook."
                                                          (:name module)))))
                             [(:name module) (update-in module [:period]
                                                        wrap-default period)])))
        config (doto (assoc config
                       :listeners (ref {})
                       :modules modules)
                 (restore-listeners))
        tcp (tcp/start-tcp-server tcp-handler
                                  (merge tcp-options {:port (or tcp-port default-tcp-port)}))
        handler (ring-handler config)
        http (http/start-http-server (http/wrap-ring-handler handler)
                                     {:port (or http-port default-http-port)})]
    {:shutdown (fn shutdown []
                 (tcp)
                 (http)
                 (doseq [[module-name {:keys [shutdown]}] (:modules config)]
                   (shutdown)))
     :config config
     :handler handler}))

(defn destroy!
  "Given a server handle returned by init, shuts down all running servers and modules."
  [server]
  ((:shutdown server)))

(defn -main
  "Starts up a basic telemetry server with all the default settings and a carbon module."
  [& args]
  (let [period (if-let [[period] (seq args)]
                 (Long/parseLong period)
                 default-aggregation-period)]
    (let [host "localhost" port 4005]
      (printf "Starting swank on %s:%d\n" host port)
      (swank/start-server :host host :port port))
    (def server (init {:period period, :config-path "config.clj"
                       :modules [{:init carbon/init
                                  :options {:host "localhost" :port 2003
                                            :config-reader #(try
                                                              (io/reader "/opt/graphite/conf/storage-schemas.conf")
                                                              (catch IOException e
                                                                (BufferedReader. (StringReader. ""))))}}]}))))
