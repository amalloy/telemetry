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
   [flatland.useful.map :refer [update keyed map-vals]]
   [telemetry.module.carbon :as carbon])
  (:use flatland.useful.debug))

(def tcp-port 1845)
(def http-port 1846)

(def default-aggregation-period
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
           (trace/trace* probe data))))))

(defn subscribe [config query]
  (cache/subscribe trace/local-trace-router query
                   :period (or (:period config) default-aggregation-period)))

;;; functions that work on the listener list, and return Ring responses

(defn inspector
  "Inspect or debug a probe query without sending it to graphite, by returning an endless, streaming
   HTTP response of its values."
  [config query]
  {:status 200
   :headers {"content-type" "application/json"}
   :body (->> (subscribe config (formats/url-decode query))
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
  [config type name query]
  (if-let [connect (get-in config [:modules type :listen])]
    (do
      (remove-listener config type name)
      (let [channel (doto (subscribe config query)
                      (lamina/receive-all (fn [value]
                                            value))
                      (connect name))
            unsubscribe #(lamina/close channel)]
        (dosync
         (alter (:listeners config)
                assoc-in [type name]
                (keyed [query channel unsubscribe])))
        {:status 204})) ;; no content
    {:status 404 :body "Unrecognized listener type"}))

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
  "Produces a summary of all probe queries and the graphite names they're writing to."
  [config]
  {:status 200 :headers {"content-type" "application/json"}
   :body (formats/encode-json->string (readable-listeners @(:listeners config)))})

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

(defn init [{:keys [modules aggregation-period] :as config}]
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
                                      (inspector config query))
                                    (GET "/listeners" []
                                      (get-listeners config)))]
                (-> (routes (wrap-saving-listeners writers config)
                            readers)
                    wrap-keyword-params
                    wrap-params
                    http/wrap-ring-handler))
              {:port http-port})]

    {:shutdown (fn []
                 (tcp)
                 (http)
                 (doseq [[name {:keys [shutdown]}] (:modules config)]
                   (shutdown)))
     :config config}))

(defn destroy [server]
  ((:shutdown server)))

(defn -main [& args]
  (let [period (if-let [[period] (seq args)]
                 (Long/parseLong period)
                 default-aggregation-period)]
    (def server (init {:period period, :config-path "config.clj"
                       :modules [{:init carbon/init
                                  :options {:host "localhost" :port 2003}}]}))))
