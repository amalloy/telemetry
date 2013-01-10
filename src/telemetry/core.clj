(ns telemetry.core
  (:use [ring.middleware params keyword-params])
  (:require
   [clojure.string :as str]
   (lamina [core :as lamina]
           [connections :as connection]
           trace)
   (gloss [core :as gloss])
   (aleph [trace :as trace]
          [formats :as formats]
          [http :as http]
          [tcp :as tcp]))
  (:import java.util.Date))

(def trace-port 8000)
(def tcp-port 8001)
(def http-port 8002)

(defonce trace-router
  (trace/start-trace-router {:port trace-port}))

(defonce endpoint
  (trace/trace-endpoint {:client-options {:host "localhost" :port trace-port}}))

(defn input-handler [ch _]
  (lamina/receive-all ch
    (fn [[probe data]]
      (let [probe (str/replace probe #"\." ":")]
        (lamina.trace/trace* probe
          (formats/decode-json data))))))

(defn consumption-handler [req]
  (let [{:keys [q]} (:params req)]
    {:status 200
     :headers {"content-type" "application/json"}
     :body (->> (trace/subscribe endpoint (formats/url-decode q))
                :messages
                (lamina/map* formats/encode-json->string)
                (lamina/map* #(str % "\n")))}))

(defn graphite-channel [host port]
  (tcp/tcp-client {:host host :port port
                   :frame [(gloss/string :utf-8 :delimiters [" "]) ;; message type
                           (gloss/string-integer :utf-8 :delimiters [" "]) ;; count
                           (gloss/string-integer :utf-8 :delimiters ["\n"])]})) ;; timestamp

(defn outgoing-channel-generator [host port]
  (connection/persistent-connection
   (fn []
     (doto @(graphite-channel host port)
       (lamina/ground))))) ;; we will only ever look at the write end of the channel

(declare outgoing-channel)

(defn unix-time [^Date date]
  (-> date (.getTime) (quot 1000)))

(defn add-listener [query name]
  (doto (trace/subscribe endpoint query)
    (-> (:messages)
        (->> (lamina/map* (fn [data]
                            [name data (unix-time (Date.))])))
        (lamina/siphon @(outgoing-channel)))))

(defn init
  ([] (init "localhost" 2003))
  ([graphite-host graphite-port]

     (def outgoing-channel (outgoing-channel-generator graphite-host graphite-port))

     (def tcp-server
       (tcp/start-tcp-server
        input-handler
        {:port tcp-port
         :delimiters ["\r\n" "\n"]
         :frame [(gloss/string :utf-8 :delimiters [" "])
                 (gloss/string :utf-8)]}))

     (def http-server
       (http/start-http-server

        (-> consumption-handler
            wrap-keyword-params
            wrap-params
            http/wrap-ring-handler)

        {:port http-port}))))

(defn stop []
  (tcp-server)
  (http-server))

(defn -main [& args]
  (init))
