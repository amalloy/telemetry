(ns telemetry.core
  (:use
    [ring.middleware params keyword-params]
    [lamina core trace]
    [gloss core])
  (:require
    [clojure.string :as str]
    [aleph
     [trace :as trace]
     [formats :as formats]
     [http :as http]
     [tcp :as tcp]]))

(def trace-port 8000)
(def tcp-port 8001)
(def http-port 8002)

(defonce trace-router
  (trace/start-trace-router {:port trace-port}))

(defonce endpoint
  (trace/trace-endpoint {:client-options {:host "localhost" :port trace-port}}))

(defn input-handler [ch _]
  (receive-all ch
    (fn [[probe data]]
      (let [probe (str/replace probe #"\." ":")]
        (trace* probe
          (formats/decode-json data))))))

(defn consumption-handler [req]
  (let [{:keys [q]} (:params req)]
    (prn req q)
    {:status 200
     :headers {"content-type" "application/json"}
     :body (->> (trace/subscribe endpoint (formats/url-decode q))
             :messages
             (map* formats/encode-json->string)
             (map* #(str % "\n")))}))

(defn init []
  
  (def tcp-server
    (tcp/start-tcp-server
      input-handler
      {:port tcp-port
       :delimiters ["\r\n" "\n"]
       :frame [(string :utf-8 :delimiters [" "]) (string :utf-8)]}))

  (def http-server
    (http/start-http-server

      (-> consumption-handler
        wrap-keyword-params
        wrap-params
        http/wrap-ring-handler)

      {:port http-port})))

(defn stop []
  (tcp-server)
  (http-server))

(init)
