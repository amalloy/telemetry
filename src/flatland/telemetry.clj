(ns flatland.telemetry
  (:gen-class))

(defn -main
  "Starts up a basic telemetry server with all the default settings and a carbon module."
  [& args]
  (require '[flatland.telemetry.server :as server])
  (require '[flatland.telemetry.graphite :as graphite])
  (let [period (if-let [[period] (seq args)]
                 (Long/parseLong period)
                 default-aggregation-period)
        read-schema #(try (io/reader "/opt/graphite/conf/storage-schemas.conf")
                          (catch IOException e
                            (BufferedReader. (StringReader. ""))))]
    (let [host "localhost" port 4005]
      (printf "Starting swank on %s:%d\n" host port)
      (swank/start-server :host host :port port))
    (def server (init {:period period, :config-path "config.clj"
                       :modules [{:init graphite/init
                                  :options {:host "localhost" :port 2003
                                            :config-reader read-schema}}]}))))
