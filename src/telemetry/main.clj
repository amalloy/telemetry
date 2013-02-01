(ns telemetry.main
  (:gen-class))

(defn -main [& args]
  (require 'telemetry.server)
  (apply (resolve 'telemetry.server/-main) args))
