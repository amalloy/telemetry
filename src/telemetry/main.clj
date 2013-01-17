(ns telemetry.main
  (:gen-class))

(defn -main [& args]
  (require 'telemetry.core)
  (apply (resolve 'telemetry.core/-main) args))
