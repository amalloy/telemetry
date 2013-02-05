(defproject org.flatland/telemetry "0.1.2-SNAPSHOT"
  :description "data from a distance"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [aleph "0.3.0-SNAPSHOT"]
                 [lamina "0.5.0-SNAPSHOT"]
                 [compojure "1.1.1"]
                 [swank-clojure "1.4.2"]
                 [io.netty/netty "3.5.9.Final"]
                 [org.flatland/useful "0.9.0"]]
  :main telemetry.main
  :uberjar-name "telemetry.jar")
