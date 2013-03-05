(defproject org.flatland/telemetry "0.1.3-alpha1"
  :description "data from a distance"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [aleph "0.3.0-beta14"]
                 [compojure "1.1.1"]
                 [swank-clojure "1.4.2"]
                 [org.flatland/useful "0.9.4"]
                 [org.flatland/phonograph "0.1.2"]
                 [ring-middleware-format "0.2.4" :exclusions [ring]]
                 [org.flatland/cassette "0.1.1"]]
  :main flatland.telemetry
  :uberjar-name "telemetry.jar")
