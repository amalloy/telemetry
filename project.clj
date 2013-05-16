(defproject org.flatland/telemetry "0.2.0-SNAPSHOT"
  :description "Data from a distance."
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.4.0"]
                 [aleph "0.3.0-rc1"]
                 [lamina "0.5.0-rc2"]
                 [compojure "1.1.1"]
                 [me.raynes/fs "1.4.2"]
                 [cheshire "5.1.1"]
                 [org.flatland/useful "0.9.4"]
                 [org.flatland/phonograph "0.1.4"]
                 [ring-middleware-format "0.2.4" :exclusions [ring]]
                 [org.flatland/cassette "0.2.5"]]
  :classifiers {:admin {:dependencies []
                        :omit-source true
                        :compile-path "target/empty"}}
  :main flatland.telemetry)
