(defproject org.flatland/telemetry "0.3.1"
  :description "Data from a distance."
  :url "http://github.com/flatland/telemetry"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [aleph "0.3.0"]
                 [lamina "0.5.0"]
                 [compojure "1.1.1"]
                 [me.raynes/fs "1.4.2"]
                 [lib-noir "0.5.5"]
                 [cheshire "5.1.1"]
                 [congomongo "0.4.1"]
                 [ring-middleware-format "0.2.4" :exclusions [ring]]
                 [com.draines/postal "1.9.2"]
                 [org.flatland/useful "0.10.3"]
                 [org.flatland/phonograph "0.1.4"]
                 [org.flatland/laminate "1.5.2"]
                 [org.flatland/cassette "0.2.5"]
                 [org.flatland/telemetry-ui "0.2.4"]]
  :exclusions [lamina]
  :main flatland.telemetry)
