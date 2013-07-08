(defproject org.flatland/telemetry "0.2.3-beta6"
  :description "Data from a distance."
  :url "http://github.com/flatland/telemetry"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [aleph "0.3.0-rc1"]
                 [org.flatland/lamina "0.5.0-rc4.1"]
                 [org.flatland/laminate "1.3.2"]
                 [compojure "1.1.1"]
                 [me.raynes/fs "1.4.2"]
                 [lib-noir "0.5.5"]
                 [cheshire "5.1.1"]
                 [congomongo "0.4.1"]
                 [com.draines/postal "1.9.2"]
                 [org.flatland/useful "0.10.2"]
                 [org.flatland/phonograph "0.1.4"]
                 [ring-middleware-format "0.2.4" :exclusions [ring]]
                 [org.flatland/cassette "0.2.5"]
                 [org.flatland/telemetry-ui "0.2.3-beta2"]]
  :exclusions [lamina]
  :main flatland.telemetry)
