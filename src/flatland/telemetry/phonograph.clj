(ns flatland.telemetry.phonograph
  (:require [flatland.telemetry.graphite.config :as config]
            [flatland.telemetry.graphing :as graphing]
            [flatland.phonograph :as phonograph]
            [lamina.core :as lamina]
            [clojure.string :as s])
  (:import java.io.File))

(defn memoize*
  "Fills its memoization cache with thunks instead of actual values, so that there is no possibility
  of calling f multiple times concurrently. Also exposes its memozation cache in the returned
  function's metadata, to permit outside fiddling."
  [f]
  (let [cache (atom {})]
    (-> (fn [& args]
          (let [thunk (delay (apply f args))]
            (-> cache
                (swap! (fn [cache]
                         (assoc cache args
                                (or (get cache args) thunk))))
                (get args)
                (deref))))
        (with-meta {:cache cache}))))

(defn retention->archive [{:keys [granularity duration]}]
  (let [granularity (config/as-seconds granularity)
        duration (config/as-seconds duration)]
    {:density granularity :count (quot duration granularity)}))

(defn phonograph-opener [{:keys [base-path db-opts archive-retentions]}]
  (let [base-file (File. base-path)]
    (memoize* (fn [label]
                (let [full-path (.getPath (reduce #(File. %1 %2)
                                                  base-file
                                                  (s/split #"\." label)))]
                  (or (apply phonograph/create full-path
                             (db-opts label)
                             (map retention->archive (archive-retentions label)))
                      (phonograph/open full-path)))))))

(defn init
  "Supported config options:
   - base-path: the path under which to store all the phonograph files.
   - db-opts: a function taking in a label and returning options to open its phonograph file
   - archive-retentions: a function taking in a label and returning a sequence of storage specs.
     each storage spec should have a :granularity and a :duration, in time units as supported by
     flatland.telemetry.graphite.config/unit-multipliers."
  [{:keys [base-path db-opts archive-retentions] :as config}]
  (let [open (phonograph-opener config)
        nexus (lamina/channel* :permanent? true :grounded? true)]
    (lamina/receive-all nexus
                        (fn [[label value time]]
                          (phonograph/append! (open label) [value time])))
    {:name :phonograph
     :shutdown (fn shutdown []
                 (lamina/close nexus)
                 (lamina/run-pipeline @(lamina/drained-result nexus)
                                      (fn [_]
                                        (doseq [[path file] @(:cache (meta open))]
                                          (phonograph/close @file))
                                        (reset! (:cache (meta open)) {}))))
     :listen (fn listen [ch name]
               (-> ch
                   (->> (lamina/mapcat* (graphing/sink name)))
                   (lamina/siphon nexus)))
     :period (fn [label]
               (when-let [granularity (:granularity (first (archive-retentions label)))]
                 (* 1000 (config/as-seconds granularity))))}))
