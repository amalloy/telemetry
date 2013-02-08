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

(defn regex-search [s tests]
  (first (for [[pattern value] tests
               :when (re-find pattern s)]
           value)))

(defn retention->archive [{:keys [granularity duration]}]
  (let [granularity (config/as-seconds granularity)
        duration (config/as-seconds duration)]
    {:density granularity :count (quot duration granularity)}))

(defn phonograph-opener [{:keys [base-path db-opts archive-retentions] :as config}]
  (let [base-file (File. base-path)]
    (memoize* (fn [label]
                (let [path-segments (vec (s/split label #"\."))
                      path-segments (conj (pop path-segments)
                                          (str (peek path-segments) ".pgr"))

                      full-path (reduce #(File. %1 %2)
                                        base-file
                                        path-segments)]
                  (.mkdirs (.getParentFile full-path))
                  (or (apply phonograph/create full-path
                             (db-opts label)
                             (map retention->archive (archive-retentions label)))
                      (phonograph/open full-path)))))))

(let [storage-modes [[#"\.(count|rate)$" :sum]
                     [#"\.max$" :max]
                     [#"\.min$" :min]
                     [#".*" :average]]]
  (defn default-db-opts [label]
    {:aggregation (regex-search label storage-modes)}))


(defn regex-archiver [tests]
  (fn [label]
    (regex-search label tests)))

(def default-archive-retentions
  (regex-archiver [[#".*" [{:granularity {:number 30, :unit :seconds}
                             :duration {:number 90, :unit :days}}]]]))

(let [default-config {:db-opts default-db-opts
                      :archive-retentions default-archive-retentions}]
  (defn init
    "Supported config options:
   - base-path: the path under which to store all the phonograph files.
   - db-opts: a function taking in a label and returning options to open its phonograph file
   - archive-retentions: a function taking in a label and returning a sequence of storage specs.
     each storage spec should have a :granularity and a :duration, in time units as supported by
     flatland.telemetry.graphite.config/unit-multipliers."
    [config]
    (let [{:keys [archive-retentions] :as config} (merge default-config config)
          open (phonograph-opener config)
          nexus (lamina/channel* :permanent? true :grounded? true)]
      (lamina/receive-all nexus
                          (fn [[label value time]]
                            (phonograph/append! (open label) [time value])))
      {:name :phonograph
       :shutdown (fn shutdown []
                   (doseq [[path file] @(:cache (meta open))]
                     (phonograph/close @file))
                   (reset! (:cache (meta open)) {}))
       :listen (fn listen [ch name]
                 (-> ch
                     (->> (lamina/mapcat* (graphing/sink name)))
                     (lamina/siphon nexus)))
       :period (fn [label]
                 (when-let [granularity (:granularity (first ((:archive-retentions config) label)))]
                   (* 1000 (config/as-seconds granularity))))
       :debug {:config config, :nexus nexus, :open open, :cache (meta open)}})))
