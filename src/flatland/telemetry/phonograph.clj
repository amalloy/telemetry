(ns flatland.telemetry.phonograph
  (:require [flatland.telemetry.graphite.config :as config]
            [flatland.telemetry.sinks :as sinks]
            [flatland.telemetry.util :as util :refer [memoize* render-handler]]
            [flatland.phonograph :as phonograph]
            [lamina.core :as lamina]
            [clojure.string :as s])
  (:import java.io.File
           java.util.regex.Pattern))

(defn regex-search [s tests]
  (first (for [[pattern value] tests
               :when (re-find pattern s)]
           value)))

(defn retention->archive [{:keys [granularity duration]}]
  (let [granularity (config/as-seconds granularity)
        duration (config/as-seconds duration)]
    {:density granularity :count (quot duration granularity)}))


;; TODO: better caching strategy than "cache files forever".
;;
;; In the short term, we want to just close a file as soon as we're done writing to it; this only
;; requires making phonograph files "reopenable" without having to re-parse the archive headers.
;;
;; In the long term it might be nice to keep the most-active files actually open, especially if we
;; switch to writing at shorter intervals than thirty seconds. I asked about this at
;; http://stackoverflow.com/q/14842457/625403, and got a good answer that addresses almost all of my
;; concerns. I think we could address my last concern by also creating a phantom reference to each
;; handle when we add it to the cache, so that we can clean up the object only once any outstanding
;; references to it are cleared.
(defn phonograph-opener [{:keys [^String base-path db-opts archive-retentions] :as config}]
  (let [base-file (File. base-path)]
    (memoize* (fn [label]
                (let [path-segments (vec (s/split label #":"))
                      path-segments (conj (pop path-segments)
                                          (str (peek path-segments) ".pgr"))

                      ^File full-path (reduce #(File. ^File %1 ^String %2)
                                              base-file
                                              path-segments)]
                  (.mkdirs (.getParentFile full-path))
                  (try
                    (or (apply phonograph/create full-path
                               (db-opts label)
                               (map retention->archive (archive-retentions label)))
                        (phonograph/open full-path))
                    (catch Exception e
                      (throw (java.io.IOException. (format "Error opening %s" full-path) e)))))))))

(let [storage-modes [[#":(mean|avg|average)$" :average]
                     [#":max$" :max]
                     [#":min$" :min]
                     [#".*" :sum]]]
  (defn default-db-opts [label]
    {:aggregation (regex-search label storage-modes)}))

(defn find-globbed [^String base-path target]
  (let [quoted-path (re-pattern (str (Pattern/quote base-path) "/*"))
        regex (re-pattern (str quoted-path
                               (-> target
                                   (s/replace #"\*\d?" ".*")
                                   (s/replace ":" "/"))
                               ".pgr"))]
    (for [^File f (file-seq (File. base-path))
          :when (and (not (.isDirectory f))
                     (re-matches regex (.getPath f)))]
      [(-> (.getPath f)
           (s/replace quoted-path "")
           (s/replace "/" ":")
           (s/replace ".pgr" ""))
       f])))

(defn regex-archiver [tests]
  (fn [label]
    (regex-search label tests)))

(defn time-span [[granularity-num granularity-unit]
                 [duration-num duration-unit]]
  {:granularity {:number granularity-num, :unit granularity-unit}
   :duration {:number duration-num, :unit duration-unit}})

(defmacro every
  "Accepts an input like (every 30 seconds for a day) or (every minute for 2 weeks) and turns it
into the time-unit representation that telemetry uses."
  ([granularity-unit for duration-num duration-unit]
     `(every 1 ~granularity-unit ~for ~duration-num ~duration-unit))
  ([granularity-num granularity-unit
    for
    duration-num duration-unit]
     (let [gunit (keyword granularity-unit)
           dunit (keyword duration-unit)
           duration-num ('{a 1, an 1} duration-num duration-num)]
       (if-let [error (cond (not= for 'for) (format "You must call your separator 'for, not '%s."
                                                    (pr-str for))
                            (not (config/unit-multipliers gunit)) (format "Unrecognized unit '%s"
                                                                          granularity-unit)
                            (not (config/unit-multipliers dunit)) (format "Unrecognized unit '%s"
                                                                          duration-unit))]

         (throw (IllegalArgumentException. error))
         `(time-span [~granularity-num ~gunit] [~duration-num ~dunit])))))

(def default-archive-retentions
  (regex-archiver [[#".*" [(every 30 seconds for a day)
                           (every 15 minutes for 14 days)
                           (every 6 hours for 12 weeks)
                           (every day for 10 years)]]]))

(defn timed-tuple [timestamp value]
  [(* 1000 timestamp) value])

(defn tuple-time [tuple]
  (nth tuple 0))

(defn tuple-value [tuple]
  (nth tuple 1))

(defn phonograph-seq [open target from until]
  (let [{:keys [from until density values]}
        ,,(phonograph/get-range (open target) from until)]
    (filter tuple-value (map timed-tuple
                             (range from until density)
                             values))))

(defn handler [open]
  (render-handler (fn [from until]
                    (fn [pattern]
                      (phonograph-seq open pattern from until)))
                  {:timestamp tuple-time :payload tuple-value}))

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
    (let [{:keys [archive-retentions base-path] :as config} (merge default-config config)
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
       :handler (handler open)
       :listen (fn listen [ch name]
                 (-> ch
                     (->> (lamina/mapcat* (sinks/sink name)))
                     (lamina/siphon nexus)))
       :clear (fn [label]
                (let [matches (find-globbed base-path label)]
                  (doseq [^File f (map second matches)]
                    (.delete f))
                  (apply swap! (:cache (meta open))
                         dissoc (for [[target f] matches]
                                  (list target)))))
       :period (fn [label]
                 (when-let [granularity (:granularity (first ((:archive-retentions config) label)))]
                   (* 1000 (config/as-seconds granularity))))
       :targets (when base-path
                  (fn []
                    (util/target-names (util/path->targets base-path ".pgr"))))
       :debug {:config config, :nexus nexus, :open open, :cache (:cache (meta open))}})))
