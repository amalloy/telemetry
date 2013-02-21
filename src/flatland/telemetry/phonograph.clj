(ns flatland.telemetry.phonograph
  (:require [flatland.telemetry.graphite.config :as config]
            [flatland.telemetry.graphing :as graphing :refer [unix-time]]
            [flatland.phonograph :as phonograph]
            [flatland.useful.utils :refer [with-adjustments]]
            [aleph.formats :as formats]
            [lamina.core :as lamina]
            [lamina.trace :as trace]
            lamina.time
            [clojure.string :as s]
            [compojure.core :refer [GET]])
  (:import java.io.File
           (java.util Date Calendar)))

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
                (force))))
        (with-meta {:cache cache}))))

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
  (regex-archiver [[#".*" [(every 30 seconds for 90 days)]]]))

(comment want to upgrade that to the following, but we can't yet upgrade phonograph files.
         [(every 30 seconds for a day)
          (every 15 minutes for 14 days)
          (every 6 hours for 12 weeks)
          (every day for 10 years)])

(defn subtract-day [^Date d]
  (.getTime (doto (Calendar/getInstance)
              (.setTime d)
              (.add Calendar/DATE -1))))

(defn timed-tuple [timestamp value]
  [timestamp value])

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

(defn points [open targets from until]
  (let [q (lamina.time/non-realtime-task-queue)
        router (trace/trace-router
                {:generator (fn [{:strs [pattern]}]
                              (apply lamina/closed-channel
                                     (phonograph-seq open (s/replace pattern ":" ".") from until)))
                 :task-queue q, :payload tuple-value :timestamp tuple-time})
        subscriptions (doall (for [target targets]
                               (lamina/map* (fn [data]
                                              [data (lamina.time/now q)])
                                            (trace/subscribe router target {}))))]
    (lamina.time/advance-until q until)
    (map (fn [target-name channel]
           {:target target-name
            :datapoints (lamina/channel->seq channel)})
         targets, subscriptions)))

(defn absolute-time [t ref]
  (if (neg? t)
    (+ ref t)
    t))

(defn handler [open]
  (GET "/render" [target from until]
    (let [targets (if (coll? target) ; if there's only one target it's a string, but if multiple are
                    target           ; specified then compojure will make a list of them
                    [target])
          now-date (Date.)
          unix-now (unix-time now-date)]
      (with-adjustments #(when (seq %) (Long/parseLong %)) [from until]
        (let [until (if until
                      (absolute-time until unix-now)
                      unix-now)
              from (if from
                     (absolute-time from until)
                     (unix-time (subtract-day now-date)))]
          (if-let [result (points open targets from until)]
            {:status 200
             :headers {"Content-Type" "application/json"}
             :body (formats/encode-json->string result)}
            {:status 404}))))))

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
       :handler (handler open)
       :listen (fn listen [ch name]
                 (-> ch
                     (->> (lamina/mapcat* (graphing/sink name)))
                     (lamina/siphon nexus)))
       :period (fn [label]
                 (when-let [granularity (:granularity (first ((:archive-retentions config) label)))]
                   (* 1000 (config/as-seconds granularity))))
       :debug {:config config, :nexus nexus, :open open, :cache (meta open)}})))
