(ns flatland.telemetry.graphite.config)

;; graphite doesn't support weeks, so even though we do we won't include it in the parsing code
(def unit-abbrevs
  {"s" :seconds, "m" :minutes, "h" :hours, "d" :days, "y" :years})

;; allow keywords to be plural, optionally
(def unit-multipliers
  (into {} (for [[k v] {:second 1 :minute 60 :hour (* 60 60) :day (* 60 60 24)
                        :week (* 60 60 24 7) :year (* 60 60 24 365)}
                 k [k (keyword (str (name k) "s"))]]
             [k v])))
(defn as-seconds [time-with-unit]
  (* (:number time-with-unit) (unit-multipliers (:unit time-with-unit))))

(defn parse-time [s]
  (let [[number unit] (rest (re-find #"(\d+)(\D?)" s))]
    {:number (Long/parseLong number)
     :unit (get unit-abbrevs unit :seconds)}))

(defn parse-retention [[granularity duration]]
  {:granularity (parse-time granularity)
   :duration (parse-time duration)})

(defn parse-line [key-name s]
  (second (->> s (re-find (re-pattern (str "\\s*" key-name "\\s*=\\s*(.*)"))))))

(defn parse-rule [[pattern-definition retentions]]
  {:pattern (re-pattern (parse-line "pattern" pattern-definition))
   :retentions (let [rules (parse-line "retentions" retentions)]
                 (map (comp parse-retention rest)
                      (re-seq #"([^:]+):([^,]+)(?:\s*,\s*)?" rules)))})

(letfn [(empty-line? [line]
          (contains? #{nil "#" "["}
                     (re-find #"\S" line)))]
  (defn parse-carbon-config [lines]
    (map parse-rule (partition 2 (remove empty-line? lines)))))
