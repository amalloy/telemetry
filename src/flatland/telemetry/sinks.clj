(ns flatland.telemetry.sinks
  (:require [clojure.string :as str]
            [lamina.time :as t]
            [lamina.core :as lamina]
            [lamina.query.operators :as q]
            [lamina.query.core :refer [def-query-operator]])
  (:import java.util.Date))

(defn timed-value [time value]
  {::time time, ::value value})

(defn is-timed-value? [x]
  (and (map? x)
       (every? #(contains? x %) [::time ::value])))

(def get-value ::value)
(def get-time ::time)

(defn timed-values-op [time values {:keys [task-queue]
                                  :or {task-queue (t/task-queue)}}
                      ch]
  (lamina/map* (fn [x]
                 (timed-value (time x) (values x)))
               ch))

(def-query-operator timed-values
  :periodic? false
  :distribute? true
  :transform (fn [{:keys [options]} ch]
               (let [{time 0, values 1 :or {time :time, values :values}} options]
                 (timed-values-op (q/getter time) (q/getter values)
                                  (dissoc options 0 1)
                                  ch))))

(defn adjust-time [name value real-timestamp]
  (cond (is-timed-value? value)
        ,,[[name (get-values value) (get-time value)]]
        (and (sequential? value)
             (is-timed-value? (first value)))
        ,,(for [v value]
            [name (get-values v) (get-time v)])
        :else [[name value real-timestamp]]))

;;; functions for interpolating values into patterns

(defn rename-multiple
  "Takes a pattern with N wildcards like foo.*1.blah.*2 and a list of N values, and replaces each
   wildcard with the matching value from the list."
  [pattern keys]
  (reduce (fn [name [attr i]]
            (str/replace name (str "*" i)
                         (str attr)))
          pattern
          (map vector keys (iterate inc 1))))

(defn rename-one
  "Replaces all *s in the pattern with the value of key."
  [pattern key]
  (str/replace pattern #"\*" key))

;;; lamina channel transformers, to turn values from a probe descriptor into a sequence of tuples
;;; suitable for encoding and sending out to the graphite server or phonograph db.

(defn timed-sink
  "Returns a function which emits each datum decorated with the given label and the current time."
  [name]
  (fn [{:keys [timestamp value]}]
    (adjust-time name value timestamp)))

(defn sink-by-name
  "Returns a function which expects to receive a map of labels to values. Each label has its
   value(s) interpolated into the pattern, and a list of [name value time] tuples is returned."
  [pattern rename-fn]
  (fn [{:keys [timestamp value]}]
    (for [[k v] value
          adjusted (adjust-time (rename-fn pattern (or k "nil"))
                                v timestamp)]
      adjusted)))

(defn sink
  "Determines what kind of pattern name is, and creates an appropriate transformer for its channel.
   name may be:
   - an ordinary name (in which case its values are emitted unchanged),
   - a pattern containing the * wildcard (in which case it is assumed to be grouped-by one field),
   - a pattern with *1, *2 wildcards (in which case it is assumed to be grouped-by a tuple)."
  [name]
  (if (re-find #"\*\d" name)
    (let [name (str/replace name #"\*(?!\d)" "*1")]
      (sink-by-name name rename-multiple))
    (if (re-find #"\*" name)
      (sink-by-name name rename-one)
      (timed-sink name))))
