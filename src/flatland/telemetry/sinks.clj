(ns flatland.telemetry.sinks
  (:require [clojure.string :as str]
            [flatland.laminate :as laminate]
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

(defn timed-op [time value {:keys [task-queue]
                            :or {task-queue (t/task-queue)}}
                ch]
  (lamina/map* (fn [x]
                 (timed-value (or (time x) (quot (t/now task-queue) 1000)) (value x)))
               ch))

(def default-time-operators
  {:operators [{:name "lookup", :options {0 :time}}]})

(def-query-operator timed
  :periodic? false
  :distribute? true
  :transform (fn [{:keys [options]} ch]
               (let [{time 0, value 1 :or {time default-time-operators}}
                     options]
                 (timed-op (q/getter time) (if value
                                             (q/getter value)
                                             (partial laminate/dissoc-lookup (:operators time)))
                           (dissoc options 0 1)
                           ch))))

(defn adjust-time [name value real-timestamp]
  (if (is-timed-value? value)
    [name (get-value value) (get-time value)]
    [name value real-timestamp]))

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
    [(adjust-time name value timestamp)]))

(defn sink-by-name
  "Returns a function which expects to receive a map of facets to values. Each facet is
   interpolated into the pattern, and a list of [topic value time] tuples is returned."
  [pattern rename-fn]
  (let [rename #(rename-fn pattern (or % "nil"))]
    (fn [{:keys [timestamp value]}]
      (if (is-timed-value? value)
        (let [t (get-time value)]
          (for [[k v] (get-value value)]
            [(rename k) v t]))
        (for [[k v] value]
          (adjust-time (rename k) v timestamp))))))

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
