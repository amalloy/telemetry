(ns flatland.telemetry.sinks
  (:require [clojure.string :as str])
  (:import java.util.Date))

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
    [[name value timestamp]]))

(defn sink-by-name
  "Returns a function which expects to receive a map of labels to values. Each label has its
   value(s) interpolated into the pattern, and a list of [name value time] tuples is returned."
  [pattern rename-fn]
  (fn [{:keys [timestamp value]}]
    (for [[k v] value]
      (let [name (rename-fn pattern (or k "nil"))]
        [name v timestamp]))))

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
