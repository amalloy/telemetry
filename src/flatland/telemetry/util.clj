(ns flatland.telemetry.util
  (:require [lamina.time :as t]
            [lamina.core :as lamina :refer [channel enqueue receive-all enqueue-and-close]]
            [lamina.core.operators :as op]
            [lamina.query.operators :as q]
            [lamina.query.core :refer [def-query-operator]]
            [flatland.useful.utils :refer [returning]])
  (:import java.util.Date)
  (:use flatland.useful.debug))

(defn unix-time
  "Number of seconds since the unix epoch, as by Linux's time() system call."
  [^Date date]
  (-> date (.getTime) (quot 1000)))

(defn from-unix-time [time]
  (Date. (long (* time 1000))))

(defmacro delay*
  "Like clojure.core/delay, with a couple changes. First, sadly, it doesn't respond to (force),
   which checks specifically for clojure.lang.Delay. More importantly, it behaves differently if
  your &body throws an exception. In that case, c.c/delay throws your exception the first time, and
  later derefs throw an NPE in the heart of c.l.Delay. delay* saves the first exception and rethrows
  it on later derefs."
  [& body]
  `(let [delay# (delay (try
                         {:success (do ~@body)}
                         (catch Throwable t#
                           {:failure t#})))]
     (reify clojure.lang.IDeref
       (deref [this#]
         (let [ret# @delay#]
           (if (contains? ret# :success)
             (:success ret#)
             (throw (:failure ret#))))))))

(defn memoize*
  "Fills its memoization cache with thunks instead of actual values, so that there is no possibility
  of calling f multiple times concurrently. Also exposes its memozation cache in the returned
  function's metadata, to permit outside fiddling."
  [f]
  (let [cache (atom {})]
    (-> (fn [& args]
          (let [thunk (delay* (apply f args))]
            (when-not (contains? @cache args)
              (swap! cache
                     (fn [cache]
                       (if (contains? cache args)
                         cache
                         (assoc cache args thunk)))))
            (deref (get @cache args))))
        (with-meta {:cache cache}))))

(defn within-window-op [{:keys [id action trigger window q period]
                         :or {id :id, action :action,
                              window (t/hours 1) q (t/task-queue) period (t/period)}}
                        ch]
  (let [result (channel)
        watch-list (ref {})
        expiries (ref (sorted-map))
        conj (fnil conj [])
        [get-id get-action] (map q/getter [id action])
        trigger (name trigger)]
    (lamina/concat*
     (op/bridge-accumulate ch result "within-window"
       {:accumulator (fn [x]
                       (let [[this-id this-action] (map #(% x) [get-id get-action])]
                         (dosync
                          (let [present? (contains? @watch-list this-id)
                                trigger? (= trigger (name this-action))]
                            (when (or present? trigger?)
                              (alter watch-list update-in [this-id]
                                     conj this-action))
                            (when (and trigger? (not present?))
                              (alter expiries update-in [(+ (t/now q) window)]
                                     conj this-id))))))
        :emitter (fn []
                   (dosync
                    (let [watches @watch-list
                          expired (subseq @expiries <= (t/now q))
                          ids (mapcat val expired)]
                      (alter expiries #(apply dissoc % (map key expired)))
                      (alter watch-list #(apply dissoc % ids))
                      (for [id ids]
                        {:id id, :actions (get watches id)}))))
        :period period
        :task-queue q}))))

(def-query-operator within-window
  :periodic? true
  :distribute? false
  :transform (fn [{:keys [options]} ch]
               (within-window-op options ch)))
