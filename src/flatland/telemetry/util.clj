(ns flatland.telemetry.util
  (:require [lamina.time :as t]
            [lamina.core :as lamina :refer [channel enqueue receive-all enqueue-and-close]]
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


(defn within-window [{:keys [id action trigger window q]
                      :or {id :id, action :action, window (t/hours 1) q (t/task-queue)}}]

  (fn [ch]
    (let [result (channel)
          log (ref {})
          conj (fnil conj [])
          report (fn [this-id actions]
                   (enqueue result {id this-id, :actions actions}))]
      (receive-all ch
                   (fn [x]
                     (let [[this-id this-action] (map x [id action])
                           is-new? (dosync
                                    (let [present? (contains? @log this-id)
                                          trigger? (= this-action trigger)]
                                      (when (or present? trigger?)
                                        (alter log update-in [this-id] conj this-action))
                                      (and trigger?
                                           (not present?))))]
                       (when is-new?
                         (t/invoke-in q window
                                      (fn []
                                        (let [items (dosync (returning (get @log this-id)
                                                              (alter log dissoc this-id)))]
                                          (report this-id items))))))))
      (lamina/on-drained ch (fn []
                              (doseq [[k v] (dosync (returning @log (ref-set log {})))]
                                (report k v))))
      result)))
