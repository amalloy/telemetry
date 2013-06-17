(ns flatland.telemetry.util
  (:require [me.raynes.fs :as fs]
            [flatland.useful.seq :refer [lazy-loop]]
            [clojure.string :as s]))

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

(defn path->targets [root extension]
  (lazy-loop [dir (fs/file root)]
    (let [name (fs/base-name dir)]
      (if (fs/directory? dir)
        (for [child (.listFiles dir)
              target (lazy-recur child)]
          (cons name target))
        (let [[name ext] (fs/split-ext name)]
          (when (= ext extension)
            [[name]]))))))

(defn target-names [targets]
  (for [target targets]
    (s/join ":" (rest target))))

(letfn [(comparator [direction]
          (fn [f]
            (fn [a b]
              (direction (f a) (f b)))))]
  (def descending (comparator >))
  (def ascending (comparator <)))
