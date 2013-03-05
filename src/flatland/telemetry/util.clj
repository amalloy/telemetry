(ns flatland.telemetry.util)

(defn memoize*
  "Fills its memoization cache with thunks instead of actual values, so that there is no possibility
  of calling f multiple times concurrently. Also exposes its memozation cache in the returned
  function's metadata, to permit outside fiddling."
  [f]
  (when-not (ifn? f)
    (throw (IllegalArgumentException. (format "Attempting to memoize non-function %s" (pr-str f)))))
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