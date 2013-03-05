(ns flatland.telemetry.util)

(defn memoize*
  "Fills its memoization cache with thunks instead of actual values, so that there is no possibility
  of calling f multiple times concurrently. Also exposes its memozation cache in the returned
  function's metadata, to permit outside fiddling."
  [f]
  (.println System/out (format "Memoizing %s" f))
  (when-not (ifn? f)
    (throw (IllegalArgumentException. (format "Attempting to memoize non-function %s" (pr-str f)))))
  (let [cache (atom {})]
    (-> (fn [& args]
          (let [thunk (delay (apply f args))]
            (when-not (contains? @cache args)
              (swap! cache
                     (fn [cache]
                       (if (contains? cache args)
                         (do (.println System/out (format "Found existing thunk for %s"
                                                          (pr-str args)))
                             cache)
                         (assoc cache args thunk)))))
            (force (get @cache args))))
        (with-meta {:cache cache}))))
