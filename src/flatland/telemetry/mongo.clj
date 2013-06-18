(ns flatland.telemetry.mongo
  (:require [lamina.core :as lamina]
            [lamina.query :as query]
            [lamina.trace :as trace]
            [aleph.formats :as formats]
            [clojure.string :as s]
            [flatland.laminate.render :as laminate]
            [flatland.telemetry.util :refer [ascending render-handler]]
            [flatland.useful.map :refer [keyed]]
            [flatland.useful.seq :as seq]
            [compojure.core :refer [GET]]
            [somnium.congomongo :as mongo]))

(defn collection-lookup []
  (let [collections (delay (mongo/collections))]
    (fn [target]
      (if (re-find #"\*" target)
        (let [regex (re-pattern (s/replace target "*" ".*"))]
          (filter #(re-matches regex %) @collections))
        [target]))))

(defn mongo-seq [conn from until]
  (let [collections (collection-lookup)]
    (fn [target]
      (mongo/with-mongo conn
        (seq ;; make sure to start realizing each coll while conn is still bound
         (apply seq/merge-sorted (ascending :timestamp)
                (for [collection (collections target)]
                  (for [{:keys [timestamp values]} (mongo/fetch collection
                                                                :where {:timestamp {:$gte from
                                                                                    :$lt until}}
                                                                :only {:_id false})
                        :let [time (* timestamp 1000)]
                        value values]
                    {:timestamp time, :payload value}))))))))

(defn handler [conn]
  (render-handler (fn [from until]
                    (mongo-seq conn from until))
                  {}))

(defn init [{:keys [uri] :or {uri "mongodb://localhost/telemetry"}}]
  (let [conn (mongo/make-connection uri)]
    {:name :mongo
     :shutdown #(mongo/close-connection conn)
     :listen (fn [ch target]
               (mongo/with-mongo conn
                 (mongo/add-index! target [:timestamp] :unique true))
               ;; TODO bulk inserts across multiple collections?
               (lamina/receive-all (query/query-stream ".partition-every(period:1)"
                                                       {:timestamp :timestamp
                                                        :payload identity}
                                                       ch)
                                   (fn [values]
                                     (when (seq values)
                                       (let [timestamp (:timestamp (first values))]
                                         (mongo/with-mongo conn
                                           ;; TODO clean up congomongo if i get around to it
                                           (try
                                             (mongo/insert! target
                                                            {:timestamp timestamp
                                                             :values (map :value values)})
                                             (catch Exception e
                                               (trace/trace :mongo:error
                                                            (keyed [uri target values]))))))))))
     :handler (handler conn)
     :debug {:mongo conn}}))
