(ns flatland.telemetry.mongo
  (:require [lamina.core :as lamina]
            [lamina.query :as query]
            [lamina.trace :as trace]
            [aleph.formats :as formats]
            [clojure.string :as s]
            [flatland.laminate.render :as laminate]
            [flatland.telemetry.sinks :as sinks]
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
                                                                :only {:_id false})]
                    {:timestamp (* timestamp 1000)
                     :payload (for [value values]
                                (assoc value :topic collection))}))))))))

(defn handler [conn]
  (render-handler (fn [from until]
                    (mongo-seq conn from until))
                  {}))

(defn init [{:keys [uri] :or {uri "mongodb://localhost/telemetry"}}]
  (let [conn (mongo/make-connection uri)
        nexus (lamina/channel* :permanent? true :grounded? true)]
    (lamina/receive-all nexus
                        (fn store [[target values timestamp]]
                          (when (seq values)
                            (mongo/with-mongo conn
                              (try
                                (mongo/add-index! target [:timestamp] :unique true)
                                (mongo/insert! target {:timestamp timestamp
                                                       :values (for [value values]
                                                                 (dissoc value :topic))})
                                (catch Exception exception
                                  (trace/trace :mongo:error
                                               (keyed [uri target timestamp values exception]))))))))
    {:name :mongo
     :shutdown #(mongo/close-connection conn)
     :listen (fn listen [ch target]
               (-> ch
                   (->> (lamina/mapcat* (sinks/sink target)))
                   (lamina/siphon nexus)))
     :handler (handler conn)
     :debug {:mongo conn}}))
