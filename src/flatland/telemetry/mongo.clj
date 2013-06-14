(ns flatland.telemetry.mongo
  (:require [lamina.core :as lamina]
            [aleph.formats :as formats]
            [clojure.string :as s]
            [flatland.laminate.render :as laminate]
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
         (apply seq/merge-sorted #(< (:timestamp %1) (:timestamp %2))
                (for [collection (collections target)]
                  (for [{:keys [timestamp values]} (mongo/fetch collection
                                                                :where {:timestamp {:$gte from
                                                                                    :$lt until}}
                                                                :only {:_id false})
                        :let [time (* timestamp 1000)]
                        value values]
                    {:timestamp time, :payload value}))))))))

(defn handler [conn]
  (GET "/render" [target from until shift period align timezone]
    (let [now (System/currentTimeMillis)
          {:keys [targets offset from until period]} (laminate/parse-render-opts
                                                      (keyed [target now from until period
                                                              shift align timezone]))]
      (if-let [result (laminate/points targets offset
                                       (-> {:timestamp :timestamp,
                                            :payload :payload
                                            :seq-generator (mongo-seq conn from until)}
                                           (merge (when period {:period period}))))]
        {:status 200
         :headers {"Content-Type" "application/json"}
         :body (formats/encode-json->string result)}
        {:status 404 :body "Not found\n"}))))

(defn init [{:keys [uri] :or {uri "mongodb://localhost/telemetry"}}]
  (let [conn (mongo/make-connection uri)]
    {:name :mongo
     :shutdown #(mongo/close-connection conn)
     :listen (fn [ch target]
               (mongo/with-mongo conn
                 (mongo/add-index! target [:timestamp] :unique true))
               (lamina/receive-all ch
                                   (fn [{:keys [timestamp value]}]
                                     (mongo/with-mongo conn
                                       (mongo/update! target
                                                      {:timestamp timestamp}
                                                      {:$push {:values value}})))))
     :handler (handler conn)
     :debug {:mongo conn}}))
