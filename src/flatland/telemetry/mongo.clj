(ns flatland.telemetry.mongo
  (:require [lamina.core :as lamina]
            [aleph.formats :as formats]
            [flatland.laminate.render :as laminate]
            [flatland.useful.map :refer [keyed]]
            [compojure.core :refer [GET]]
            [somnium.congomongo :as mongo]))

(defn mongo-seq [conn from until]
  (fn [target]
    (mongo/with-mongo conn
      (for [{:keys [timestamp values]} (seque (mongo/fetch target
                                                           :where {:timestamp {:$gte from
                                                                               :$lt until}}
                                                           :only {:_id false}))
            :let [time (* timestamp 1000)]
            value values]
        {:timestamp time, :payload value}))))

(defn handler [conn]
  (GET "/render" [target from until shift period align timezone]
    (let [targets (if (coll? target)
                    target
                    [target])
          now (System/currentTimeMillis)
          {:keys [offset from until period]} (laminate/parse-render-opts
                                              (keyed [now from until period
                                                      shift align timezone]))]
      (if (some #(re-find #"\*" %) targets)
        {:status 400 :body "Mongo plugin doesn't yet support querying targets with wildcards"}
        (if-let [result (laminate/points targets offset
                                         (-> {:timestamp :timestamp,
                                              :payload :payload
                                              :seq-generator (mongo-seq conn from until)}
                                             (merge (when period {:period period}))))]
          {:status 200
           :headers {"Content-Type" "application/json"}
           :body (formats/encode-json->string result)}
          {:status 404 :body "Not found\n"})))))

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
