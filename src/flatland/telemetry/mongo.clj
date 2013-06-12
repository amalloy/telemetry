(ns flatland.telemetry.mongo
  (:require [lamina.core :as lamina]
            [somnium.congomongo :as mongo]))

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
     :debug {:mongo conn}}))
