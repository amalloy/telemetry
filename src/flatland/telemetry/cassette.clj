(ns flatland.telemetry.cassette
  "Module for logging data to cassette backups."
  (:require [lamina.core :as lamina]
            [aleph.formats :refer [encode-json->string decode-json]]
            [flatland.cassette :as cassette :refer [create-or-open append-message!]]
            [flatland.telemetry.sinks :as sinks]
            [flatland.telemetry.util :refer [memoize*]]
            [flatland.useful.seq :as seq]
            [me.raynes.fs :as fs]
            [clojure.string :as s]
            [gloss.core :refer [string compile-frame]]))

(def codec (compile-frame (string :utf-8)
                          encode-json->string
                          #(decode-json % false)))

(defn replay-generator [{:keys [base-path codec] :or {codec codec}}]
  (let [base-file (fs/file base-path)]
    (fn [{:keys [pattern start-time]}]
      (let [streams (for [file (fs/glob base-file pattern)]
                      (let [topic (fs/base-name file)]
                        (for [record (cassette/messages-since (cassette/open file codec false)
                                                               #(>= (% "time") start-time))]
                          (assoc record "messages"
                                 (for [message (get record "messages")]
                                   (assoc message :topic topic))))))
            timeline (apply seq/merge-sorted #(< (%1 "time") (%2 "time"))
                            (pmap seq streams))]
        (for [{:strs [time messages]} timeline
              message messages]
          (-> message
              (dissoc "timestamp")
              (assoc :timestamp time)))))))

(defn init [{:keys [base-path file-size] :as config}]
  (let [nexus (lamina/channel* :permanent? true :grounded? true)
        open (memoize* (fn [name]
                         (create-or-open (fs/file base-path name) codec file-size)))]
    (lamina/receive-all nexus
                        (fn [[label value time]]
                          (when (seq value)
                            (append-message! (open label)
                                             {:time time, :messages (for [message value]
                                                                      (dissoc message :topic))}))))
    {:name :cassette
     :replay (replay-generator config)
     :shutdown (fn shutdown []
                 ;; the only way to close these files is to let them get GCed
                 (reset! (:cache (meta open)) nil))
     :targets (fn []
                (map fs/base-name (.listFiles (fs/file base-path))))
     :listen (fn [ch name]
               (-> ch
                   (->> (lamina/mapcat* (sinks/sink name)))
                   (lamina/siphon nexus)))}))
