(ns flatland.telemetry.cassette
  "Module for logging data to cassette backups."
  (:require [lamina.core :as lamina]
            [lamina.trace :as trace]
            [aleph.formats :refer [encode-json->string decode-json]]
            [flatland.cassette :as cassette :refer [create-or-open append-message!]]
            [flatland.telemetry.sinks :as sinks]
            [flatland.telemetry.util :refer [memoize* ascending render-handler]]
            [flatland.useful.seq :as seq]
            [me.raynes.fs :as fs]
            [clojure.string :as s]
            [gloss.core :refer [string compile-frame]]))

(def codec (compile-frame (string :utf-8)
                          encode-json->string
                          #(decode-json % false)))

(defn cassette-seq [{:keys [base-path codec] :or {codec codec}}]
  (let [base-file (fs/file base-path)
        get-time #(get % "time")]
    (fn [pattern from until]
      (let [streams (for [file (fs/glob base-file pattern)]
                      (let [topic (fs/base-name file)]
                        (for [record (cassette/messages-since (cassette/open file codec false)
                                                              #(>= (% "time") from))]
                          (assoc record "messages"
                                 (for [message (get record "messages")]
                                   (assoc message :topic topic))))))
            timeline (seq/increasing* get-time compare
                                      (apply seq/merge-sorted (ascending get-time)
                                             (pmap seq streams)))]
        (for [[in-order? entry] timeline
              {:strs [time messages]} (if in-order?
                                        [entry]
                                        (do (trace/trace :cassette:timeline:error entry)
                                            nil))
              :while (< time until)
              message messages]
          (-> message
              (dissoc "timestamp")
              (assoc :timestamp time)))))))

(defn replay-generator [config]
  (let [generator (cassette-seq config)]
    (fn [{:keys [pattern start-time]}]
      (generator pattern start-time Long/MAX_VALUE))))

(defn handler [config]
  (render-handler (fn [from until]
                    (let [generator (cassette-seq config)]
                      (fn [pattern]
                        (generator pattern from until))))
                  {:timestamp #(* 1000 (:timestamp %)) :payload identity}))

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
     :handler (handler config)
     :listen (fn [ch name]
               (-> ch
                   (->> (lamina/mapcat* (sinks/sink name)))
                   (lamina/siphon nexus)))}))
