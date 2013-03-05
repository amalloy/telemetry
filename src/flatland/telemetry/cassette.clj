(ns flatland.telemetry.cassette
  "Module for logging data to cassette backups."
  (:require [lamina.core :as lamina]
            [aleph.formats :refer [encode-json->string decode-json]]
            [flatland.cassette :refer [create-or-open append-message!]]
            [flatland.telemetry.sinks :as sinks]
            [flatland.telemetry.util :refer [memoize*]]
            [gloss.core :refer [string compile-frame]]))

(defn init [{:keys [base-path file-size]}]
  (let [nexus (lamina/channel :permanent? true :grounded? true)
        open (let [codec (compile-frame (string :utf-8)
                                        encode-json->string
                                        decode-json)]
               (memoize* (fn [name]
                           (create-or-open base-path name codec file-size))))]
    (lamina/receive-all nexus
                        (fn [[label value time]]
                          (when (seq value)
                            (append-message! (open label)
                                             {:time time, :messages value}))))
    {:name :cassette
     :shutdown (fn shutdown []
                 ;; the only way to close these files is to let them get GCed
                 (reset! (:cache (meta open)) nil))
     :listen (fn [ch name]
               (-> ch
                   (->> (lamina/mapcat* (sinks/sink name)))
                   (lamina/siphon nexus)))}))
