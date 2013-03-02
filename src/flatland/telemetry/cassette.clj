(ns flatland.telemetry.cassette
  "Module for logging data to cassette backups."
  (:require [lamina.core :refer [receive-all]]
            [aleph.formats :refer [encode-json->string decode-json]]
            [flatland.cassette :refer [create-or-open append-message!]]
            [gloss.core :refer [string compile-frame]]))

(defn init [{:keys [base-path file-size]}]
  {:name :cassette
   :shutdown (fn []
               ;; do nothing, lol
               )
   :listen (fn listen [ch name]
             (let [topic (create-or-open base-path name
                                         (compile-frame (string :utf-8)
                                                        encode-json->string
                                                        decode-json)
                                         file-size)]
               (receive-all ch
                            (fn [x]
                              (append-message! topic x)))))})
