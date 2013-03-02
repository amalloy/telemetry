(ns flatland.telemetry.cassette
  "Module for logging data to cassette backups."
  (:require [lamina.core :as lamina]
            [aleph.tcp :as tcp]
            [gloss.core :as gloss]
            [flatland.telemetry.graphing :as graphing]
            [lamina.connections :as connection]
            [flatland.telemetry.graphite.config :as config]
            [compojure.core :refer [GET]])
  (:use flatland.useful.debug))

(defn init [{:keys [base-path file-size]}]
  (let []
    {:name :cassette
     :shutdown (fn []
                 ;; do nothing, lol
                 )
     :listen (fn listen [ch name]
               (let [topic (cassette/open-or-create root-path name
                                                    (compile-frame (string :utf-8)
                                                                   encode-json->string
                                                                   decode-json))]
                 (lamina/receive-all ch
                                     (fn [x]
                                       (write-message! topic x)))))}))
