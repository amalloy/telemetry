(ns flatland.telemetry.cassette
  "Module for logging data to cassette backups."
  (:require [lamina.core :as lamina]
            [aleph.formats :refer [encode-json->string decode-json]]
            [flatland.cassette :refer [create-or-open append-message!]]
            [flatland.telemetry.sinks :as sinks]
            [flatland.telemetry.util :refer [memoize*]]
            [clojure.string :as s]
            [gloss.core :refer [string compile-frame]]))

(defn sink
  "Like the default sinks/sink, but names including a % are rewritten to exactly match the incoming
  topic. e.g., if a messages topic is foo:bar, then the name foo:% would cause it to be written with
  the topic foo:bar, while the name foo:* would cause it to be written as foo:foo:bar.

  This is used because cassette messages, once written to disk, are generally intended to be looked
  up by exact topic match, but there may be multiple different listener groups and they can't all be
  named *. So we say that any label containing a % character will act like just \"*\"."
  [name]
  (sinks/sink (if (re-find #"%" name)
                "*"
                name)))

(def codec (compile-frame (string :utf-8)
                          encode-json->string
                          decode-json))

(defn init [{:keys [base-path file-size]}]
  (let [nexus (lamina/channel* :permanent? true :grounded? true)
        open (memoize* (fn [name]
                         (create-or-open base-path name codec file-size)))]
    (lamina/receive-all nexus
                        (fn [[label value time]]
                          (when (seq value)
                            (append-message! (open label)
                                             {:time time, :messages value}))))
    {:name :cassette
     :subscription-filter (fn [{:keys [label query] :as original}]
                            (if (and (re-find #"\*" label)
                                     (-> (re-pattern (s/replace label "*" ".*"))
                                         (re-matches query)))
                              {:label (s/replace label "*" "%")
                               :query (str query ".group-by(topic)")}
                              original))
     :shutdown (fn shutdown []
                 ;; the only way to close these files is to let them get GCed
                 (reset! (:cache (meta open)) nil))
     :listen (fn [ch name]
               (-> ch
                   (->> (lamina/mapcat* (sink name)))
                   (lamina/siphon nexus)))}))
