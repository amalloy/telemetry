(ns flatland.telemetry.email
  (:require [flatland.telemetry.graphite.config :as config]
            [flatland.useful.map :refer [keyed]]
            [flatland.useful.utils :as useful]
            [cheshire.core :as cheshire]
            [postal.core :as postal]
            [lamina.core :as lamina]
            [clojure.string :as s]))

(def formatter
  {:edn pr-str
   :json cheshire/generate-string
   :pretty-json #(cheshire/generate-string % {:pretty true})})

(defn mailer [send-mail from-addr format]
  (let [generate (formatter format)]
    (fn [{:keys [to subject message]}]
      (send-mail {:to [to]
                  :from from-addr
                  :subject subject
                  :body [{:type "text/plain"
                          :content (str "Telemetry alert:\n"
                                        (generate message)
                                        "\n")}]}))))

(defn extract-value [{:keys [timestamp value]}]
  (if (map? value)
    (into {} (for [[k v] value
                   :when (not (util/empty-coll? v))]
               [k v]))
    value))

(defn init
  "Supported config options:
   - base-path: the path under which to store all the phonograph files.
   - db-opts: a function taking in a label and returning options to open its phonograph file
   - archive-retentions: a function taking in a label and returning a sequence of storage specs.
     each storage spec should have a :granularity and a :duration, in time units as supported by
     flatland.telemetry.graphite.config/unit-multipliers."
  [{:keys [email! from-addr format]
    :or {format :pretty-json, :from-addr "", email! postal/send-message} :as config}]
  (let [nexus (lamina/channel* :permanent? true :grounded? true)]
    (lamina/receive-all nexus (mailer email! from-addr format))
    {:name :email
     :shutdown (fn shutdown []
                 ;; do nothing: all siphons into the nexus are closed already, so
                 ;; we just have to wait for it to be GCed
                 )
     :listen (fn listen [ch target]
               (let [[to subject] (s/split target #":" 2)
                     template (keyed [to subject])]
                 (-> ch
                     (->> (lamina/map* extract-value)
                          (lamina/remove* useful/empty-coll?)
                          (lamina/map* (fn [value]
                                         (assoc template :message value))))
                     (lamina/siphon nexus))))
     :period (fn [label]
               30000) ;; ms
     :debug {:config config, :nexus nexus}}))
