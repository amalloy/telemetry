(ns flatland.telemetry.operators
  (:require [lamina.time :as t]
            [lamina.core :as lamina :refer [channel enqueue receive-all enqueue-and-close]]
            [lamina.core.operators :as op]
            [lamina.query.operators :as q]
            [lamina.query.core :refer [def-query-operator]]
            [flatland.useful.utils :refer [returning]]))

(defn within-window-op [{:keys [id action trigger window q period]
                         :or {id :id, action :action,
                              window (t/hours 1) q (t/task-queue) period (t/period)}}
                        ch]
  (let [result (channel)
        watch-list (ref {})
        expiries (ref (sorted-map))
        conj (fnil conj [])
        [get-id get-action] (map q/getter [id action])
        trigger (name trigger)]
    (lamina/concat*
     (op/bridge-accumulate ch result "within-window"
       {:accumulator (fn [x]
                       (let [[this-id this-action] ((juxt get-id get-action) x)]
                         (dosync
                          (let [present? (contains? @watch-list this-id)
                                trigger? (= trigger (name this-action))]
                            (when (or present? trigger?)
                              (alter watch-list update-in [this-id]
                                     conj this-action))
                            (when (and trigger? (not present?))
                              (alter expiries update-in [(+ (t/now q) window)]
                                     conj this-id))))))
        :emitter (fn []
                   (dosync
                    (let [watches @watch-list
                          expired (subseq @expiries <= (t/now q))
                          ids (mapcat val expired)]
                      (alter expiries #(apply dissoc % (map key expired)))
                      (alter watch-list #(apply dissoc % ids))
                      (for [id ids]
                        {:id id, :actions (get watches id)}))))
        :period period
        :task-queue q}))))

(def-query-operator within-window
  :periodic? true
  :distribute? false
  :transform (fn [{:keys [options]} ch]
               (within-window-op options ch)))
