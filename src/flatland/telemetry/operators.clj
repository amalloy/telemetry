(ns flatland.telemetry.operators
  (:require [lamina.time :as t]
            [lamina.core :as lamina :refer [channel enqueue receive-all enqueue-and-close]]
            [lamina.core.operators :as op]
            [lamina.query.operators :as q]
            [lamina.query.core :refer [def-query-operator def-query-comparator]]
            [flatland.useful.utils :refer [returning]]))

(defn within-window-op [trigger {:keys [id action window task-queue period]
                                 :or {id :id, action :action,
                                      window (t/hours 1) period (t/period)
                                      task-queue (t/task-queue)}}
                        ch]
  (let [result (channel)
        watch-list (ref {})
        expiries (ref (sorted-map))
        conj (fnil conj [])
        now #(t/now task-queue)
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
                              (alter expiries update-in [(+ (now) window)]
                                     conj this-id))))))
        :emitter (fn []
                   (dosync
                    (let [watches @watch-list
                          expired (subseq @expiries <= (now))
                          ids (mapcat val expired)]
                      (alter expiries #(apply dissoc % (map key expired)))
                      (alter watch-list #(apply dissoc % ids))
                      (for [id ids]
                        {:id id, :actions (get watches id)}))))
        :period period
        :task-queue task-queue}))))

(def-query-operator within-window
  :periodic? true
  :distribute? false
  :transform (fn [{:keys [options]} ch]
               (within-window-op (get options 0) (dissoc options 0) ch)))

(def-query-comparator contains
  (fn [field value]
    (let [f (comp (partial map q/normalize-for-comparison)
                  (q/getter field))
          pred #{value}]
      #(some pred (f %)))))
