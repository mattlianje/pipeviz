(ns pipeviz.ui.hash
    "URL hash parsing and manipulation"
    (:require [clojure.string :as str]))

(defn parse []
      "Parse current URL hash into a map"
      (let [hash (subs (or (.-hash js/window.location) "") 1)
            parts (str/split hash #"&")]
           (reduce (fn [acc part]
                       (cond
                        (str/includes? part "=")
                        (let [[k v] (str/split part #"=" 2)]
                             (assoc acc (keyword k) (js/decodeURIComponent v)))
                        (not (str/blank? part))
                        (assoc acc :tab part)
                        :else acc))
                   {}
                   parts)))

(defn build
      "Build hash string from map"
      [{:keys [tab node blast view pipelines]}]
      (let [parts (cond-> []
                    tab (conj tab)
                    node (conj (str "node=" (js/encodeURIComponent node)))
                    blast (conj (str "blast=" (js/encodeURIComponent blast)))
                    view (conj (str "view=" (js/encodeURIComponent view)))
                    (seq pipelines) (conj (str "pipelines=" (str/join "," (map js/encodeURIComponent pipelines)))))]
           (str/join "&" parts)))

(defn get-node []
      (:node (parse)))

(defn get-blast []
      (:blast (parse)))

(defn get-planner-state []
      (let [{:keys [view pipelines]} (parse)]
           {:view (when view (keyword view))
            :pipelines (when pipelines (str/split pipelines #","))}))

(defn set-hash! [hash-str]
      (.replaceState js/history nil "" (if (str/blank? hash-str) "" (str "#" hash-str))))

(defn update-node! [node-name]
      (let [current (parse)
            updated (if node-name
                        (assoc current :node node-name)
                        (dissoc current :node))]
           (set-hash! (build updated))))

(defn update-blast! [blast-name]
      (let [current (parse)
            updated (-> current
                        (assoc :tab (or (:tab current) "graph"))
                        (assoc :blast blast-name))]
           (set-hash! (build updated))))

(defn clear-blast! []
      (let [current (parse)
            updated (dissoc current :blast)]
           (set-hash! (build updated))))

(defn update-planner! [view pipelines]
      (set-hash! (build {:tab "planner"
                         :view (name view)
                         :pipelines pipelines})))

(defn clear! []
      (.replaceState js/history nil "" (.-pathname js/window.location)))
