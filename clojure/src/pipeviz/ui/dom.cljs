(ns pipeviz.ui.dom
    "DOM manipulation helpers"
    (:require ["d3" :as d3]
              [clojure.string :as str]
              [pipeviz.styles :as styles]))

;; Core DOM helpers
(defn $id [id] (.getElementById js/document id))
(defn on! [el event f] (when el (.addEventListener el event f)))
(defn set-html! [el html] (when el (set! (.-innerHTML el) html)))
(defn add-class! [el cls] (when el (.add (.-classList el) cls)))
(defn remove-class! [el cls] (when el (.remove (.-classList el) cls)))
(defn toggle-class! [el cls] (when el (.toggle (.-classList el) cls)))

(defn dark? []
      (= "dark" (.getAttribute js/document.documentElement "data-theme")))

;; HTML rendering helpers
(defn detail-row
      "Render a label/value detail row"
      ([label value] (detail-row label value nil))
      ([label value opts]
       (str "<div class='detail-label'>" label "</div>"
            "<div class='detail-value" (when (:class opts) (str " " (:class opts))) "'>" value "</div>")))

(defn badge
      "Render a badge span"
      ([text badge-class] (str "<span class='badge " badge-class "'>" text "</span>"))
      ([text badge-class style] (str "<span class='badge " badge-class "' style='" style "'>" text "</span>")))

(defn badges
      "Render multiple badges from a collection"
      [items badge-class]
      (str/join "" (map #(badge % badge-class) items)))

(defn lineage-link
      "Render a clickable lineage link"
      ([name depth] (lineage-link name depth false))
      ([name depth show-prefix?]
       (let [prefix (if (and show-prefix? (> depth 1)) "└ " "")]
            (str "<div class='lineage-link' data-node-name='" name "' style='" (styles/lineage-style depth) "'>"
                 prefix name "</div>"))))

(defn lineage-section
      "Render upstream or downstream lineage section"
      ([label items] (lineage-section label items false))
      ([label items show-prefix?]
       (when (seq items)
             (str "<div class='detail-label'>" label " (" (count items) ")</div>"
                  "<div class='detail-value'>"
                  (str/join "" (map (fn [{:keys [name depth]}] (lineage-link name depth show-prefix?))
                                    (sort-by :depth items)))
                  "</div>"))))

;; SVG icons
(def icon-copy "<svg width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2'><rect x='9' y='9' width='13' height='13' rx='2' ry='2'/><path d='M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1'/></svg>")
(def icon-check "<svg width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2'><polyline points='20 6 9 17 4 12'/></svg>")

(defn setup-copy-btn! [btn text]
      (on! btn "click"
           (fn [_]
               (-> (js/navigator.clipboard.writeText text)
                   (.then (fn []
                              (add-class! btn "copied")
                              (set! (.-innerHTML btn) icon-check)
                              (js/setTimeout #(do (remove-class! btn "copied")
                                                  (set! (.-innerHTML btn) icon-copy))
                                             1500)))))))

;; Graph highlighting helpers
(defn clear-graph-highlight! [graph-selector & {:keys [clusters?] :or {clusters? false}}]
      (let [graph (d3/select graph-selector)]
           (-> graph (.selectAll ".node")
               (.classed "node-highlighted" false)
               (.classed "node-connected" false)
               (.classed "node-dimmed" false))
           (-> graph (.selectAll ".edge")
               (.classed "edge-highlighted" false)
               (.classed "edge-dimmed" false))
           (when clusters?
                 (-> graph (.selectAll ".cluster")
                     (.classed "cluster-highlighted" false)
                     (.classed "cluster-connected" false)
                     (.classed "cluster-dimmed" false)))))

(defn setup-view-toggle! [container]
      (when-let [slider (.querySelector container ".lineage-toggle-slider")]
                (on! slider "click"
                     (fn [_]
                         (let [is-json (.toggle (.-classList slider) "json-active")
                               tree (.querySelector container ".lineage-tree-view")
                               json (.querySelector container ".lineage-json-view")
                               tree-label (.querySelector container ".lineage-toggle-label[data-view='tree']")
                               json-label (.querySelector container ".lineage-toggle-label[data-view='json']")]
                              (if is-json
                                  (do (add-class! tree "hidden") (remove-class! json "hidden")
                                      (remove-class! tree-label "active") (add-class! json-label "active"))
                                  (do (remove-class! tree "hidden") (add-class! json "hidden")
                                      (add-class! tree-label "active") (remove-class! json-label "active"))))))))
