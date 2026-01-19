(ns pipeviz.ui.stats
  "Stats view showing configuration statistics and health metrics"
  (:require [clojure.string :as str]
            [pipeviz.core.graph :as core]
            [pipeviz.ui.dom :refer [$id set-html!]]
            [pipeviz.ui.state :as state]))

(def ^:private colors
  ["#6b9dc4" "#a88bc4" "#d4a574" "#7cb47c" "#c98b8b" "#6bb5b5" "#a89080" "#8a9aa8"])

(defn- render-pie-chart [data size]
  (when (seq data)
    (let [total (reduce + (map :value data))]
      (when (pos? total)
        (let [paths (atom "")
              current-angle (atom -90)]
          (doseq [[d i] (map vector data (range))]
            (let [angle (/ (* (:value d) 360) total)
                  start-rad (/ (* @current-angle js/Math.PI) 180)
                  end-rad (/ (* (+ @current-angle angle) js/Math.PI) 180)
                  r (- (/ size 2) 1)
                  cx (/ size 2)
                  cy (/ size 2)
                  x1 (+ cx (* r (js/Math.cos start-rad)))
                  y1 (+ cy (* r (js/Math.sin start-rad)))
                  x2 (+ cx (* r (js/Math.cos end-rad)))
                  y2 (+ cy (* r (js/Math.sin end-rad)))
                  color (nth colors (mod i (count colors)))]
              (if (= 1 (count data))
                (swap! paths str "<circle cx=\"" cx "\" cy=\"" cy "\" r=\"" r "\" fill=\"" color "\" />")
                (swap! paths str "<path d=\"M " cx " " cy " L " x1 " " y1 " A " r " " r " 0 "
                       (if (> angle 180) 1 0) " 1 " x2 " " y2 " Z\" fill=\"" color "\" />"))
              (swap! current-angle + angle)))
          (str "<svg width=\"" size "\" height=\"" size "\" viewBox=\"0 0 " size " " size "\">" @paths "</svg>"))))))

(defn- render-pie-legend [data]
  (let [total (reduce + (map :value data))]
    (str/join ""
              (map-indexed
               (fn [i d]
                 (let [pct (if (pos? total) (js/Math.round (* (/ (:value d) total) 100)) 0)
                       color (nth colors (mod i (count colors)))]
                   (str "<div class=\"pie-legend-row\">"
                        "<span class=\"pie-legend-dot\" style=\"background:" color "\"></span>"
                        "<span class=\"pie-legend-name\">" (:label d) "</span>"
                        "<span class=\"pie-legend-val\">" (:value d) " (" pct "%)</span>"
                        "</div>")))
               data))))

(defn- render-coverage-bar [label data]
  (let [pct (if (pos? (:total data)) (js/Math.round (* (/ (:covered data) (:total data)) 100)) 100)
        color (cond (>= pct 80) "good" (>= pct 50) "okay" :else "low")]
    (str "<div class=\"coverage-row\">"
         "<span class=\"coverage-label\">" label "</span>"
         "<div class=\"coverage-bar-wrap\"><div class=\"coverage-bar " color "\" style=\"width: " pct "%\"></div></div>"
         "<span class=\"coverage-pct " color "\">" pct "%</span>"
         "</div>")))

(defn- render-missing-block [label items type]
  (when (seq items)
    (str "<div class=\"missing-block\">"
         "<div class=\"missing-header\">" label " <span class=\"missing-count\">" (count items) "</span></div>"
         "<div class=\"missing-names " type "\">" (str/join ", " items) "</div>"
         "</div>")))

(defn render-stats! []
  (when-let [container ($id "stats-content")]
    (let [config (:config @state/app)]
      (if-not config
        (set-html! container "<div class=\"text-muted\">Load a configuration to see statistics.</div>")
        (let [stats (core/compute-stats config)
              cluster-data (->> (:clusters (:distributions stats))
                                (sort-by val >)
                                (map (fn [[k v]] {:label k :value v}))
                                vec)
              type-data (->> (:types (:distributions stats))
                             (sort-by val >)
                             (map (fn [[k v]] {:label k :value v}))
                             vec)
              html (atom "")]
          ;; Summary row
          (swap! html str
                 "<div class=\"stats-summary\">"
                 "<span><strong>" (get-in stats [:counts :pipelines]) "</strong> pipelines</span>"
                 "<span><strong>" (get-in stats [:counts :datasources]) "</strong> datasources</span>"
                 "<span><strong>" (get-in stats [:counts :clusters]) "</strong> clusters</span>"
                 "</div>")
          ;; Charts row
          (swap! html str
                 "<div class=\"stats-row\">"
                 "<div class=\"chart-box\">"
                 "<div class=\"chart-title\">Pipelines by Cluster</div>"
                 "<div class=\"pie-container\">"
                 (render-pie-chart cluster-data 120)
                 "<div class=\"pie-legend\">" (render-pie-legend cluster-data) "</div>"
                 "</div></div>"
                 "<div class=\"chart-box\">"
                 "<div class=\"chart-title\">Datasources by Type</div>"
                 "<div class=\"pie-container\">"
                 (render-pie-chart type-data 120)
                 "<div class=\"pie-legend\">" (render-pie-legend type-data) "</div>"
                 "</div></div></div>")
          ;; Coverage and hubs row
          (swap! html str
                 "<div class=\"stats-row\">"
                 "<div class=\"stats-section\">"
                 "<div class=\"section-title\">Coverage</div>"
                 "<div class=\"coverage-bars\">"
                 (render-coverage-bar "Schedules" (get-in stats [:coverage :schedules]))
                 (render-coverage-bar "Airflow Links" (get-in stats [:coverage :airflow]))
                 "</div></div>")
          (when (seq (:hubs stats))
            (swap! html str
                   "<div class=\"stats-section\">"
                   "<div class=\"section-title\">Hubs <span class=\"section-hint\">most connected</span></div>"
                   "<div class=\"hub-list\">"
                   (str/join ""
                             (map-indexed
                              (fn [i h]
                                (str "<div class=\"hub-row\">"
                                     "<span class=\"hub-rank\">" (inc i) "</span>"
                                     "<span class=\"hub-name " (:type h) "\">" (:name h) "</span>"
                                     "<span class=\"hub-stats\">↑" (:upstream h) " ↓" (:downstream h) "</span>"
                                     "</div>"))
                              (:hubs stats)))
                   "</div></div>"))
          (swap! html str "</div>")
          ;; Cycles warning
          (when (seq (:cycles stats))
            (swap! html str
                   "<div class=\"stats-section cycles-warning\">"
                   "<div class=\"section-title\">⚠ Cycles Detected <span class=\"cycle-count\">" (count (:cycles stats)) "</span></div>"
                   "<div class=\"section-hint\">Circular dependencies in pipeline graph</div>"
                   "<div class=\"cycle-list\">"
                   (str/join ""
                             (map-indexed
                              (fn [i cycle]
                                (str "<div class=\"cycle-item\">"
                                     "<span class=\"cycle-label\">#" (inc i) "</span>"
                                     "<span class=\"cycle-path\">" (str/join " > " cycle) "</span>"
                                     "</div>"))
                              (:cycles stats)))
                   "</div></div>"))
          ;; Needs attention section
          (let [has-missing (or (seq (get-in stats [:coverage :schedules :missing]))
                                (seq (get-in stats [:coverage :airflow :missing]))
                                (seq (:orphaned stats)))]
            (if has-missing
              (swap! html str
                     "<div class=\"stats-section\">"
                     "<div class=\"section-title\">Needs Attention</div>"
                     "<div class=\"missing-scroll\">"
                     (render-missing-block "Missing Schedule" (get-in stats [:coverage :schedules :missing]) "pipeline")
                     (render-missing-block "Missing Airflow Link" (get-in stats [:coverage :airflow :missing]) "pipeline")
                     (render-missing-block "Orphaned" (:orphaned stats) "orphan")
                     "</div></div>")
              (when (empty? (:cycles stats))
                (swap! html str
                       "<div class=\"stats-section all-good\">"
                       "<div class=\"section-title\">✓ All Good</div>"
                       "<div class=\"section-hint\">No cycles, coverage gaps, or orphaned nodes.</div>"
                       "</div>"))))
          (set-html! container @html))))))

(defn init! []
  (render-stats!))
