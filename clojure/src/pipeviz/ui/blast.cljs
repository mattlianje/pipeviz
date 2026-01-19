(ns pipeviz.ui.blast
  "Blast radius modal"
  (:require ["d3" :as d3]
            [clojure.string :as str]
            [pipeviz.core :as core]
            [pipeviz.ui.dom :refer [$id on! set-html! add-class! remove-class! icon-copy setup-copy-btn!]]
            [pipeviz.ui.hash :as hash]
            [pipeviz.ui.state :as state]))

(defn- dark? []
  (= "dark" (.getAttribute js/document.documentElement "data-theme")))

(defn hide-modal! []
  (when-let [modal ($id "blast-radius-modal")]
    (remove-class! modal "show")
    (set! (.-display (.-style modal)) "none")
    ;; Clear blast from URL hash
    (hash/clear-blast!)))

(defn show! [node-name]
  (when-let [config (:config @state/app)]
    (let [modal ($id "blast-radius-modal")
          graph-container ($id "blast-radius-graph")
          summary-container ($id "blast-radius-summary")
          node-name-el ($id "blast-radius-node-name")
          blast-cache (:blast-radius-cache @state/app)]
      (when (and modal graph-container summary-container)
        ;; Update URL with blast parameter
        (hash/update-blast! node-name)

        ;; Set node name in title
        (when node-name-el
          (set! (.-textContent node-name-el) node-name))

        ;; Reset graph container
        (set! (.-innerHTML graph-container) "")
        (swap! state/app assoc :blast-radius-graphviz nil)

        ;; Get analysis from cache or compute on-the-fly
        (let [analysis (core/generate-blast-radius-analysis config node-name blast-cache)]
          (if (or (nil? analysis) (zero? (:total-affected analysis)))
            ;; No downstream dependencies
            (do
              (set-html! summary-container
                         "<div style='text-align: center; padding: 2rem; color: var(--text-muted);'>
                            <div style='font-size: 2rem; margin-bottom: 1rem;'>✓</div>
                            <div>No downstream dependencies</div>
                            <div style='font-size: 0.85em; margin-top: 0.5rem;'>This node has no downstream impact.</div>
                          </div>")
              (set-html! graph-container
                         "<div style='display: flex; align-items: center; justify-content: center; height: 100%; color: var(--text-muted);'>
                            No downstream dependencies to visualize
                          </div>"))
            ;; Has downstream dependencies - build summary
            (let [is-group? (= :group (:source-type analysis))
                  depth-colors ["#c98b8b" "#d4a574" "#c4c474" "#7cb47c" "#6b9dc4" "#a88bc4"]

                  ;; Group members section
                  group-members-html
                  (if (and is-group? (:group-members analysis))
                    (str "<div style='margin-bottom: 1rem;'>"
                         "<div style='font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.25rem;'>"
                         "Group Members (" (:group-size analysis) ")</div>"
                         "<div style='display: flex; flex-wrap: wrap; gap: 0.25rem;'>"
                         (str/join "" (map #(str "<span style='font-size: 0.75rem; padding: 0.15rem 0.4rem; background: var(--bg-secondary); border-radius: 3px; border-left: 2px solid #00897b;'>▢ " % "</span>")
                                           (:group-members analysis)))
                         "</div></div>")
                    "")

                  ;; Impact summary section
                  impact-html
                  (str "<div style='margin-bottom: 1rem;'>"
                       "<div style='font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.25rem;'>Impact Summary</div>"
                       "<div style='display: flex; gap: 1rem;'>"
                       "<div style='text-align: center; padding: 0.5rem 1rem; background: var(--bg-secondary); border-radius: 4px;'>"
                       "<div style='font-size: 1.5rem; font-weight: bold; color: #c98b8b;'>" (:total-affected analysis) "</div>"
                       "<div style='font-size: 0.7rem; color: var(--text-muted);'>Total Affected</div>"
                       "</div>"
                       "<div style='text-align: center; padding: 0.5rem 1rem; background: var(--bg-secondary); border-radius: 4px;'>"
                       "<div style='font-size: 1.5rem; font-weight: bold; color: #6b9dc4;'>" (:max-depth analysis) "</div>"
                       "<div style='font-size: 0.7rem; color: var(--text-muted);'>Max Depth</div>"
                       "</div></div></div>")

                  ;; Nodes by depth section
                  depth-html
                  (str/join ""
                            (for [[depth nodes] (:by-depth analysis)]
                              (let [color (nth depth-colors (min depth (dec (count depth-colors))))]
                                (str "<div style='margin-bottom: 0.5rem;'>"
                                     "<div style='font-size: 0.7rem; color: var(--text-muted); text-transform: uppercase; margin-bottom: 0.25rem;'>"
                                     "Depth " depth " <span style='color: " color ";'>●</span></div>"
                                     "<div style='display: flex; flex-wrap: wrap; gap: 0.25rem;'>"
                                     (str/join ""
                                               (for [node nodes]
                                                 (let [icon (if (= :pipeline (:type node)) "▢" "○")]
                                                   (str "<span style='font-size: 0.75rem; padding: 0.15rem 0.4rem; background: var(--bg-secondary); border-radius: 3px; border-left: 2px solid " color ";'>"
                                                        icon " " (:name node) "</span>"))))
                                     "</div></div>"))))

                  ;; JSON view
                  analysis-json (js/JSON.stringify (clj->js analysis) nil 2)
                  json-html
                  (str "<div id='blast-radius-json-view' style='display: none; position: relative;'>"
                       "<button class='json-copy-btn' id='blast-radius-copy-btn' title='Copy to clipboard'>" icon-copy "</button>"
                       "<pre style='font-size: 0.7rem; max-height: 400px; overflow: auto; background: var(--bg-secondary); color: var(--text-primary); padding: 0.75rem; padding-top: 2rem; border-radius: 4px; margin: 0; border: 1px solid var(--border-color);'>"
                       analysis-json
                       "</pre></div>")

                  summary-html
                  (str "<div class='lineage-view-toggle' style='margin-bottom: 0.75rem;'>"
                       "<span class='lineage-toggle-label active' data-view='tree'>Summary</span>"
                       "<div class='lineage-toggle-slider' id='blast-radius-toggle'></div>"
                       "<span class='lineage-toggle-label' data-view='json'>JSON</span>"
                       "</div>"
                       "<div id='blast-radius-tree-view'>"
                       group-members-html
                       impact-html
                       depth-html
                       "</div>"
                       json-html)]

              (set-html! summary-container summary-html)

              ;; Set up toggle listener
              (when-let [toggle ($id "blast-radius-toggle")]
                (on! toggle "click"
                     (fn [_]
                       (let [is-json? (.toggle (.-classList toggle) "json-active")
                             tree-view ($id "blast-radius-tree-view")
                             json-view ($id "blast-radius-json-view")
                             tree-label (.querySelector summary-container ".lineage-toggle-label[data-view='tree']")
                             json-label (.querySelector summary-container ".lineage-toggle-label[data-view='json']")]
                         (if is-json?
                           (do
                             (set! (.-display (.-style tree-view)) "none")
                             (set! (.-display (.-style json-view)) "block")
                             (remove-class! tree-label "active")
                             (add-class! json-label "active"))
                           (do
                             (set! (.-display (.-style tree-view)) "block")
                             (set! (.-display (.-style json-view)) "none")
                             (add-class! tree-label "active")
                             (remove-class! json-label "active")))))))

              ;; Set up copy button
              (when-let [copy-btn ($id "blast-radius-copy-btn")]
                (setup-copy-btn! copy-btn analysis-json))

              ;; Render graph after modal is visible
              (js/setTimeout
               (fn []
                 (let [dot (core/generate-blast-radius-dot analysis (dark?))
                       width (or (.-clientWidth graph-container) 600)
                       height 400]
                   (when dot
                     (try
                       ;; Clear any existing content completely
                       (set! (.-innerHTML graph-container) "")
                       ;; Create fresh graphviz with zoom disabled (we add our own)
                       (let [gv (-> (.select d3 "#blast-radius-graph")
                                    (.graphviz)
                                    (.width width)
                                    (.height height)
                                    (.fit true)
                                    (.zoom false)
                                    (.on "end"
                                         (fn []
                                           ;; Add manual zoom behavior after render
                                           (let [svg (.select d3 "#blast-radius-graph svg")
                                                 g (.select svg "g")
                                                 zoom (-> (.zoom d3)
                                                          (.scaleExtent #js [0.1 4])
                                                          (.on "zoom"
                                                               (fn [event]
                                                                 (.attr g "transform" (.-transform event)))))]
                                             (.call svg zoom)
                                             ;; Double-click to reset zoom
                                             (.on svg "dblclick.zoom"
                                                  (fn []
                                                    (-> svg
                                                        (.transition)
                                                        (.duration 300)
                                                        (.call (.transform zoom (.zoomIdentity d3))))))))))]
                         (swap! state/app assoc :blast-radius-graphviz gv)
                         (.renderDot gv dot))
                       (catch :default e
                         (js/console.error "Blast radius graph error:" e)
                         (set-html! graph-container
                                    "<div style='display: flex; align-items: center; justify-content: center; height: 100%; color: var(--text-muted);'>Error rendering graph</div>"))))))
               100)))

          ;; Show modal
          (add-class! modal "show")
          (set! (.-display (.-style modal)) "flex"))))))
