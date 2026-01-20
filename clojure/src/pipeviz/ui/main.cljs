(ns pipeviz.ui.main
    "Browser entry point"
    (:require ["d3" :as d3]
              ["d3-graphviz" :as d3-graphviz]
              [clojure.string :as str]
              [pipeviz.core.graph :as core]
              [pipeviz.core.styles :as styles]
              [pipeviz.ui.attributes :as attributes]
              [pipeviz.ui.blast :as blast]
              [pipeviz.ui.dom :as dom :refer [$id on! set-html! add-class! remove-class! toggle-class!
                                              detail-row badge badges lineage-link lineage-section
                                              icon-copy icon-check setup-copy-btn!
                                              clear-graph-highlight! setup-view-toggle!]]
              [pipeviz.ui.export :as export]
              [pipeviz.ui.hash :as hash]
              [pipeviz.ui.planner :as planner]
              [pipeviz.ui.state :as state]
              [pipeviz.ui.stats :as stats]
              [pipeviz.ui.tables :as tables]))

;; Ensure d3-graphviz side-effects load
(def ^:private _ d3-graphviz)

(declare render-splash! render-graph! setup-interactivity! show-node-details! render-tables! update-json-input! render-group-toggles! select-group! show-group-details! select-node-from-hash!)

;; View state cache key for snappy toggling
(defn- view-cache-key []
       (str (dom/dark?) "|" (:pipelines-only? @state/app) "|" (pr-str (:collapsed-groups @state/app))))

;; Set config and initialize groups as collapsed by default
(defn set-config! [config]
      (let [all-groups (set (core/all-groups config))]
           (swap! state/app assoc
                  :config config
                  :collapsed-groups all-groups  ;; collapsed by default
                  :dot-cache {}
                  :last-rendered-key nil)))

;; Precompute lineage for all nodes
(defn precompute-lineage! [config]
      (let [adjacency (core/build-full-adjacency config)
            all-nodes (distinct (concat
                                 (map :name (:pipelines config))
                                 (map :name (:datasources config))
                                 (keys (:downstream adjacency))
                                 (keys (:upstream adjacency))))
            cache (into {}
                        (for [node all-nodes]
                             [node (core/full-graph-lineage config node)]))]
           (swap! state/app assoc :lineage-cache cache)
           cache))

;; Precompute blast radius for all nodes and groups
(defn precompute-blast-radius! [config]
      (let [blast-data (core/precompute-all-blast-radius config)]
           (swap! state/app assoc :blast-radius-cache blast-data)
           blast-data))

(defn- precompute-all! [config]
       (precompute-lineage! config)
       (attributes/precompute-lineage! config)
       (precompute-blast-radius! config))

;; Re-export dark? for convenience (used extensively)
(def dark? dom/dark?)

;; Theme
(defn set-theme! [dark?]
      (.setAttribute js/document.documentElement "data-theme" (if dark? "dark" "light"))
      (js/localStorage.setItem "theme" (if dark? "dark" "light"))
      (when-let [btn ($id "theme-toggle")]
                (set! (.-textContent btn) (if dark? "☀️" "🌙")))
      (swap! state/app assoc :dark? dark? :last-rendered-key nil) ;; invalidate cache on theme change
      (when ($id "splash-graph")
            (render-splash!))
      (when (:config @state/app)
            (render-graph!)))

(defn toggle-theme! []
      (set-theme! (not (dark?))))

(defn init-theme! []
      (let [stored (js/localStorage.getItem "theme")
            prefers-dark (.-matches (js/window.matchMedia "(prefers-color-scheme: dark)"))
            use-dark (if stored (= stored "dark") prefers-dark)]
           (set-theme! use-dark)))

;; Sidebar
(defn toggle-sidebar! []
      (when-let [sidebar ($id "sidebar")]
                (toggle-class! sidebar "collapsed")))

;; Tabs
(defn- tab-id->hash-name [tab-id]
       (when tab-id
             (str/replace tab-id #"-pane$" "")))

(defn switch-tab! [tab-id]
  ;; Update tab buttons (old nav-tabs)
      (doseq [btn (array-seq (.querySelectorAll js/document ".nav-tab"))]
             (if (= tab-id (.getAttribute btn "data-tab"))
                 (add-class! btn "active")
                 (remove-class! btn "active")))
  ;; Update sidebar items
      (doseq [item (array-seq (.querySelectorAll js/document ".sidebar-item"))]
             (if (= tab-id (.getAttribute item "data-tab"))
                 (add-class! item "active")
                 (remove-class! item "active")))
  ;; Update panes
      (doseq [pane (array-seq (.querySelectorAll js/document ".tab-pane"))]
             (if (= tab-id (.-id pane))
                 (add-class! pane "active")
                 (remove-class! pane "active")))
  ;; Show/hide sidebar based on home tab and update URL hash
      (when-let [app-layout ($id "app-layout")]
                (if (= tab-id "home-pane")
                    (do
                     (add-class! app-layout "home-mode")
                     (add-class! js/document.body "home-active")
                     (hash/clear!))
                    (do
                     (remove-class! app-layout "home-mode")
                     (remove-class! js/document.body "home-active")
                     (let [current (hash/parse)
                           tab-name (tab-id->hash-name tab-id)
                           new-hash (if (and (= tab-id "graph-pane") (:node current))
                                        (hash/build {:tab tab-name :node (:node current)})
                                        tab-name)]
                          (.replaceState js/history nil "" (str "#" new-hash))))))
  ;; Render graph if switching to graph tab
      (when (and (= tab-id "graph-pane") (:config @state/app))
            (js/setTimeout render-graph! 50))
  ;; Render attribute graph if switching to attributes tab
      (when (and (= tab-id "attributes-pane") (:config @state/app))
            (js/setTimeout attributes/render-graph! 50))
  ;; Render planner graph if switching to planner tab
      (when (and (= tab-id "planner-pane") (:config @state/app))
            (js/setTimeout #(do (planner/populate-picker!)
                                (planner/render-graph!)) 50))
  ;; Render export if switching to export tab
      (when (and (= tab-id "export-pane") (:config @state/app))
            (js/setTimeout export/update-export! 50))
  ;; Render stats if switching to stats tab
      (when (and (= tab-id "stats-pane") (:config @state/app))
            (js/setTimeout stats/render-stats! 50)))

(defn setup-tabs! []
      (doseq [btn (array-seq (.querySelectorAll js/document ".nav-tab"))]
             (on! btn "click" #(switch-tab! (.getAttribute btn "data-tab")))))

;; Splash graph
(defn render-splash! []
      (when-let [container ($id "splash-graph")]
                (let [dot (core/generate-splash-dot {:dark? (dark?)})]
                     (-> (d3/select "#splash-graph")
                         (.graphviz)
                         (.width 260)
                         (.height 110)
                         (.fit true)
                         (.renderDot dot)))))

;; Main graph rendering (with DOT caching for snappy toggles)
(defn render-graph!
      ([] (render-graph! false))
      ([force?]
       (when-let [config (:config @state/app)]
                 (when-let [container ($id "graph")]
                           (let [cache-key (view-cache-key)
                                 last-key (:last-rendered-key @state/app)]
         ;; Skip re-render if already rendered with same view state (unless forced)
                                (when (or force? (not= cache-key last-key))
                                      (let [cached-dot (get (:dot-cache @state/app) cache-key)
                                            dot (or cached-dot
                                                    (let [d (core/generate-dot config {:dark? (dark?)
                                                                                       :show-datasources? (not (:pipelines-only? @state/app))
                                                                                       :collapsed-groups (:collapsed-groups @state/app)})]
                                                         (swap! state/app assoc-in [:dot-cache cache-key] d)
                                                         d))
                                            width (.-clientWidth container)
                                            gv (-> (d3/select "#graph")
                                                   (.graphviz)
                                                   (.width width)
                                                   (.height 650)
                                                   (.fit true)
                                                   (.transition (fn [] (-> (d3/transition "graph")
                                                                           (.duration 300)
                                                                           (.ease (.-easeCubicInOut d3))))))]
                                           (swap! state/app assoc :graphviz gv :last-rendered-key cache-key)
                                           (.renderDot gv dot)
                                           (.on gv "end" (fn []
                                                             (setup-interactivity!)
                                                             (select-node-from-hash!)))
                                           (render-group-toggles!))))))))

;; Node selection with full provenance highlighting
(defn select-node! [node-name]
      (swap! state/app assoc :selected-node node-name)
      (hash/update-node! node-name)
      (when-let [config (:config @state/app)]
                (let [;; Use cached lineage if available, otherwise compute
                      {:keys [upstream downstream]} (or (get (:lineage-cache @state/app) node-name)
                                                        (core/full-graph-lineage config node-name))
                      collapsed-groups (:collapsed-groups @state/app)
          ;; Map pipeline names to group nodes if collapsed
                      {:keys [pipeline->group]} (core/build-group-data config)
                      effective-name (fn [n]
                                         (if-let [g (pipeline->group n)]
                                                 (if (collapsed-groups g)
                                                     (str "group:" g)
                                                     n)
                                                 n))
          ;; Build set of connected nodes, mapping to group nodes where applicable
                      all-connected (set (concat
                                          (map #(effective-name (:name %)) upstream)
                                          (map #(effective-name (:name %)) downstream)))]

      ;; Highlight nodes
                     (-> (d3/select "#graph")
                         (.selectAll ".node")
                         (.each (fn [d]
                                    (this-as this
                                      (let [node (d3/select this)
                                            title (-> node (.select "title") (.text) str/trim)]
                                           (.classed node "node-highlighted" false)
                                           (.classed node "node-connected" false)
                                           (.classed node "node-dimmed" false)
                                           (cond
                                            (= title node-name)
                                            (.classed node "node-highlighted" true)

                                            (all-connected title)
                                            (.classed node "node-connected" true)

                                            :else
                                            (.classed node "node-dimmed" true)))))))

      ;; Dim edges not on provenance path (but don't change color of connected ones)
                     (-> (d3/select "#graph")
                         (.selectAll ".edge")
                         (.each (fn [d]
                                    (this-as this
                                      (let [edge (d3/select this)
                                            title (-> edge (.select "title") (.text) str/trim)
                                            match (re-find #"^(.+?)(?:->|--)\s*(.+?)$" title)]
                                           (.classed edge "edge-dimmed" false)
                                           (when match
                                                 (let [[_ source target] match
                                                       source-in-chain (or (= source node-name) (all-connected source))
                                                       target-in-chain (or (= target node-name) (all-connected target))]
                                                      (when-not (and source-in-chain target-in-chain)
                                                                (.classed edge "edge-dimmed" true)))))))))

      ;; Show details panel
                     (show-node-details! node-name upstream downstream))))

(defn select-node-from-hash! []
      (when-let [node-name (hash/get-node)]
                (if (str/starts-with? node-name "group:")
                    (select-group! (subs node-name 6))
                    (select-node! node-name))))

(defn clear-selection! []
      (swap! state/app assoc :selected-node nil)
      (hash/update-node! nil)
      (clear-graph-highlight! "#graph")
      (let [was-visible (.contains (.-classList ($id "details-col")) "visible")]
           (remove-class! ($id "details-col") "visible")
           ;; Re-render to fit new size after panel transition (200ms) completes
           (when was-visible
                 (js/setTimeout #(render-graph! true) 220))))

;; Group selection and details
(defn show-group-details! [group-name upstream downstream]
      (when-let [config (:config @state/app)]
                (let [{:keys [node]} (core/group-provenance config group-name)
                      members (:members node)
                      collapsed? (contains? (:collapsed-groups @state/app) group-name)
                      escaped-name (str/replace group-name "'" "\\'")
                      has-lineage (or (seq upstream) (seq downstream))
                      html (str
                            (detail-row "Type" (badge "Group" "" (styles/badge-style :group)))
                            "<div class='node-actions'>"
                            "<button class='graph-ctrl-btn danger' onclick=\"showBlastRadius('" escaped-name "')\">◉ Blast Radius</button>"
                            "</div>"
                            (detail-row (str "Pipelines (" (count members) ")") (badges members "badge-source"))
                            (when (:cluster node)
                                  (detail-row "Cluster" (badge (:cluster node) "badge-cluster")))
                            (when (seq (:inputs node))
                                  (detail-row "Input Sources" (badges (:inputs node) "badge-input")))
                            (when (seq (:outputs node))
                                  (detail-row "Output Sources" (badges (:outputs node) "badge-output")))
                            "<div class='group-toggle-section'>"
                            "<button class='graph-ctrl-btn' onclick='toggleGroup(\"" group-name "\")'>"
                            (if collapsed? "▶ Expand Group" "▼ Collapse Group") "</button></div>"
                ;; Split lineage by type (pipelines vs datasources)
                            (let [pipeline-names (set (map :name (:pipelines config)))
                                  upstream-pipelines (filter #(pipeline-names (:name %)) upstream)
                                  upstream-datasources (filter #(not (pipeline-names (:name %))) upstream)
                                  downstream-pipelines (filter #(pipeline-names (:name %)) downstream)
                                  downstream-datasources (filter #(not (pipeline-names (:name %))) downstream)]
                                 (str "<div class='lineage-section'><div class='lineage-header'>"
                                      "<span class='detail-label' style='margin-bottom:0'>Lineage</span></div>"
                                      "<div class='lineage-content'><div class='lineage-tree-view'>"
                                      (lineage-section "↑ Pipelines" upstream-pipelines)
                                      (lineage-section "↑ Datasources" upstream-datasources)
                                      (lineage-section "↓ Pipelines" downstream-pipelines)
                                      (lineage-section "↓ Datasources" downstream-datasources)
                                      (when-not has-lineage "<div class='detail-value text-muted'>No lineage connections</div>")
                                      "</div></div></div>")))]
                     (set-html! ($id "details-title") (str group-name " (Group)"))
                     (set-html! ($id "details-content") html)
      ;; Setup click handlers for lineage links
                     (doseq [item (array-seq (.querySelectorAll ($id "details-content") ".lineage-link"))]
                            (set! (.. item -style -cursor) "pointer")
                            (on! item "click" (fn [_] (select-node! (.getAttribute item "data-node-name")))))
                     (add-class! ($id "details-col") "visible"))))

(defn select-group! [group-name]
      (swap! state/app assoc :selected-node (str "group:" group-name))
      (hash/update-node! (str "group:" group-name))
      (when-let [config (:config @state/app)]
                (let [{:keys [upstream downstream]} (core/group-provenance config group-name)
                      {:keys [groups]} (core/build-group-data config)
                      members (set (:members (get groups group-name)))
                      collapsed? (contains? (:collapsed-groups @state/app) group-name)
                      all-connected (set (concat (map :name upstream) (map :name downstream)))
                      group-node-title (str "group:" group-name)]

      ;; Highlight nodes
                     (-> (d3/select "#graph")
                         (.selectAll ".node")
                         (.each (fn [d]
                                    (this-as this
                                      (let [node (d3/select this)
                                            title (-> node (.select "title") (.text) str/trim)]
                                           (.classed node "node-highlighted" false)
                                           (.classed node "node-connected" false)
                                           (.classed node "node-dimmed" false)
                                           (cond
                                ;; Highlight group node (collapsed) or members (expanded)
                                            (or (= title group-node-title)
                                                (and (not collapsed?) (members title)))
                                            (.classed node "node-highlighted" true)

                                            (all-connected title)
                                            (.classed node "node-connected" true)

                                            :else
                                            (.classed node "node-dimmed" true)))))))

      ;; Dim unrelated edges (but don't highlight - keep original colors)
                     (let [in-group? (fn [n] (or (= n group-node-title)
                                                 (and (not collapsed?) (members n))))]
                          (-> (d3/select "#graph")
                              (.selectAll ".edge")
                              (.each (fn [d]
                                         (this-as this
                                           (let [edge (d3/select this)
                                                 title (-> edge (.select "title") (.text) str/trim)
                                                 match (re-find #"^(.+?)(?:->|--)\s*(.+?)$" title)]
                                                (.classed edge "edge-dimmed" false)
                                                (.classed edge "edge-highlighted" false)
                                                (when match
                                                      (let [[_ source target] match
                                                            source-in-chain (or (in-group? source) (all-connected source))
                                                            target-in-chain (or (in-group? target) (all-connected target))]
                                                           (when-not (and source-in-chain target-in-chain)
                                                                     (.classed edge "edge-dimmed" true))))))))))

      ;; Show details panel
                     (show-group-details! group-name upstream downstream))))

;; Node details panel
(defn show-node-details! [node-name upstream downstream]
      (when-let [config (:config @state/app)]
                (let [pipeline (first (filter #(= (:name %) node-name) (:pipelines config)))
                      datasource (first (filter #(= (:name %) node-name) (:datasources config)))
                      node-data (or pipeline datasource)
                      node-type (cond pipeline "Pipeline" datasource "Data Source" :else "Node")
                      provenance-json (js/JSON.stringify (clj->js (core/node-provenance config node-name)) nil 2)
                      pipeline-group (:group pipeline)
                      has-lineage (or (seq upstream) (seq downstream))
                      escaped-name (str/replace node-name "'" "\\'")
                      html (str
                            (detail-row "Type" (badge node-type "badge-type"))
                            "<div class='node-actions'>"
                            "<button class='graph-ctrl-btn danger' onclick=\"showBlastRadius('" escaped-name "')\">◉ Blast Radius</button>"
                            "</div>"
                            (when-let [desc (:description node-data)]
                                      (detail-row "Description" desc))
                            (when-let [schedule (:schedule node-data)]
                                      (detail-row "Schedule" (str "<code>" (core/format-schedule schedule) "</code>")))
                            (when-let [ds-type (:type node-data)]
                                      (detail-row "Source Type" (badge (str/upper-case ds-type) "badge-source")))
                            (when (seq (:input_sources node-data))
                                  (detail-row "Input Sources" (badges (:input_sources node-data) "badge-source")))
                            (when (seq (:output_sources node-data))
                                  (detail-row "Output Sources" (badges (:output_sources node-data) "badge-source")))
                            (when (seq (:tags node-data))
                                  (detail-row "Tags" (str/join "" (map #(badge % "" (styles/badge-style :tag)) (:tags node-data)))))
                            (when (seq (:links node-data))
                                  (str "<div class='links-section'><div class='detail-label'>Links</div>"
                                       (str/join "" (map (fn [[k v]] (str "<a href='" v "' target='_blank' class='graph-ctrl-btn link-btn'>" (name k) "</a>"))
                                                         (:links node-data)))
                                       "</div>"))
                            (when pipeline-group
                                  (str "<div class='detail-label'>Group</div><div class='detail-value group-membership'>"
                                       (badge pipeline-group "" (styles/badge-style :group))
                                       "<button class='graph-ctrl-btn' onclick='toggleGroup(\"" pipeline-group "\")'>Collapse Group</button></div>"))
                ;; Lineage section
                ;; Split lineage by type (pipelines vs datasources)
                            (let [pipeline-names (set (map :name (:pipelines config)))
                                  upstream-pipelines (filter #(pipeline-names (:name %)) upstream)
                                  upstream-datasources (filter #(not (pipeline-names (:name %))) upstream)
                                  downstream-pipelines (filter #(pipeline-names (:name %)) downstream)
                                  downstream-datasources (filter #(not (pipeline-names (:name %))) downstream)]
                                 (str "<div class='lineage-section'><div class='lineage-header'>"
                                      "<span class='detail-label' style='margin-bottom:0'>Lineage</span>"
                                      (when has-lineage
                                            (str "<div class='lineage-view-toggle'><span class='lineage-toggle-label active' data-view='tree'>Tree</span>"
                                                 "<div class='lineage-toggle-slider'></div><span class='lineage-toggle-label' data-view='json'>JSON</span></div>"))
                                      "</div><div class='lineage-content'><div class='lineage-tree-view'>"
                                      (lineage-section "↑ Pipelines" upstream-pipelines true)
                                      (lineage-section "↑ Datasources" upstream-datasources true)
                                      (lineage-section "↓ Pipelines" downstream-pipelines true)
                                      (lineage-section "↓ Datasources" downstream-datasources true)
                                      (when-not has-lineage "<div class='detail-value text-muted'>No lineage connections</div>")))
                            "</div><div class='lineage-json-view hidden'>"
                            "<button class='json-copy-btn' title='Copy to clipboard'>" icon-copy "</button>"
                            "<pre class='lineage-json-pre'>" provenance-json "</pre>"
                            "</div></div></div>")]
                     (set-html! ($id "details-title") node-name)
                     (set-html! ($id "details-content") html)
                     (setup-view-toggle! ($id "details-content"))
                     (when-let [copy-btn (.querySelector ($id "details-content") ".json-copy-btn")]
                               (setup-copy-btn! copy-btn provenance-json))
                     (doseq [item (array-seq (.querySelectorAll ($id "details-content") ".lineage-link"))]
                            (on! item "click" (fn [_] (select-node! (.getAttribute item "data-node-name")))))
                     (add-class! ($id "details-col") "visible"))))

(defn setup-interactivity! []
  ;; Node click handlers
      (-> (d3/select "#graph")
          (.selectAll ".node")
          (.style "cursor" "pointer")
          (.on "click" (fn [event d]
                           (this-as this
                             (.stopPropagation event)
                             (let [node-name (-> (d3/select this) (.select "title") (.text))]
                                  (if (str/starts-with? node-name "group:")
                                      (select-group! (subs node-name 6))
                                      (select-node! node-name)))))))
  ;; Click background to clear (if not clicking on a node)
      (-> (d3/select "#graph svg")
          (.on "click" (fn [event]
                           (let [target (.-target event)]
                                (when-not (.closest target ".node")
                                          (clear-selection!)))))))

;; Graph controls
(defn reset-graph! []
      (when-let [gv (:graphviz @state/app)]
                (.resetZoom gv))
      (clear-selection!))

(defn toggle-pipelines-only! []
      (swap! state/app assoc :pipelines-only? (not (:pipelines-only? @state/app)) :last-rendered-key nil)
      (toggle-class! ($id "pipelines-only-btn") "active")
      (render-graph!))

(defn toggle-group! [group-name]
      (let [selected (:selected-node @state/app)]
           (swap! state/app (fn [s]
                                (-> s
                                    (update :collapsed-groups
                                            (fn [groups]
                                                (if (groups group-name)
                                                    (disj groups group-name)
                                                    (conj groups group-name))))
                                    (assoc :last-rendered-key nil))))
           (render-graph!)
    ;; Re-apply selection after transition completes (300ms + buffer)
           (when selected
                 (js/setTimeout
                  #(if (str/starts-with? selected "group:")
                       (select-group! (subs selected 6))
                       (select-node! selected))
                  350))))

(defn toggle-all-groups! []
      (when-let [config (:config @state/app)]
                (let [all-groups (set (core/all-groups config))
                      collapsed (:collapsed-groups @state/app)
                      all-collapsed? (= all-groups collapsed)
                      selected (:selected-node @state/app)]
                     (swap! state/app assoc
                            :collapsed-groups (if all-collapsed? #{} all-groups)
                            :last-rendered-key nil)
                     (render-graph!)
                     (when selected
                           (js/setTimeout
                            #(if (str/starts-with? selected "group:")
                                 (select-group! (subs selected 6))
                                 (select-node! selected))
                            350)))))

(defn render-group-toggles! []
      (when-let [config (:config @state/app)]
                (when-let [container ($id "group-toggles")]
                          (let [groups (core/all-groups config)
                                collapsed (:collapsed-groups @state/app)
                                all-collapsed? (= (set groups) collapsed)]
                               (if (empty? groups)
                                   (set-html! container "")
                                   (set-html! container
                                              (str "<button class='graph-ctrl-btn' onclick='toggleAllGroups()'>"
                                                   (if all-collapsed? "Expand All" "Collapse All")
                                                   "</button>")))))))

;; Search functionality
(defn select-search-result! [node-name]
      (let [dropdown ($id "graph-search-results")
            input ($id "graph-search")]
           (remove-class! dropdown "show")
           (set! (.-value input) "")
    ;; Select the node directly - select-node! handles highlighting
           (select-node! node-name)))

(defn render-search-results! [results query]
      (let [dropdown ($id "graph-search-results")]
           (if (empty? results)
               (do
                (set-html! dropdown "<div class=\"search-result-item no-match\">No matches found</div>")
                (add-class! dropdown "show"))
               (do
                (set-html! dropdown
                           (str/join ""
                                     (map-indexed
                                      (fn [idx {:keys [name type]}]
                                          (str "<div class=\"search-result-item"
                                               (when (zero? idx) " selected")
                                               "\" data-name=\"" name "\">"
                                               "<span class=\"result-type " (clojure.core/name type) "\">"
                                               (if (= type :pipeline) "Pipeline" "Source")
                                               "</span>"
                                               "<span class=\"result-name\">"
                                               (core/highlight-match name query)
                                               "</span></div>"))
                                      results)))
                (add-class! dropdown "show")
        ;; Add click handlers
                (doseq [item (array-seq (.querySelectorAll dropdown ".search-result-item[data-name]"))]
                       (on! item "click" (fn [_]
                                             (select-search-result! (.getAttribute item "data-name")))))))))

(defn handle-search-key! [e]
      (let [dropdown ($id "graph-search-results")
            items (array-seq (.querySelectorAll dropdown ".search-result-item[data-name]"))
            key (.-key e)]
           (cond
      ;; Arrow navigation
            (or (= key "ArrowDown") (= key "ArrowUp"))
            (do
             (.preventDefault e)
             (when (seq items)
                   (let [current (.querySelector dropdown ".search-result-item.selected")
                         current-idx (if current
                                         (.indexOf (js/Array.from items) current)
                                         -1)
                         new-idx (cond
                                  (= key "ArrowDown")
                                  (if (< current-idx (dec (count items)))
                                      (inc current-idx)
                                      0)
                                  :else
                                  (if (> current-idx 0)
                                      (dec current-idx)
                                      (dec (count items))))]
                        (doseq [item items]
                               (remove-class! item "selected"))
                        (when-let [target (nth items new-idx nil)]
                                  (add-class! target "selected")
                                  (.scrollIntoView target #js {:block "nearest"})))))

      ;; Enter to select
            (= key "Enter")
            (when-let [selected (.querySelector dropdown ".search-result-item.selected")]
                      (when-let [name (.getAttribute selected "data-name")]
                                (select-search-result! name)))

      ;; Escape to close
            (= key "Escape")
            (do
             (remove-class! dropdown "show")
             (set-html! dropdown ""))

      ;; Text input - search
            :else
            (let [query (str/trim (.-value (.-target e)))]
                 (if (or (empty? query) (nil? (:config @state/app)))
                     (do
                      (remove-class! dropdown "show")
                      (set-html! dropdown ""))
                     (let [results (core/search-nodes (:config @state/app) query)]
                          (render-search-results! results query)))))))

(defn setup-search! []
      (when-let [input ($id "graph-search")]
                (on! input "keyup" handle-search-key!)
                (on! input "keydown" (fn [e]
                                         (when (or (= (.-key e) "ArrowDown")
                                                   (= (.-key e) "ArrowUp"))
                                               (.preventDefault e))))))

;; File handling
(defn load-config! [json-str]
      (try
       (let [config (js->clj (js/JSON.parse json-str) :keywordize-keys true)
             {:keys [valid? errors]} (core/validate-config config)]
            (if valid?
                (do
                 (set-config! config)
                 (precompute-all! config)
                 (update-json-input! config)
                 (render-tables!)
                 (switch-tab! "graph-pane"))
                (js/console.error "Invalid config:" (clj->js errors))))
       (catch :default e
              (js/console.error "Failed to parse config:" e))))

(defn- read-file! [file]
       (let [reader (js/FileReader.)]
            (set! (.-onload reader) #(load-config! (-> % .-target .-result)))
            (.readAsText reader file)))

(defn handle-file-drop [e]
      (.preventDefault e)
      (some-> e .-dataTransfer .-files (aget 0) read-file!))

(defn handle-file-select [e]
      (some-> e .-target .-files (aget 0) read-file!))

;; Setup
(defn setup-drop-zone! []
      (when-let [drop-zone ($id "drop-zone")]
                (on! drop-zone "dragover" (fn [e]
                                              (.preventDefault e)
                                              (add-class! drop-zone "dragover")))
                (on! drop-zone "dragleave" #(remove-class! drop-zone "dragover"))
                (on! drop-zone "drop" (fn [e]
                                          (remove-class! drop-zone "dragover")
                                          (handle-file-drop e)))
                (on! drop-zone "click" #(.click ($id "file-input"))))
      (when-let [input ($id "file-input")]
                (on! input "change" handle-file-select)))

(defn update-json-input! [config]
      (when-let [input ($id "json-input")]
                (set! (.-value input) (js/JSON.stringify (clj->js config) nil 2))))

(defn show-json-status! [msg error?]
      (when-let [status ($id "json-status")]
                (set-html! status msg)
                (remove-class! status "error")
                (remove-class! status "success")
                (add-class! status (if error? "error" "success"))))

(defn- active-tab []
       (some-> ($id "main-tabs") (.querySelector ".nav-tab.active") (.getAttribute "data-tab")))

(defn load-example! []
      (set-config! core/example-config)
      (precompute-all! core/example-config)
      (update-json-input! core/example-config)
      (render-tables!)
      (show-json-status! "Example loaded" false)
      (when-not (= "config-pane" (active-tab))
                (switch-tab! "graph-pane")))

(defn apply-config! []
      (when-let [input ($id "json-input")]
                (let [json-str (.-value input)]
                     (if (empty? json-str)
                         (show-json-status! "Please enter some JSON" true)
                         (try
                          (let [config (js->clj (js/JSON.parse json-str) :keywordize-keys true)
                                {:keys [valid? errors]} (core/validate-config config)]
                               (if valid?
                                   (do
                                    (set-config! config)
                                    (precompute-all! config)
                                    (render-tables!)
                                    (show-json-status! "Config applied successfully" false)
                                    (switch-tab! "graph-pane"))
                                   (show-json-status! (str "Invalid config: " (first errors)) true)))
                          (catch :default e
                                 (show-json-status! (str "JSON parse error: " (.-message e)) true)))))))

(defn format-json! []
      (when-let [input ($id "json-input")]
                (let [json-str (.-value input)]
                     (when (seq json-str)
                           (try
                            (let [parsed (js/JSON.parse json-str)
                                  formatted (js/JSON.stringify parsed nil 2)]
                                 (set! (.-value input) formatted)
                                 (show-json-status! "Formatted" false))
                            (catch :default e
                                   (show-json-status! (str "Cannot format - invalid JSON: " (.-message e)) true)))))))

;; Table rendering - delegated to tables module
(def render-tables! tables/render-all!)
(def setup-table-search! tables/setup-search!)

;; Handle hashchange for browser back/forward navigation
(defn setup-hashchange-listener! []
      (.addEventListener js/window "hashchange"
                         (fn [_]
                             (let [{:keys [tab node blast view pipelines]} (hash/parse)
                                   current-selected (:selected-node @state/app)
                                   modal-visible? (when-let [m ($id "blast-radius-modal")]
                                                            (.contains (.-classList m) "show"))]
                                  (cond
                           ;; Blast in hash - show blast radius modal
                                   (and blast (not modal-visible?))
                                   (do
                                    (switch-tab! "graph-pane")
                                    (js/setTimeout #(blast/show! blast) 100))

                           ;; No blast in hash but modal is visible - close it
                                   (and (nil? blast) modal-visible?)
                                   (blast/hide-modal!)

                           ;; Planner tab with state
                                   (and (= tab "planner") (or view pipelines))
                                   (do
                                    (switch-tab! "planner-pane")
                                    (js/setTimeout planner/restore-from-hash! 100))

                           ;; Node in hash but different from current selection
                                   (and node (not= node current-selected))
                                   (do
                                    (switch-tab! "graph-pane")
                                    (js/setTimeout select-node-from-hash! 100))

                           ;; No node in hash but we have a selection - clear it
                                   (and (nil? node) current-selected)
                                   (clear-selection!))))))

;; Load config from URL
(defn load-from-url! [url]
      (js/console.log "Loading config from URL:" url)
      (-> (js/fetch url)
          (.then (fn [response]
                     (if (.-ok response)
                         (.json response)
                         (throw (js/Error. (str "Failed to fetch: " (.-status response)))))))
          (.then (fn [json]
                     (let [config (js->clj json :keywordize-keys true)]
                          (set-config! config)
                          (precompute-all! config)
                          (update-json-input! config)
                          (render-tables!)
                          ;; Handle hash state after config loads
                          (let [{:keys [tab node blast view pipelines]} (hash/parse)]
                               (cond
                                blast (do (switch-tab! "graph-pane")
                                          (js/setTimeout #(blast/show! blast) 200))
                                node (do (switch-tab! "graph-pane")
                                         (js/setTimeout select-node-from-hash! 200))
                                (and (= tab "planner") (or view pipelines))
                                (do (switch-tab! "planner-pane")
                                    (js/setTimeout planner/restore-from-hash! 200))
                                (and tab (not= tab "home"))
                                (switch-tab! (str tab "-pane"))
                                ;; Default to graph when loading from URL
                                :else
                                (switch-tab! "graph-pane"))))))
          (.catch (fn [err]
                      (js/console.error "Failed to load config from URL:" err)
                      ;; Fall back to example config
                      (set-config! core/example-config)
                      (precompute-all! core/example-config)
                      (update-json-input! core/example-config)
                      (render-tables!)))))

;; Init
(defn init []
      (js/console.log "Pipeviz ClojureScript initialized")
      (init-theme!)
      (setup-tabs!)
      (setup-drop-zone!)
      (setup-search!)
      (setup-table-search!)
      (attributes/setup-search!)
      (setup-hashchange-listener!)
      (render-splash!)
      (planner/init!)
      ;; Check for ?url= parameter
      (if-let [url (hash/get-url-param)]
              (load-from-url! url)
              ;; Load example by default
              (do
               (set-config! core/example-config)
               (precompute-all! core/example-config)
               (update-json-input! core/example-config)
               (render-tables!)
               ;; Handle hash state on page load
               (let [{:keys [tab node blast view pipelines]} (hash/parse)]
                    (cond
                     ;; If there's a blast in the hash, show blast radius modal
                     blast
                     (do
                      (switch-tab! "graph-pane")
                      (js/setTimeout #(blast/show! blast) 200))

                     ;; If there's a node in the hash, switch to graph and select it
                     node
                     (do
                      (switch-tab! "graph-pane")
                      (js/setTimeout select-node-from-hash! 200))

                     ;; If it's the planner tab with view/pipelines, restore state
                     (and (= tab "planner") (or view pipelines))
                     (do
                      (switch-tab! "planner-pane")
                      (js/setTimeout planner/restore-from-hash! 200))

                     ;; If there's a specific tab in the hash, switch to it
                     (and tab (not= tab "home"))
                     (switch-tab! (str tab "-pane")))))))

(defn reload []
      (when (:config @state/app)
            (render-graph! true)) ;; force re-render
      (render-splash!))

;; Expose to window for HTML onclick handlers
(js/Object.assign js/window
                  #js {:toggleTheme toggle-theme!
                       :toggleSidebar toggle-sidebar!
                       :loadExample load-example!
                       :resetGraph reset-graph!
                       :togglePipelinesOnly toggle-pipelines-only!
                       :toggleGroup toggle-group!
                       :toggleAllGroups toggle-all-groups!
                       :switchTab switch-tab!
                       :applyConfig apply-config!
                       :formatJson format-json!
                       :resetAttributeGraph attributes/reset-graph!
                       :showBlastRadius blast/show!
                       :hideBlastRadiusModal blast/hide-modal!
                       :togglePlannerPicker planner/toggle-picker!
                       :filterPlannerItems planner/filter-items!
                       :clearPlannerSelection planner/clear-selection!
                       :setPlannerView planner/set-view!
                       :copyPlannerOutput planner/copy-output!
                       :setExportFormat export/set-format!
                       :copyExport export/copy-export!})
