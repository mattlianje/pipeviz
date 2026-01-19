(ns pipeviz.core
  "Browser entry point - DOM and d3 interop only"
  (:require [pipeviz.graph :as graph]
            [clojure.string :as str]
            ["d3" :as d3]
            ["d3-graphviz" :as d3-graphviz]))

;; Ensure d3-graphviz side-effects load
(def ^:private _ d3-graphviz)

(declare dark? render-splash! render-graph! setup-interactivity! show-node-details! render-tables! update-json-input! render-attribute-graph! precompute-attribute-lineage! select-attribute! show-datasource-in-attribute-panel! render-group-toggles! select-group! show-group-details! select-node-from-hash!)

;; State
(defonce state (atom {:config nil
                      :selected-node nil
                      :graphviz nil
                      :dark? false
                      :pipelines-only? false
                      :lineage-cache nil
                      :collapsed-groups #{}
                      :dot-cache {}
                      :last-rendered-key nil}))

;; Filter state
(defonce filter-state (atom {:pipeline-tags #{}
                             :pipeline-clusters #{}
                             :datasource-types #{}
                             :datasource-tags #{}
                             :datasource-clusters #{}}))

;; View state cache key for snappy toggling
(defn- view-cache-key []
  (str (dark?) "|" (:pipelines-only? @state) "|" (pr-str (:collapsed-groups @state))))

;; Set config and initialize groups as collapsed by default
(defn set-config! [config]
  (let [all-groups (set (graph/all-groups config))]
    (swap! state assoc
           :config config
           :collapsed-groups all-groups
           :dot-cache {}
           :last-rendered-key nil)))

;; Precompute lineage for all nodes
(defn precompute-lineage! [config]
  (let [adjacency (graph/build-full-adjacency config)
        all-nodes (distinct (concat
                             (map :name (:pipelines config))
                             (map :name (:datasources config))
                             (keys (:downstream adjacency))
                             (keys (:upstream adjacency))))
        cache (into {}
                    (for [node all-nodes]
                      [node (graph/full-graph-lineage config node)]))]
    (swap! state assoc :lineage-cache cache)
    cache))

;; DOM helpers
(defn $id [id] (.getElementById js/document id))
(defn on! [el event f] (when el (.addEventListener el event f)))
(defn set-html! [el html] (when el (set! (.-innerHTML el) html)))
(defn add-class! [el cls] (when el (.add (.-classList el) cls)))
(defn remove-class! [el cls] (when el (.remove (.-classList el) cls)))
(defn toggle-class! [el cls] (when el (.toggle (.-classList el) cls)))

(defn dark? []
  (= "dark" (.getAttribute js/document.documentElement "data-theme")))

;; URL hash helpers for direct node linking
(defn parse-hash []
  (let [hash (subs (or (.-hash js/window.location) "") 1)
        parts (str/split hash #"&")]
    (into {}
          (for [part parts
                :when (str/includes? part "=")]
            (let [[k v] (str/split part #"=" 2)]
              [(keyword k) (js/decodeURIComponent v)])))))

(defn get-node-from-hash []
  (:node (parse-hash)))

(defn update-hash-with-node [node-name]
  (let [current (parse-hash)
        updated (if node-name
                  (assoc current :node node-name)
                  (dissoc current :node))
        hash-str (str/join "&" (for [[k v] updated]
                                 (str (name k) "=" (js/encodeURIComponent v))))]
    (set! (.-hash js/window.location) hash-str)))

;; Theme
(defn set-theme! [dark?]
  (.setAttribute js/document.documentElement "data-theme" (if dark? "dark" "light"))
  (js/localStorage.setItem "theme" (if dark? "dark" "light"))
  (when-let [btn ($id "theme-toggle")]
    (set! (.-textContent btn) (if dark? "☀️" "🌙")))
  (swap! state assoc :dark? dark? :last-rendered-key nil) ;; invalidate cache on theme change
  (when ($id "splash-graph")
    (render-splash!))
  (when (:config @state)
    (render-graph!)))

(defn toggle-theme! []
  (set-theme! (not (dark?))))

(defn init-theme! []
  (let [stored (js/localStorage.getItem "theme")
        prefers-dark (.-matches (js/window.matchMedia "(prefers-color-scheme: dark)"))
        use-dark (if stored (= stored "dark") prefers-dark)]
    (set-theme! use-dark)))

;; Tabs
(defn switch-tab! [tab-id]
  ;; Update tab buttons
  (doseq [btn (array-seq (.querySelectorAll js/document ".nav-tab"))]
    (if (= tab-id (.getAttribute btn "data-tab"))
      (add-class! btn "active")
      (remove-class! btn "active")))
  ;; Update panes
  (doseq [pane (array-seq (.querySelectorAll js/document ".tab-pane"))]
    (if (= tab-id (.-id pane))
      (add-class! pane "active")
      (remove-class! pane "active")))
  ;; Show/hide tabs based on home page
  (when-let [tabs ($id "main-tabs")]
    (if (= tab-id "home-pane")
      (add-class! tabs "hidden-on-home")
      (remove-class! tabs "hidden-on-home")))
  ;; Render graph if switching to graph tab
  (when (and (= tab-id "graph-pane") (:config @state))
    (js/setTimeout render-graph! 50))
  ;; Render attribute graph if switching to attributes tab
  (when (and (= tab-id "attributes-pane") (:config @state))
    (js/setTimeout render-attribute-graph! 50)))

(defn setup-tabs! []
  (doseq [btn (array-seq (.querySelectorAll js/document ".nav-tab"))]
    (on! btn "click" #(switch-tab! (.getAttribute btn "data-tab")))))

;; Splash graph
(defn render-splash! []
  (when-let [container ($id "splash-graph")]
    (let [dot (graph/generate-splash-dot {:dark? (dark?)})]
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
   (when-let [config (:config @state)]
     (when-let [container ($id "graph")]
       (let [cache-key (view-cache-key)
             last-key (:last-rendered-key @state)]
         ;; Skip re-render if already rendered with same view state (unless forced)
         (when (or force? (not= cache-key last-key))
           (let [cached-dot (get (:dot-cache @state) cache-key)
                 dot (or cached-dot
                         (let [d (graph/generate-dot config {:dark? (dark?)
                                                             :show-datasources? (not (:pipelines-only? @state))
                                                             :collapsed-groups (:collapsed-groups @state)})]
                           (swap! state assoc-in [:dot-cache cache-key] d)
                           d))
                 gv (-> (d3/select "#graph")
                        (.graphviz)
                        (.width (.-clientWidth container))
                        (.height 500)
                        (.fit true)
                        (.transition (fn [] (-> (d3/transition "graph")
                                                (.duration 300)
                                                (.ease (.-easeCubicInOut d3))))))]
             (swap! state assoc :graphviz gv :last-rendered-key cache-key)
             (.renderDot gv dot)
             (.on gv "end" (fn []
                             (setup-interactivity!)
                             (select-node-from-hash!)))
             (render-group-toggles!))))))))

;; Node selection with full provenance highlighting
(defn select-node! [node-name]
  (swap! state assoc :selected-node node-name)
  (update-hash-with-node node-name)
  (when-let [config (:config @state)]
    (let [;; Use cached lineage if available, otherwise compute
          {:keys [upstream downstream]} (or (get (:lineage-cache @state) node-name)
                                            (graph/full-graph-lineage config node-name))
          collapsed-groups (:collapsed-groups @state)
          ;; Map pipeline names to group nodes if collapsed
          {:keys [pipeline->group]} (graph/build-group-data config)
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
  (when-let [node-name (get-node-from-hash)]
    (if (str/starts-with? node-name "group:")
      (select-group! (subs node-name 6))
      (select-node! node-name))))

(defn clear-selection! []
  (swap! state assoc :selected-node nil)
  (update-hash-with-node nil)
  ;; Clear node classes
  (-> (d3/select "#graph")
      (.selectAll ".node")
      (.classed "node-highlighted" false)
      (.classed "node-connected" false)
      (.classed "node-dimmed" false))
  ;; Clear edge dimming
  (-> (d3/select "#graph")
      (.selectAll ".edge")
      (.classed "edge-dimmed" false))
  ;; Hide details panel and re-render graph to fit new container size
  (let [was-visible (.contains (.-classList ($id "details-col")) "visible")]
    (remove-class! ($id "graph-col") "with-details")
    (remove-class! ($id "details-col") "visible")
    (when was-visible
      (js/setTimeout render-graph! 50))))

;; Group selection and details
(defn show-group-details! [group-name upstream downstream]
  (when-let [config (:config @state)]
    (let [{:keys [node]} (graph/group-provenance config group-name)
          members (:members node)
          collapsed? (contains? (:collapsed-groups @state) group-name)]
      ;; Update title
      (set-html! ($id "details-title") (str group-name " (Group)"))

      ;; Build details HTML
      (let [has-lineage (or (seq upstream) (seq downstream))
            html (str
                  "<div class='detail-label'>Type</div>"
                  "<div class='detail-value'><span class='badge' style='background:#fff3e0;color:#e65100'>Group</span></div>"

                  "<div class='detail-label'>Pipelines (" (count members) ")</div>"
                  "<div class='detail-value'>"
                  (str/join "" (map #(str "<span class='badge badge-source'>" % "</span>") members))
                  "</div>"

                  (when (:cluster node)
                    (str "<div class='detail-label'>Cluster</div>"
                         "<div class='detail-value'><span class='badge badge-cluster'>" (:cluster node) "</span></div>"))

                  (when (seq (:inputs node))
                    (str "<div class='detail-label'>Input Sources</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map #(str "<span class='badge badge-input'>" % "</span>") (:inputs node)))
                         "</div>"))

                  (when (seq (:outputs node))
                    (str "<div class='detail-label'>Output Sources</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map #(str "<span class='badge badge-output'>" % "</span>") (:outputs node)))
                         "</div>"))

                  ;; Expand/Collapse button
                  "<div style='margin: 1rem 0'>"
                  "<button class='graph-ctrl-btn' onclick='toggleGroup(\"" group-name "\")'>"
                  (if collapsed? "▶ Expand Group" "▼ Collapse Group")
                  "</button>"
                  "</div>"

                  ;; Lineage section
                  "<div class='lineage-section'>"
                  "<div class='lineage-header'>"
                  "<span class='detail-label' style='margin-bottom:0'>Lineage</span>"
                  "</div>"
                  "<div class='lineage-content'>"
                  "<div class='lineage-tree-view'>"

                  (when (seq upstream)
                    (str "<div class='detail-label'>Upstream (" (count upstream) ")</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map (fn [{:keys [name depth]}]
                                             (let [indent (* (dec depth) 12)
                                                   opacity (max 0.5 (- 1 (* (dec depth) 0.15)))]
                                               (str "<div class='lineage-link' data-node-name='" name
                                                    "' style='padding-left:" indent "px;opacity:" opacity "'>"
                                                    name "</div>")))
                                           (sort-by :depth upstream)))
                         "</div>"))

                  (when (seq downstream)
                    (str "<div class='detail-label'>Downstream (" (count downstream) ")</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map (fn [{:keys [name depth]}]
                                             (let [indent (* (dec depth) 12)
                                                   opacity (max 0.5 (- 1 (* (dec depth) 0.15)))]
                                               (str "<div class='lineage-link' data-node-name='" name
                                                    "' style='padding-left:" indent "px;opacity:" opacity "'>"
                                                    name "</div>")))
                                           (sort-by :depth downstream)))
                         "</div>"))

                  (when-not has-lineage
                    "<div class='detail-value text-muted'>No lineage connections</div>")

                  "</div></div></div>")]
        (set-html! ($id "details-content") html))

      ;; Setup click handlers for lineage links
      (doseq [item (array-seq (.querySelectorAll ($id "details-content") ".lineage-link"))]
        (set! (.. item -style -cursor) "pointer")
        (on! item "click" (fn [_]
                            (let [target-name (.getAttribute item "data-node-name")]
                              (select-node! target-name)))))

      ;; Show panel
      (add-class! ($id "graph-col") "with-details")
      (add-class! ($id "details-col") "visible"))))

(defn select-group! [group-name]
  (swap! state assoc :selected-node (str "group:" group-name))
  (update-hash-with-node (str "group:" group-name))
  (when-let [config (:config @state)]
    (let [{:keys [upstream downstream]} (graph/group-provenance config group-name)
          {:keys [groups]} (graph/build-group-data config)
          members (set (:members (get groups group-name)))
          collapsed? (contains? (:collapsed-groups @state) group-name)
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

      ;; Dim unrelated edges
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
                                    (if (and source-in-chain target-in-chain)
                                      (.classed edge "edge-highlighted" true)
                                      (.classed edge "edge-dimmed" true))))))))))

      ;; Show details panel
      (show-group-details! group-name upstream downstream))))

;; Node details panel
(defn show-node-details! [node-name upstream downstream]
  (when-let [config (:config @state)]
    (let [pipeline (first (filter #(= (:name %) node-name) (:pipelines config)))
          datasource (first (filter #(= (:name %) node-name) (:datasources config)))
          node-data (or pipeline datasource)
          node-type (cond pipeline "Pipeline" datasource "Data Source" :else "Node")
          provenance (graph/node-provenance config node-name)
          provenance-json (js/JSON.stringify (clj->js provenance) nil 2)
          ;; Check if pipeline belongs to a group
          pipeline-group (:group pipeline)]

      ;; Update title
      (set-html! ($id "details-title") node-name)

      ;; Build details HTML
      (let [has-lineage (or (seq upstream) (seq downstream))
            html (str
                  ;; Node info section
                  "<div class='detail-label'>Type</div>"
                  "<div class='detail-value'><span class='badge badge-type'>" node-type "</span></div>"

                  (when-let [desc (:description node-data)]
                    (str "<div class='detail-label'>Description</div>"
                         "<div class='detail-value'>" desc "</div>"))

                  (when-let [schedule (:schedule node-data)]
                    (str "<div class='detail-label'>Schedule</div>"
                         "<div class='detail-value'><code>" (graph/format-schedule schedule) "</code></div>"))

                  (when-let [ds-type (:type node-data)]
                    (str "<div class='detail-label'>Source Type</div>"
                         "<div class='detail-value'><span class='badge badge-source'>" (str/upper-case ds-type) "</span></div>"))

                  (when (seq (:input_sources node-data))
                    (str "<div class='detail-label'>Input Sources</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map #(str "<span class='badge badge-source'>" % "</span>") (:input_sources node-data)))
                         "</div>"))

                  (when (seq (:output_sources node-data))
                    (str "<div class='detail-label'>Output Sources</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map #(str "<span class='badge badge-source'>" % "</span>") (:output_sources node-data)))
                         "</div>"))

                  (when (seq (:tags node-data))
                    (str "<div class='detail-label'>Tags</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map #(str "<span class='badge' style='background-color:#fff3cd;color:#856404'>" % "</span>") (:tags node-data)))
                         "</div>"))

                  (when (and (:links node-data) (seq (:links node-data)))
                    (str "<div class='links-section'>"
                         "<div class='detail-label'>Links</div>"
                         (str/join ""
                                   (map (fn [[k v]]
                                          (str "<a href='" v "' target='_blank' class='graph-ctrl-btn link-btn'>" (name k) "</a>"))
                                        (:links node-data)))
                         "</div>"))

                  ;; Group membership with collapse button
                  (when pipeline-group
                    (str "<div class='detail-label'>Group</div>"
                         "<div class='detail-value'>"
                         "<span class='badge' style='background:#fff3e0;color:#e65100'>" pipeline-group "</span>"
                         "<button class='graph-ctrl-btn' style='margin-left:8px' onclick='toggleGroup(\"" pipeline-group "\")'>Collapse Group</button>"
                         "</div>"))

                  ;; Lineage section with header
                  "<div class='lineage-section'>"
                  "<div class='lineage-header'>"
                  "<span class='detail-label' style='margin-bottom:0'>Lineage</span>"
                  (when has-lineage
                    (str "<div class='lineage-view-toggle'>"
                         "<span class='lineage-toggle-label active' data-view='tree'>Tree</span>"
                         "<div class='lineage-toggle-slider'></div>"
                         "<span class='lineage-toggle-label' data-view='json'>JSON</span>"
                         "</div>"))
                  "</div>"
                  "<div class='lineage-content'>"

                  ;; Tree view
                  "<div class='lineage-tree-view'>"
                  (when (seq upstream)
                    (str "<div class='detail-label'>Upstream (" (count upstream) ")</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map (fn [{:keys [name depth]}]
                                             (let [indent (* (dec depth) 12)
                                                   opacity (max 0.5 (- 1 (* (dec depth) 0.15)))
                                                   prefix (if (> depth 1) "└ " "")]
                                               (str "<div class='lineage-link' data-node-name='" name
                                                    "' style='padding-left:" indent "px;opacity:" opacity "'>"
                                                    prefix name "</div>")))
                                           (sort-by :depth upstream)))
                         "</div>"))

                  (when (seq downstream)
                    (str "<div class='detail-label'>Downstream (" (count downstream) ")</div>"
                         "<div class='detail-value'>"
                         (str/join "" (map (fn [{:keys [name depth]}]
                                             (let [indent (* (dec depth) 12)
                                                   opacity (max 0.5 (- 1 (* (dec depth) 0.15)))
                                                   prefix (if (> depth 1) "└ " "")]
                                               (str "<div class='lineage-link' data-node-name='" name
                                                    "' style='padding-left:" indent "px;opacity:" opacity "'>"
                                                    prefix name "</div>")))
                                           (sort-by :depth downstream)))
                         "</div>"))

                  (when-not has-lineage
                    "<div class='detail-value text-muted'>No lineage connections</div>")
                  "</div>"

                  ;; JSON view (starts hidden)
                  "<div class='lineage-json-view hidden'>"
                  "<button class='json-copy-btn' title='Copy to clipboard'>"
                  "<svg width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2'>"
                  "<rect x='9' y='9' width='13' height='13' rx='2' ry='2'/>"
                  "<path d='M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1'/>"
                  "</svg></button>"
                  "<pre class='lineage-json-pre'>" provenance-json "</pre>"
                  "</div>"
                  "</div>" ;; close lineage-content
                  "</div>")]

        (set-html! ($id "details-content") html)

        ;; Add toggle handler (use classes for visibility to prevent layout jump)
        (when-let [slider (.querySelector ($id "details-content") ".lineage-toggle-slider")]
          (on! slider "click" (fn [_]
                                (let [tree-view (.querySelector ($id "details-content") ".lineage-tree-view")
                                      json-view (.querySelector ($id "details-content") ".lineage-json-view")
                                      tree-label (.querySelector ($id "details-content") "[data-view='tree']")
                                      json-label (.querySelector ($id "details-content") "[data-view='json']")
                                      is-json (.contains (.-classList slider) "json-active")]
                                  (if is-json
                                    (do
                                      (remove-class! slider "json-active")
                                      (remove-class! tree-view "hidden")
                                      (add-class! json-view "hidden")
                                      (add-class! tree-label "active")
                                      (remove-class! json-label "active"))
                                    (do
                                      (add-class! slider "json-active")
                                      (add-class! tree-view "hidden")
                                      (remove-class! json-view "hidden")
                                      (remove-class! tree-label "active")
                                      (add-class! json-label "active")))))))

        ;; Add copy button handler
        (when-let [copy-btn (.querySelector ($id "details-content") ".json-copy-btn")]
          (let [copy-icon "<svg width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2'><rect x='9' y='9' width='13' height='13' rx='2' ry='2'/><path d='M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1'/></svg>"
                check-icon "<svg width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2'><polyline points='20 6 9 17 4 12'/></svg>"]
            (on! copy-btn "click" (fn [_]
                                    (-> (js/navigator.clipboard.writeText provenance-json)
                                        (.then (fn []
                                                 (add-class! copy-btn "copied")
                                                 (set! (.-innerHTML copy-btn) check-icon)
                                                 (js/setTimeout #(do
                                                                   (remove-class! copy-btn "copied")
                                                                   (set! (.-innerHTML copy-btn) copy-icon))
                                                                1500))))))))

        ;; Add click handlers for lineage links
        (doseq [item (array-seq (.querySelectorAll ($id "details-content") ".lineage-link"))]
          (on! item "click" (fn [_]
                              (let [target-name (.getAttribute item "data-node-name")]
                                (select-node! target-name))))))

      ;; Show panel
      (add-class! ($id "graph-col") "with-details")
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
  ;; Click background to clear
  (-> (d3/select "#graph svg")
      (.on "click" (fn [event]
                     (when (or (= "svg" (.-tagName (.-target event)))
                               (some-> (.-target event) .-classList (.contains "graph")))
                       (clear-selection!))))))

;; Graph controls
(defn reset-graph! []
  (when-let [gv (:graphviz @state)]
    (.resetZoom gv))
  (clear-selection!))

(defn toggle-pipelines-only! []
  (swap! state assoc :pipelines-only? (not (:pipelines-only? @state)) :last-rendered-key nil)
  (toggle-class! ($id "pipelines-only-btn") "active")
  (render-graph!))

(defn toggle-group! [group-name]
  (let [selected (:selected-node @state)]
    (swap! state (fn [s]
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
  (when-let [config (:config @state)]
    (let [all-groups (set (graph/all-groups config))
          collapsed (:collapsed-groups @state)
          all-collapsed? (= all-groups collapsed)
          selected (:selected-node @state)]
      (swap! state assoc
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
  (when-let [config (:config @state)]
    (when-let [container ($id "group-toggles")]
      (let [groups (graph/all-groups config)
            collapsed (:collapsed-groups @state)
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
                                     (graph/highlight-match name query)
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
        (if (or (empty? query) (nil? (:config @state)))
          (do
            (remove-class! dropdown "show")
            (set-html! dropdown ""))
          (let [results (graph/search-nodes (:config @state) query)]
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
          {:keys [valid? errors]} (graph/validate-config config)]
      (if valid?
        (do
          (set-config! config)
          ;; Precompute lineage for fast selection
          (precompute-lineage! config)
          (precompute-attribute-lineage! config)
          (update-json-input! config)
          (render-tables!)
          (switch-tab! "graph-pane"))
        (js/console.error "Invalid config:" (clj->js errors))))
    (catch :default e
      (js/console.error "Failed to parse config:" e))))

(defn handle-file-drop [e]
  (.preventDefault e)
  (when-let [file (-> e .-dataTransfer .-files (aget 0))]
    (let [reader (js/FileReader.)]
      (set! (.-onload reader) #(load-config! (-> % .-target .-result)))
      (.readAsText reader file))))

(defn handle-file-select [e]
  (when-let [file (-> e .-target .-files (aget 0))]
    (let [reader (js/FileReader.)]
      (set! (.-onload reader) #(load-config! (-> % .-target .-result)))
      (.readAsText reader file))))

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

(defn load-example! []
  (set-config! graph/example-config)
  (precompute-lineage! graph/example-config)
  (precompute-attribute-lineage! graph/example-config)
  (update-json-input! graph/example-config)
  (render-tables!)
  (show-json-status! "Example loaded" false)
  (when (= "config-pane" (some-> ($id "main-tabs") (.querySelector ".nav-tab.active") (.getAttribute "data-tab")))
    nil ;; stay on config
    )
  (when (not= "config-pane" (some-> ($id "main-tabs") (.querySelector ".nav-tab.active") (.getAttribute "data-tab")))
    (switch-tab! "graph-pane")))

(defn apply-config! []
  (when-let [input ($id "json-input")]
    (let [json-str (.-value input)]
      (if (empty? json-str)
        (show-json-status! "Please enter some JSON" true)
        (try
          (let [config (js->clj (js/JSON.parse json-str) :keywordize-keys true)
                {:keys [valid? errors]} (graph/validate-config config)]
            (if valid?
              (do
                (set-config! config)
                (precompute-lineage! config)
                (precompute-attribute-lineage! config)
                (render-tables!)
                (show-json-status! "Config applied successfully" false)
                (switch-tab! "graph-pane"))
              (show-json-status! (str "Invalid config: " (first errors)) true)))
          (catch :default e
            (show-json-status! (str "JSON parse error: " (.-message e)) true)))))))

(defn format-json! []
  (when-let [input ($id "json-input")]
    (let [json-str (.-value input)]
      (when (not (empty? json-str))
        (try
          (let [parsed (js/JSON.parse json-str)
                formatted (js/JSON.stringify parsed nil 2)]
            (set! (.-value input) formatted)
            (show-json-status! "Formatted" false))
          (catch :default e
            (show-json-status! (str "Cannot format - invalid JSON: " (.-message e)) true)))))))

;; Table rendering
(defn render-pipelines-table! []
  (when-let [config (:config @state)]
    (let [container ($id "pipelines-table-container")
          pipelines (:pipelines config)]
      (if (empty? pipelines)
        (set-html! container "<p class='table-empty'>No pipelines found</p>")
        (let [rows (str/join ""
                             (map (fn [p]
                                    (let [input-sources (str/join ""
                                                                  (map #(str "<span class='badge badge-input'>" % "</span>")
                                                                       (:input_sources p)))
                                          output-sources (str/join ""
                                                                   (map #(str "<span class='badge badge-output'>" % "</span>")
                                                                        (:output_sources p)))
                                          cluster (if (:cluster p)
                                                    (str "<span class='badge badge-cluster'>" (:cluster p) "</span>")
                                                    "")
                                          tags (str/join ""
                                                         (map #(str "<span class='badge badge-tag'>" % "</span>")
                                                              (:tags p)))
                                          links (if (:links p)
                                                  (str "<div class='links-scroll'>"
                                                       (str/join ""
                                                                 (map (fn [[k v]]
                                                                        (str "<a href='" v "' target='_blank' class='graph-ctrl-btn' style='font-size:0.7em;padding:2px 6px'>" (name k) "</a>"))
                                                                      (:links p)))
                                                       "</div>")
                                                  "")]
                                      (str "<tr class='pipeline-row' "
                                           "data-name='" (str/lower-case (:name p)) "' "
                                           "data-desc='" (str/lower-case (or (:description p) "")) "' "
                                           "data-tags='" (str/join "," (map str/lower-case (:tags p))) "' "
                                           "data-cluster='" (str/lower-case (or (:cluster p) "")) "'>"
                                           "<td class='col-name'><div><strong>" (:name p) "</strong></div></td>"
                                           "<td class='col-desc'><div>" (or (:description p) "") "</div></td>"
                                           "<td class='col-schedule'><div><code style='color:#28a745'>" (graph/format-schedule (:schedule p)) "</code></div></td>"
                                           "<td class='col-sources'><div>" input-sources "</div></td>"
                                           "<td class='col-sources'><div>" output-sources "</div></td>"
                                           "<td class='col-cluster'><div>" cluster "</div></td>"
                                           "<td class='col-tags'><div>" tags "</div></td>"
                                           "<td class='col-links'><div>" links "</div></td>"
                                           "</tr>")))
                                  pipelines))
              html (str "<div class='table-container'><table class='data-table'>"
                        "<thead><tr>"
                        "<th>Pipeline</th><th>Description</th><th>Schedule</th>"
                        "<th>Input Sources</th><th>Output Sources</th>"
                        "<th>Cluster</th><th>Tags</th><th>Links</th>"
                        "</tr></thead><tbody>"
                        rows
                        "</tbody></table></div>")]
          (set-html! container html))))))

(defn render-datasources-table! []
  (when-let [config (:config @state)]
    (let [container ($id "datasources-table-container")
          ;; Collect all datasources (explicit + auto-created from pipeline refs)
          explicit-ds (into {} (map (fn [ds] [(:name ds) ds]) (:datasources config)))
          referenced (set (concat
                           (mapcat :input_sources (:pipelines config))
                           (mapcat :output_sources (:pipelines config))))
          all-ds (merge
                  (into {} (for [name referenced
                                 :when (not (contains? explicit-ds name))]
                             [name {:name name
                                    :type "auto-created"
                                    :description "Auto-created from pipeline references"
                                    :auto-created? true}]))
                  explicit-ds)]
      (if (empty? all-ds)
        (set-html! container "<p class='table-empty'>No datasources found</p>")
        (let [rows (str/join ""
                             (map (fn [[_ ds]]
                                    (let [type-badge (str "<span class='badge badge-" (str/lower-case (or (:type ds) "unknown")) "'>"
                                                          (str/upper-case (or (:type ds) "unknown")) "</span>")
                                          metadata (if (:metadata ds)
                                                     (str/join ""
                                                               (map (fn [[k v]]
                                                                      (str "<div style='font-size:11px;color:var(--text-muted);margin-bottom:2px'>"
                                                                           "<span style='font-weight:500'>" (str/replace (name k) "_" " ") ":</span> " v "</div>"))
                                                                    (:metadata ds)))
                                                     "")
                                          cluster (if (:cluster ds)
                                                    (str "<span class='badge badge-cluster'>" (:cluster ds) "</span>")
                                                    "")
                                          tags (str/join ""
                                                         (map #(str "<span class='badge badge-tag'>" % "</span>")
                                                              (:tags ds)))
                                          links (if (:links ds)
                                                  (str "<div class='links-scroll'>"
                                                       (str/join ""
                                                                 (map (fn [[k v]]
                                                                        (str "<a href='" v "' target='_blank' class='graph-ctrl-btn' style='font-size:0.7em;padding:2px 6px'>" (name k) "</a>"))
                                                                      (:links ds)))
                                                       "</div>")
                                                  "")
                                          desc (if (:auto-created? ds)
                                                 (str "<span style='color:var(--text-muted);font-style:italic'>" (:description ds) "</span>")
                                                 (or (:description ds) ""))]
                                      (str "<tr class='datasource-row' "
                                           "data-name='" (str/lower-case (:name ds)) "' "
                                           "data-type='" (str/lower-case (or (:type ds) "unknown")) "' "
                                           "data-tags='" (str/join "," (map str/lower-case (:tags ds))) "' "
                                           "data-cluster='" (str/lower-case (or (:cluster ds) "")) "' "
                                           "data-search='" (str/lower-case (str (:name ds) " " (or (:description ds) "") " " (or (:owner ds) ""))) "'>"
                                           "<td class='col-name'><div><strong>" (:name ds) "</strong><br>" type-badge "</div></td>"
                                           "<td class='col-desc'><div>" desc "</div></td>"
                                           "<td class='col-owner'><div style='font-size:11px;color:var(--text-muted)'>" (or (:owner ds) "") "</div></td>"
                                           "<td class='col-metadata'><div>" metadata "</div></td>"
                                           "<td class='col-cluster'><div>" cluster "</div></td>"
                                           "<td class='col-tags'><div>" tags "</div></td>"
                                           "<td class='col-links'><div>" links "</div></td>"
                                           "</tr>")))
                                  all-ds))
              html (str "<div class='table-container'><table class='data-table'>"
                        "<thead><tr>"
                        "<th>Name & Type</th><th>Description</th><th>Owner</th>"
                        "<th>Metadata</th><th>Cluster</th><th>Tags</th><th>Links</th>"
                        "</tr></thead><tbody>"
                        rows
                        "</tbody></table></div>")]
          (set-html! container html))))))

(defn filter-pipelines! []
  (let [search-term (str/lower-case (or (some-> ($id "pipeline-search") .-value) ""))
        active-tags (:pipeline-tags @filter-state)
        active-clusters (:pipeline-clusters @filter-state)
        rows (array-seq (.querySelectorAll js/document ".pipeline-row"))]
    (doseq [row rows]
      (let [name (.getAttribute row "data-name")
            desc (.getAttribute row "data-desc")
            tags (set (remove empty? (str/split (or (.getAttribute row "data-tags") "") #",")))
            cluster (.getAttribute row "data-cluster")
            matches-search (or (empty? search-term)
                               (str/includes? name search-term)
                               (str/includes? desc search-term))
            matches-tags (or (empty? active-tags)
                             (some active-tags tags))
            matches-cluster (or (empty? active-clusters)
                                (active-clusters cluster))]
        (set! (.-display (.-style row)) (if (and matches-search matches-tags matches-cluster) "" "none"))))))

(defn filter-datasources! []
  (let [search-term (str/lower-case (or (some-> ($id "datasource-search") .-value) ""))
        active-types (:datasource-types @filter-state)
        active-tags (:datasource-tags @filter-state)
        active-clusters (:datasource-clusters @filter-state)
        rows (array-seq (.querySelectorAll js/document ".datasource-row"))]
    (doseq [row rows]
      (let [search-content (.getAttribute row "data-search")
            ds-type (.getAttribute row "data-type")
            tags (set (remove empty? (str/split (or (.getAttribute row "data-tags") "") #",")))
            cluster (.getAttribute row "data-cluster")
            matches-search (or (empty? search-term)
                               (str/includes? search-content search-term))
            matches-type (or (empty? active-types)
                             (active-types ds-type))
            matches-tags (or (empty? active-tags)
                             (some active-tags tags))
            matches-cluster (or (empty? active-clusters)
                                (active-clusters cluster))]
        (set! (.-display (.-style row)) (if (and matches-search matches-type matches-tags matches-cluster) "" "none"))))))

(defn build-filter-group [label filter-key values]
  (when (seq values)
    (let [active-filters (get @filter-state filter-key)
          active-count (count active-filters)
          has-active (pos? active-count)]
      (str "<span class='filter-group' data-filter-key='" (name filter-key) "'>"
           "<span class='filter-group-header" (when has-active " has-active") "'>"
           "<span class='filter-group-label'>" label "</span>"
           "<span class='filter-group-count'>" (if has-active active-count (count values)) "</span>"
           "</span>"
           "<div class='filter-group-dropdown'><div class='filter-group-dropdown-inner'>"
           (str/join "" (map (fn [v]
                               (str "<span class='filter-tag" (when (contains? active-filters v) " active") "' data-value='" v "'>" v "</span>"))
                             (sort values)))
           "</div></div></span>"))))

(defn setup-filter-handlers! [container-id apply-filter-fn]
  (when-let [container ($id container-id)]
    (doseq [tag (array-seq (.querySelectorAll container ".filter-tag"))]
      (on! tag "click" (fn [e]
                         (.stopPropagation e)
                         (let [group (.closest tag ".filter-group")
                               filter-key (keyword (.getAttribute group "data-filter-key"))
                               value (.getAttribute tag "data-value")]
                           (if (contains? (get @filter-state filter-key) value)
                             (swap! filter-state update filter-key disj value)
                             (swap! filter-state update filter-key conj value))
                           (apply-filter-fn)))))))

(defn update-filter-ui! [container-id]
  (when-let [container ($id container-id)]
    (doseq [group (array-seq (.querySelectorAll container ".filter-group"))]
      (let [filter-key (keyword (.getAttribute group "data-filter-key"))
            active-filters (get @filter-state filter-key)
            active-count (count active-filters)
            has-active (pos? active-count)
            header (.querySelector group ".filter-group-header")
            count-el (.querySelector group ".filter-group-count")
            tags (.querySelectorAll group ".filter-tag")]
        (when header
          (if has-active
            (add-class! header "has-active")
            (remove-class! header "has-active")))
        (when count-el
          (set! (.-textContent count-el) (if has-active active-count (.-length tags))))
        (doseq [tag (array-seq tags)]
          (let [value (.getAttribute tag "data-value")]
            (if (contains? active-filters value)
              (add-class! tag "active")
              (remove-class! tag "active"))))))))

(defn render-pipeline-filters! []
  (when-let [config (:config @state)]
    (let [all-tags (set (mapcat :tags (:pipelines config)))
          all-clusters (set (remove nil? (map :cluster (:pipelines config))))
          html (str (build-filter-group "cluster" :pipeline-clusters all-clusters)
                    (build-filter-group "tag" :pipeline-tags all-tags))]
      (set-html! ($id "pipeline-filters") html)
      (setup-filter-handlers! "pipeline-filters" (fn []
                                                   (update-filter-ui! "pipeline-filters")
                                                   (filter-pipelines!))))))

(defn render-datasource-filters! []
  (when-let [config (:config @state)]
    (let [;; Include auto-created datasources
          explicit-ds (:datasources config)
          referenced (set (concat (mapcat :input_sources (:pipelines config))
                                  (mapcat :output_sources (:pipelines config))))
          all-ds (concat explicit-ds
                         (for [name referenced
                               :when (not (some #(= (:name %) name) explicit-ds))]
                           {:name name :type "auto-created"}))
          all-types (set (map #(or (:type %) "unknown") all-ds))
          all-tags (set (mapcat :tags explicit-ds))
          all-clusters (set (remove nil? (map :cluster explicit-ds)))
          html (str (build-filter-group "type" :datasource-types all-types)
                    (build-filter-group "cluster" :datasource-clusters all-clusters)
                    (build-filter-group "tag" :datasource-tags all-tags))]
      (set-html! ($id "datasource-filters") html)
      (setup-filter-handlers! "datasource-filters" (fn []
                                                     (update-filter-ui! "datasource-filters")
                                                     (filter-datasources!))))))

(defn setup-table-search! []
  (when-let [pipeline-search ($id "pipeline-search")]
    (on! pipeline-search "keyup" (fn [_] (filter-pipelines!))))
  (when-let [datasource-search ($id "datasource-search")]
    (on! datasource-search "keyup" (fn [_] (filter-datasources!)))))

(defn render-tables! []
  (render-pipelines-table!)
  (render-datasources-table!)
  (render-pipeline-filters!)
  (render-datasource-filters!))

;; ============================================================================
;; Attribute Graph
;; ============================================================================

(defonce attribute-state (atom {:graphviz nil
                                :lineage-map nil
                                :selected-attribute nil}))

(defn precompute-attribute-lineage! [config]
  (let [result (graph/build-attribute-lineage-map config)]
    (swap! attribute-state assoc
           :lineage-map (:attribute-map result)
           :datasource-lineage-map (:datasource-map result))))

(defn clear-attribute-selection! []
  (swap! attribute-state assoc :selected-attribute nil)
  ;; Clear highlighting
  (-> (.select d3 "#attribute-graph")
      (.selectAll ".node")
      (.classed "node-highlighted" false)
      (.classed "node-connected" false)
      (.classed "node-dimmed" false))
  (-> (.select d3 "#attribute-graph")
      (.selectAll ".cluster")
      (.classed "cluster-highlighted" false)
      (.classed "cluster-connected" false)
      (.classed "cluster-dimmed" false))
  (-> (.select d3 "#attribute-graph")
      (.selectAll ".edge")
      (.classed "edge-highlighted" false)
      (.classed "edge-dimmed" false))
  ;; Hide details panel
  (when-let [col ($id "attribute-details-col")]
    (set! (.-display (.-style col)) "none"))
  (when-let [graph-col ($id "attribute-graph-col")]
    (remove-class! graph-col "col-md-8")
    (add-class! graph-col "col-md-12")))

(defn show-datasource-in-attribute-panel! [ds]
  (swap! attribute-state assoc :selected-attribute nil)
  (let [ds-id (graph/sanitize-id (:name ds))
        ds-lineage-map (:datasource-lineage-map @attribute-state)
        ds-lineage (get ds-lineage-map ds-id {:upstream [] :downstream []})
        upstream-list (:upstream ds-lineage)
        downstream-list (:downstream ds-lineage)
        connected-ids (set (concat (map :id upstream-list) (map :id downstream-list)))]

    ;; Clear previous highlighting
    (-> (.select d3 "#attribute-graph")
        (.selectAll ".node")
        (.classed "node-highlighted" false)
        (.classed "node-connected" false)
        (.classed "node-dimmed" false))
    (-> (.select d3 "#attribute-graph")
        (.selectAll ".edge")
        (.classed "edge-highlighted" false)
        (.classed "edge-dimmed" false))
    (-> (.select d3 "#attribute-graph")
        (.selectAll ".cluster")
        (.classed "cluster-highlighted" false)
        (.classed "cluster-connected" false)
        (.classed "cluster-dimmed" false))

    ;; Highlight connected clusters
    (-> (.select d3 "#attribute-graph")
        (.selectAll ".cluster")
        (.each (fn []
                 (this-as this
                          (let [cluster (.select d3 this)
                                title (-> cluster (.select "title") .text)]
                            (when (and title (str/starts-with? title "cluster_"))
                              (let [cluster-id (subs title 8)]
                                (cond
                                  (= cluster-id ds-id)
                                  (.classed cluster "cluster-highlighted" true)

                                  (connected-ids cluster-id)
                                  (.classed cluster "cluster-connected" true)))))))))

    ;; Show details panel
    (when-let [col ($id "attribute-details-col")]
      (set! (.-display (.-style col)) "block"))
    (when-let [graph-col ($id "attribute-graph-col")]
      (remove-class! graph-col "col-md-12")
      (add-class! graph-col "col-md-8"))

    ;; Build HTML
    (let [config (:config @state)
          datasources (:datasources config)
          html (str "<h5>" (:name ds) "</h5>"

                    (when (:type ds)
                      (str "<div class='detail-label'>TYPE</div>"
                           "<div class='detail-value'><span class='badge badge-" (:type ds) "'>"
                           (str/upper-case (:type ds)) "</span></div>"))

                    (when (seq upstream-list)
                      (str "<div class='detail-label'>UPSTREAM (" (count upstream-list) ")</div>"
                           "<div class='detail-value'>"
                           (str/join ""
                                     (map (fn [{:keys [id depth]}]
                                            (let [found (first (filter #(= (graph/sanitize-id (:name %)) id) datasources))
                                                  name (if found (:name found) id)
                                                  indent (* (dec depth) 16)
                                                  opacity (max 0.4 (- 1 (* (dec depth) 0.2)))
                                                  prefix (if (> depth 1) "└ " "")]
                                              (str "<div class='lineage-link' data-ds-name='" name
                                                   "' style='padding-left: " indent "px; opacity: " opacity ";'>"
                                                   prefix name "</div>")))
                                          upstream-list))
                           "</div>"))

                    (when (seq downstream-list)
                      (str "<div class='detail-label'>DOWNSTREAM (" (count downstream-list) ")</div>"
                           "<div class='detail-value'>"
                           (str/join ""
                                     (map (fn [{:keys [id depth]}]
                                            (let [found (first (filter #(= (graph/sanitize-id (:name %)) id) datasources))
                                                  name (if found (:name found) id)
                                                  indent (* (dec depth) 16)
                                                  opacity (max 0.4 (- 1 (* (dec depth) 0.2)))
                                                  prefix (if (> depth 1) "└ " "")]
                                              (str "<div class='lineage-link' data-ds-name='" name
                                                   "' style='padding-left: " indent "px; opacity: " opacity ";'>"
                                                   prefix name "</div>")))
                                          downstream-list))
                           "</div>"))

                    (when (:description ds)
                      (str "<div class='detail-label'>DESCRIPTION</div>"
                           "<div class='detail-value'>" (:description ds) "</div>"))

                    (when (:owner ds)
                      (str "<div class='detail-label'>OWNER</div>"
                           "<div class='detail-value'>" (:owner ds) "</div>"))

                    (when (:cluster ds)
                      (str "<div class='detail-label'>CLUSTER</div>"
                           "<div class='detail-value'><span class='badge badge-cluster'>" (:cluster ds) "</span></div>"))

                    (when (seq (:tags ds))
                      (str "<div class='detail-label'>TAGS</div>"
                           "<div class='detail-value'>"
                           (str/join "" (map (fn [t] (str "<span class='badge me-1 mb-1' style='background-color: #fff3cd; color: #856404;'>" t "</span>")) (:tags ds)))
                           "</div>"))

                    (when (seq (:attributes ds))
                      (let [count-attrs (fn count-attrs [attrs]
                                          (reduce (fn [c attr]
                                                    (+ c 1 (if (:attributes attr)
                                                             (count-attrs (:attributes attr))
                                                             0)))
                                                  0 attrs))
                            attr-count (count-attrs (:attributes ds))
                            list-attrs (fn list-attrs [attrs indent]
                                         (str/join ""
                                                   (map (fn [attr]
                                                          (let [has-children (seq (:attributes attr))]
                                                            (str "<div class='small' style='padding-left: " (* indent 12) "px;'>"
                                                                 (if has-children "▸ " "") (:name attr) "</div>"
                                                                 (when has-children
                                                                   (list-attrs (:attributes attr) (inc indent))))))
                                                        attrs)))]
                        (str "<div class='detail-label'>ATTRIBUTES (" attr-count ")</div>"
                             "<div class='detail-value'>"
                             (list-attrs (:attributes ds) 0)
                             "</div>")))

                    (when (and (:metadata ds) (seq (:metadata ds)))
                      (str "<div class='detail-label'>METADATA</div>"
                           "<div class='detail-value'>"
                           (str/join ""
                                     (map (fn [[k v]]
                                            (str "<div class='small'><strong>" (str/replace (name k) "_" " ") ":</strong> " v "</div>"))
                                          (:metadata ds)))
                           "</div>"))

                    (when (and (:links ds) (seq (:links ds)))
                      (str "<div class='links-section'>"
                           "<div class='detail-label'>LINKS</div>"
                           (str/join ""
                                     (map (fn [[link-name url]]
                                            (str "<a href='" url "' target='_blank' class='graph-ctrl-btn link-btn'>" (name link-name) "</a>"))
                                          (:links ds)))
                           "</div>")))]

      (set-html! ($id "attribute-details-content") html)

      ;; Click handlers for lineage links
      (doseq [el (array-seq (.querySelectorAll ($id "attribute-details-content") ".lineage-link[data-ds-name]"))]
        (on! el "click" (fn [_]
                          (let [ds-name (.getAttribute el "data-ds-name")
                                target-ds (first (filter #(= (:name %) ds-name) datasources))]
                            (when target-ds
                              (show-datasource-in-attribute-panel! target-ds)))))))))

(defn show-attribute-details! [attr-id]
  (let [lineage-map (:lineage-map @attribute-state)
        attr (get lineage-map attr-id)]
    (when attr
      (let [upstream (:full-upstream attr)
            downstream (:full-downstream attr)
            provenance-json (js/JSON.stringify (clj->js (graph/attribute-provenance lineage-map attr-id)) nil 2)
            has-lineage (or (seq upstream) (seq downstream))

            html (str "<h5>" (:name attr) "</h5>"
                      "<div class='detail-label'>DATASOURCE</div>"
                      "<div class='detail-value'>" (:datasource attr) "</div>"

                      ;; Lineage section with header
                      "<div class='lineage-section'>"
                      "<div class='lineage-header'>"
                      "<span class='detail-label' style='margin-bottom:0'>Lineage</span>"
                      (when has-lineage
                        (str "<div class='lineage-view-toggle'>"
                             "<span class='lineage-toggle-label active' data-view='tree'>Tree</span>"
                             "<div class='lineage-toggle-slider'></div>"
                             "<span class='lineage-toggle-label' data-view='json'>JSON</span>"
                             "</div>"))
                      "</div>"
                      "<div class='lineage-content'>"
                      "<div class='lineage-tree-view'>"

                      (when (seq upstream)
                        (str "<div class='detail-label'>UPSTREAM (" (count upstream) ")</div>"
                             "<div class='detail-value'>"
                             (str/join ""
                                       (map (fn [{:keys [id depth]}]
                                              (let [up-attr (get lineage-map id)]
                                                (when up-attr
                                                  (let [indent (* (dec depth) 12)
                                                        opacity (max 0.5 (- 1 (* (dec depth) 0.15)))
                                                        prefix (if (> depth 1) "└ " "")]
                                                    (str "<div class='lineage-link' data-attr-id='" id
                                                         "' style='padding-left:" indent "px;opacity:" opacity "'>"
                                                         prefix (:full-name up-attr) "</div>")))))
                                            upstream))
                             "</div>"))

                      (when (seq downstream)
                        (str "<div class='detail-label'>DOWNSTREAM (" (count downstream) ")</div>"
                             "<div class='detail-value'>"
                             (str/join ""
                                       (map (fn [{:keys [id depth]}]
                                              (let [down-attr (get lineage-map id)]
                                                (when down-attr
                                                  (let [indent (* (dec depth) 12)
                                                        opacity (max 0.5 (- 1 (* (dec depth) 0.15)))
                                                        prefix (if (> depth 1) "└ " "")]
                                                    (str "<div class='lineage-link' data-attr-id='" id
                                                         "' style='padding-left:" indent "px;opacity:" opacity "'>"
                                                         prefix (:full-name down-attr) "</div>")))))
                                            downstream))
                             "</div>"))

                      (when-not has-lineage
                        "<div class='detail-value text-muted'>No lineage connections</div>")

                      "</div>"

                      ;; JSON view (starts hidden)
                      "<div class='lineage-json-view hidden'>"
                      "<button class='json-copy-btn' title='Copy to clipboard'>"
                      "<svg width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2'>"
                      "<rect x='9' y='9' width='13' height='13' rx='2' ry='2'/>"
                      "<path d='M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1'/>"
                      "</svg></button>"
                      "<pre class='lineage-json-pre'>" provenance-json "</pre>"
                      "</div>"
                      "</div>" ;; close lineage-content
                      "</div>")]

        ;; Show details panel
        (when-let [col ($id "attribute-details-col")]
          (set! (.-display (.-style col)) "block"))
        (when-let [graph-col ($id "attribute-graph-col")]
          (remove-class! graph-col "col-md-12")
          (add-class! graph-col "col-md-8"))

        (set-html! ($id "attribute-details-content") html)

        ;; Set up toggle handlers (use classes for visibility to prevent layout jump)
        (when has-lineage
          (when-let [slider (.querySelector ($id "attribute-details-content") ".lineage-toggle-slider")]
            (on! slider "click" (fn [_]
                                  (let [is-json (.toggle (.-classList slider) "json-active")
                                        tree-view (.querySelector ($id "attribute-details-content") ".lineage-tree-view")
                                        json-view (.querySelector ($id "attribute-details-content") ".lineage-json-view")
                                        tree-label (.querySelector ($id "attribute-details-content") ".lineage-toggle-label[data-view='tree']")
                                        json-label (.querySelector ($id "attribute-details-content") ".lineage-toggle-label[data-view='json']")]
                                    (if is-json
                                      (do
                                        (add-class! tree-view "hidden")
                                        (remove-class! json-view "hidden")
                                        (remove-class! tree-label "active")
                                        (add-class! json-label "active"))
                                      (do
                                        (remove-class! tree-view "hidden")
                                        (add-class! json-view "hidden")
                                        (add-class! tree-label "active")
                                        (remove-class! json-label "active"))))))))

        ;; Copy button handler
        (when-let [copy-btn (.querySelector ($id "attribute-details-content") ".json-copy-btn")]
          (let [copy-icon "<svg width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2'><rect x='9' y='9' width='13' height='13' rx='2' ry='2'/><path d='M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1'/></svg>"
                check-icon "<svg width='14' height='14' viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2'><polyline points='20 6 9 17 4 12'/></svg>"]
            (on! copy-btn "click" (fn [_]
                                    (-> (js/navigator.clipboard.writeText provenance-json)
                                        (.then (fn []
                                                 (add-class! copy-btn "copied")
                                                 (set! (.-innerHTML copy-btn) check-icon)
                                                 (js/setTimeout #(do
                                                                   (remove-class! copy-btn "copied")
                                                                   (set! (.-innerHTML copy-btn) copy-icon))
                                                                1500))))))))

        ;; Click handlers for lineage links
        (doseq [item (array-seq (.querySelectorAll ($id "attribute-details-content") ".lineage-link[data-attr-id]"))]
          (on! item "click" (fn [_]
                              (let [target-id (.getAttribute item "data-attr-id")]
                                (when target-id
                                  (select-attribute! target-id))))))))))

(defn select-attribute! [attr-id]
  (swap! attribute-state assoc :selected-attribute attr-id)
  (let [lineage-map (:lineage-map @attribute-state)
        attr (get lineage-map attr-id)]
    (when attr
      (let [upstream (:full-upstream attr)
            downstream (:full-downstream attr)
            all-connected (set (concat [attr-id]
                                       (map :id upstream)
                                       (map :id downstream)))]
        ;; Clear previous highlighting
        (-> (.select d3 "#attribute-graph")
            (.selectAll ".node")
            (.classed "node-highlighted" false)
            (.classed "node-connected" false)
            (.classed "node-dimmed" false))
        (-> (.select d3 "#attribute-graph")
            (.selectAll ".cluster")
            (.classed "cluster-highlighted" false)
            (.classed "cluster-connected" false)
            (.classed "cluster-dimmed" false))
        (-> (.select d3 "#attribute-graph")
            (.selectAll ".edge")
            (.classed "edge-highlighted" false)
            (.classed "edge-dimmed" false))

        ;; Highlight nodes
        (-> (.select d3 "#attribute-graph")
            (.selectAll ".node")
            (.each (fn []
                     (this-as this
                              (let [node (.select d3 this)
                                    title (-> node (.select "title") .text)]
                                (cond
                                  (= title attr-id)
                                  (-> node (.classed "node-highlighted" true) (.classed "node-dimmed" false))

                                  (all-connected title)
                                  (-> node (.classed "node-connected" true) (.classed "node-dimmed" false))

                                  :else
                                  (.classed node "node-dimmed" true)))))))

        ;; Highlight clusters
        (-> (.select d3 "#attribute-graph")
            (.selectAll ".cluster")
            (.each (fn []
                     (this-as this
                              (let [cluster (.select d3 this)
                                    title (-> cluster (.select "title") .text)]
                                (when (and title (str/starts-with? title "cluster_"))
                                  (let [cluster-id (subs title 8)]
                                    (cond
                                      (= cluster-id attr-id)
                                      (-> cluster (.classed "cluster-highlighted" true) (.classed "cluster-dimmed" false))

                                      (all-connected cluster-id)
                                      (-> cluster (.classed "cluster-connected" true) (.classed "cluster-dimmed" false))

                                      :else
                                      (.classed cluster "cluster-dimmed" true)))))))))

        ;; Highlight edges
        (-> (.select d3 "#attribute-graph")
            (.selectAll ".edge")
            (.each (fn []
                     (this-as this
                              (let [edge (.select d3 this)
                                    title (-> edge (.select "title") .text)
                                    parts (when title (str/split title #"->"))
                                    from-node (when (>= (count parts) 2) (str/trim (first parts)))
                                    to-node (when (>= (count parts) 2) (str/trim (second parts)))]
                                (if (and (all-connected from-node) (all-connected to-node))
                                  (-> edge (.classed "edge-highlighted" true) (.classed "edge-dimmed" false))
                                  (.classed edge "edge-dimmed" true)))))))

        ;; Show details
        (show-attribute-details! attr-id)))))

(defn render-attribute-graph! []
  (when-let [config (:config @state)]
    (let [dot (graph/generate-attribute-dot config)
          container ($id "attribute-graph")]
      (when (and dot container (not (empty? (str/trim dot))))
        (when-not (:graphviz @attribute-state)
          (set! (.-innerHTML container) "")
          (swap! attribute-state assoc :graphviz
                 (-> (.select d3 "#attribute-graph")
                     (.graphviz)
                     (.width (or (.-offsetWidth container) 800))
                     (.height 500)
                     (.fit true)
                     (.zoom true))))

        (-> (:graphviz @attribute-state)
            (.renderDot dot)
            (.on "end" (fn []
                         ;; Node click handlers
                         (-> (.select d3 "#attribute-graph")
                             (.selectAll ".node")
                             (.on "click" (fn [event]
                                            (.stopPropagation event)
                                            (this-as this
                                                     (let [title (-> (.select d3 this) (.select "title") .text)
                                                           lineage-map (:lineage-map @attribute-state)]
                                                       (when (and title (get lineage-map title))
                                                         (select-attribute! title)))))))

                         ;; Cluster click handlers
                         (-> (.select d3 "#attribute-graph")
                             (.selectAll ".cluster")
                             (.on "click" (fn [event]
                                            (.stopPropagation event)
                                            (this-as this
                                                     (let [title (-> (.select d3 this) (.select "title") .text)
                                                           config (:config @state)]
                                                       (when (and title (str/starts-with? title "cluster_"))
                                                         (let [id (subs title 8)
                                                               lineage-map (:lineage-map @attribute-state)]
                                                           (if (str/includes? id "__")
                                                      ;; Nested attribute cluster
                                                             (when (get lineage-map id)
                                                               (select-attribute! id))
                                                      ;; Datasource cluster
                                                             (let [ds (first (filter #(= (graph/sanitize-id (:name %)) id)
                                                                                     (:datasources config)))]
                                                               (when ds
                                                                 (show-datasource-in-attribute-panel! ds)))))))))))

                         ;; Background click to clear
                         (-> (.select d3 "#attribute-graph svg")
                             (.on "click" (fn [event]
                                            (when (or (= (.-tagName (.-target event)) "svg")
                                                      (.contains (.-classList (.-target event)) "graph"))
                                              (clear-attribute-selection!))))))))))))

(defn reset-attribute-graph! []
  (when-let [gv (:graphviz @attribute-state)]
    (.resetZoom gv)))

(defn setup-attribute-search! []
  (when-let [input ($id "attribute-search")]
    (let [results-div ($id "attribute-search-results")]
      (on! input "keyup" (fn [e]
                           (let [key (.-key e)
                                 query (str/lower-case (str/trim (.-value input)))
                                 lineage-map (:lineage-map @attribute-state)]
                             (cond
                               (= key "Escape")
                               (do
                                 (remove-class! results-div "show")
                                 (set! (.-innerHTML results-div) ""))

                               (= key "Enter")
                               (when-let [selected (.querySelector results-div ".search-result-item.selected")]
                                 (let [attr-id (.-id (.-dataset selected))]
                                   (when attr-id
                                     (remove-class! results-div "show")
                                     (set! (.-value input) "")
                                     (select-attribute! attr-id))))

                               (< (count query) 2)
                               (remove-class! results-div "show")

                               :else
                               (let [matches (->> (vals lineage-map)
                                                  (filter (fn [attr]
                                                            (or (str/includes? (str/lower-case (:full-name attr)) query)
                                                                (str/includes? (str/lower-case (:name attr)) query))))
                                                  (take 10))]
                                 (if (empty? matches)
                                   (remove-class! results-div "show")
                                   (do
                                     (set-html! results-div
                                                (str/join ""
                                                          (map-indexed (fn [i attr]
                                                                         (str "<div class='search-result-item" (when (= i 0) " selected") "' data-id='" (:id attr) "'>"
                                                                              "<span class='result-type datasource'>" (:datasource attr) "</span>"
                                                                              "<span class='result-name'>" (:name attr) "</span>"
                                                                              "</div>"))
                                                                       matches)))
                                     (add-class! results-div "show")
                                     ;; Click handlers
                                     (doseq [item (array-seq (.querySelectorAll results-div ".search-result-item"))]
                                       (on! item "click" (fn [_]
                                                           (let [attr-id (.-id (.-dataset item))]
                                                             (remove-class! results-div "show")
                                                             (set! (.-value input) "")
                                                             (select-attribute! attr-id)))))))))))))))

;; Handle hashchange for browser back/forward navigation
(defn setup-hashchange-listener! []
  (.addEventListener js/window "hashchange"
                     (fn [_]
                       (let [node-from-hash (get-node-from-hash)
                             current-selected (:selected-node @state)]
                         (cond
                           ;; Node in hash but different from current selection
                           (and node-from-hash (not= node-from-hash current-selected))
                           (do
                             (switch-tab! "graph-pane")
                             (js/setTimeout select-node-from-hash! 100))

                           ;; No node in hash but we have a selection - clear it
                           (and (nil? node-from-hash) current-selected)
                           (clear-selection!))))))

;; Init
(defn init []
  (js/console.log "Pipeviz ClojureScript initialized")
  (init-theme!)
  (setup-tabs!)
  (setup-drop-zone!)
  (setup-search!)
  (setup-table-search!)
  (setup-attribute-search!)
  (setup-hashchange-listener!)
  (render-splash!)
  ;; Load example by default
  (set-config! graph/example-config)
  (precompute-lineage! graph/example-config)
  (precompute-attribute-lineage! graph/example-config)
  (update-json-input! graph/example-config)
  (render-tables!)
  ;; If there's a node in the hash, switch to graph pane
  (when (get-node-from-hash)
    (switch-tab! "graph-pane")))

(defn reload []
  (when (:config @state)
    (render-graph! true)) ;; force re-render
  (render-splash!))

;; Expose to window for HTML onclick handlers
(set! js/window.toggleTheme toggle-theme!)
(set! js/window.loadExample load-example!)
(set! js/window.resetGraph reset-graph!)
(set! js/window.togglePipelinesOnly toggle-pipelines-only!)
(set! js/window.toggleGroup toggle-group!)
(set! js/window.toggleAllGroups toggle-all-groups!)
(set! js/window.switchTab switch-tab!)
(set! js/window.applyConfig apply-config!)
(set! js/window.formatJson format-json!)
(set! js/window.resetAttributeGraph reset-attribute-graph!)


