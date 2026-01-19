(ns pipeviz.ui.attributes
  "Attribute lineage graph and details panel"
  (:require ["d3" :as d3]
            [clojure.string :as str]
            [pipeviz.core.graph :as core]
            [pipeviz.ui.dom :refer [$id on! set-html! add-class! remove-class!
                                    icon-copy setup-copy-btn!
                                    clear-graph-highlight! setup-view-toggle!]]
            [pipeviz.ui.state :as state]))

(declare select-attribute! show-datasource-in-attribute-panel!)

(defn precompute-lineage! [config]
  (let [result (core/build-attribute-lineage-map config)]
    (swap! state/attributes assoc
           :lineage-map (:attribute-map result)
           :datasource-lineage-map (:datasource-map result))))

(defn clear-selection! []
  (swap! state/attributes assoc :selected-attribute nil)
  (clear-graph-highlight! "#attribute-graph" :clusters? true)
  ;; Hide details panel
  (when-let [col ($id "attribute-details-col")]
    (set! (.-display (.-style col)) "none"))
  (when-let [graph-col ($id "attribute-graph-col")]
    (remove-class! graph-col "col-md-8")
    (add-class! graph-col "col-md-12")))

(defn show-datasource-in-attribute-panel! [ds]
  (swap! state/attributes assoc :selected-attribute nil)
  (let [ds-id (core/sanitize-id (:name ds))
        ds-lineage-map (:datasource-lineage-map @state/attributes)
        ds-lineage (get ds-lineage-map ds-id {:upstream [] :downstream []})
        upstream-list (:upstream ds-lineage)
        downstream-list (:downstream ds-lineage)
        connected-ids (set (concat (map :id upstream-list) (map :id downstream-list)))]

    ;; Clear previous highlighting
    (clear-graph-highlight! "#attribute-graph" :clusters? true)

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
    (let [config (:config @state/app)
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
                                            (let [found (first (filter #(= (core/sanitize-id (:name %)) id) datasources))
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
                                            (let [found (first (filter #(= (core/sanitize-id (:name %)) id) datasources))
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

(defn- show-details! [attr-id]
  (let [lineage-map (:lineage-map @state/attributes)
        attr (get lineage-map attr-id)]
    (when attr
      (let [upstream (:full-upstream attr)
            downstream (:full-downstream attr)
            provenance-json (js/JSON.stringify (clj->js (core/attribute-provenance lineage-map attr-id)) nil 2)
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
                      "<button class='json-copy-btn' title='Copy to clipboard'>" icon-copy "</button>"
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

        ;; Set up toggle and copy handlers
        (when has-lineage
          (setup-view-toggle! ($id "attribute-details-content")))
        (when-let [copy-btn (.querySelector ($id "attribute-details-content") ".json-copy-btn")]
          (setup-copy-btn! copy-btn provenance-json))

        ;; Click handlers for lineage links
        (doseq [item (array-seq (.querySelectorAll ($id "attribute-details-content") ".lineage-link[data-attr-id]"))]
          (on! item "click" (fn [_]
                              (let [target-id (.getAttribute item "data-attr-id")]
                                (when target-id
                                  (select-attribute! target-id))))))))))

(defn select-attribute! [attr-id]
  (swap! state/attributes assoc :selected-attribute attr-id)
  (let [lineage-map (:lineage-map @state/attributes)
        attr (get lineage-map attr-id)]
    (when attr
      (let [upstream (:full-upstream attr)
            downstream (:full-downstream attr)
            all-connected (set (concat [attr-id]
                                       (map :id upstream)
                                       (map :id downstream)))]
        ;; Clear previous highlighting
        (clear-graph-highlight! "#attribute-graph" :clusters? true)

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
        (show-details! attr-id)))))

(defn render-graph! []
  (when-let [config (:config @state/app)]
    (let [dot (core/generate-attribute-dot config)
          container ($id "attribute-graph")]
      (when (and dot container (not (empty? (str/trim dot))))
        (when-not (:graphviz @state/attributes)
          (set! (.-innerHTML container) "")
          (swap! state/attributes assoc :graphviz
                 (-> (.select d3 "#attribute-graph")
                     (.graphviz)
                     (.width (or (.-offsetWidth container) 800))
                     (.height 500)
                     (.fit true)
                     (.zoom true))))

        (-> (:graphviz @state/attributes)
            (.renderDot dot)
            (.on "end" (fn []
                         ;; Node click handlers
                         (-> (.select d3 "#attribute-graph")
                             (.selectAll ".node")
                             (.on "click" (fn [event]
                                            (.stopPropagation event)
                                            (this-as this
                                              (let [title (-> (.select d3 this) (.select "title") .text)
                                                    lineage-map (:lineage-map @state/attributes)]
                                                (when (and title (get lineage-map title))
                                                  (select-attribute! title)))))))

                         ;; Cluster click handlers
                         (-> (.select d3 "#attribute-graph")
                             (.selectAll ".cluster")
                             (.on "click" (fn [event]
                                            (.stopPropagation event)
                                            (this-as this
                                              (let [title (-> (.select d3 this) (.select "title") .text)
                                                    config (:config @state/app)]
                                                (when (and title (str/starts-with? title "cluster_"))
                                                  (let [id (subs title 8)
                                                        lineage-map (:lineage-map @state/attributes)]
                                                    (if (str/includes? id "__")
                                                      ;; Nested attribute cluster
                                                      (when (get lineage-map id)
                                                        (select-attribute! id))
                                                      ;; Datasource cluster
                                                      (let [ds (first (filter #(= (core/sanitize-id (:name %)) id)
                                                                              (:datasources config)))]
                                                        (when ds
                                                          (show-datasource-in-attribute-panel! ds)))))))))))

                         ;; Background click to clear
                         (-> (.select d3 "#attribute-graph svg")
                             (.on "click" (fn [event]
                                            (when (or (= (.-tagName (.-target event)) "svg")
                                                      (.contains (.-classList (.-target event)) "graph"))
                                              (clear-selection!))))))))))))

(defn reset-graph! []
  (when-let [gv (:graphviz @state/attributes)]
    (.resetZoom gv)))

(defn setup-search! []
  (when-let [input ($id "attribute-search")]
    (let [results-div ($id "attribute-search-results")]
      (on! input "keyup" (fn [e]
                           (let [key (.-key e)
                                 query (str/lower-case (str/trim (.-value input)))
                                 lineage-map (:lineage-map @state/attributes)]
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
