(ns pipeviz.ui.tables
    "Table rendering and filtering for pipelines and datasources"
    (:require [clojure.string :as str]
              [pipeviz.core.graph :as core]
              [pipeviz.ui.dom :refer [$id on! set-html! add-class! remove-class!]]
              [pipeviz.ui.state :as state]))

(defn render-pipelines-table! []
      (when-let [config (:config @state/app)]
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
                                                                 "<td class='col-schedule'><div><code style='color:#28a745'>" (core/format-schedule (:schedule p)) "</code></div></td>"
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
      (when-let [config (:config @state/app)]
                (let [container ($id "datasources-table-container")
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
            active-tags (:pipeline-tags @state/filters)
            active-clusters (:pipeline-clusters @state/filters)
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
            active-types (:datasource-types @state/filters)
            active-tags (:datasource-tags @state/filters)
            active-clusters (:datasource-clusters @state/filters)
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

(defn- build-filter-group [label filter-key values]
       (when (seq values)
             (let [active-filters (get @state/filters filter-key)
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

(defn- setup-filter-handlers! [container-id apply-filter-fn]
       (when-let [container ($id container-id)]
                 (doseq [tag (array-seq (.querySelectorAll container ".filter-tag"))]
                        (on! tag "click" (fn [e]
                                             (.stopPropagation e)
                                             (let [group (.closest tag ".filter-group")
                                                   filter-key (keyword (.getAttribute group "data-filter-key"))
                                                   value (.getAttribute tag "data-value")]
                                                  (if (contains? (get @state/filters filter-key) value)
                                                      (swap! state/filters update filter-key disj value)
                                                      (swap! state/filters update filter-key conj value))
                                                  (apply-filter-fn)))))))

(defn- update-filter-ui! [container-id]
       (when-let [container ($id container-id)]
                 (doseq [group (array-seq (.querySelectorAll container ".filter-group"))]
                        (let [filter-key (keyword (.getAttribute group "data-filter-key"))
                              active-filters (get @state/filters filter-key)
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
      (when-let [config (:config @state/app)]
                (let [all-tags (set (mapcat :tags (:pipelines config)))
                      all-clusters (set (remove nil? (map :cluster (:pipelines config))))
                      html (str (build-filter-group "cluster" :pipeline-clusters all-clusters)
                                (build-filter-group "tag" :pipeline-tags all-tags))]
                     (set-html! ($id "pipeline-filters") html)
                     (setup-filter-handlers! "pipeline-filters" (fn []
                                                                    (update-filter-ui! "pipeline-filters")
                                                                    (filter-pipelines!))))))

(defn render-datasource-filters! []
      (when-let [config (:config @state/app)]
                (let [explicit-ds (:datasources config)
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

(defn setup-search! []
      (when-let [pipeline-search ($id "pipeline-search")]
                (on! pipeline-search "keyup" (fn [_] (filter-pipelines!))))
      (when-let [datasource-search ($id "datasource-search")]
                (on! datasource-search "keyup" (fn [_] (filter-datasources!)))))

(defn render-all! []
      (render-pipelines-table!)
      (render-datasources-table!)
      (render-pipeline-filters!)
      (render-datasource-filters!))
