(ns pipeviz.graph
  "Pure graph logic for pipeline lineage - no DOM, no JS interop"
  (:require [clojure.string :as str]))

(declare format-schedule)

;; =============================================================================
;; Specs (lean, data-oriented)
;; =============================================================================

(def pipeline-keys #{:name :description :input_sources :output_sources
                     :schedule :duration :cost :cluster :tags :links
                     :upstream_pipelines :group})

(def datasource-keys #{:name :type :description :cluster :owner :tags
                       :metadata :links :attributes})

(def attribute-keys #{:name :from :attributes})

(defn valid-pipeline? [p]
  (and (map? p) (string? (:name p))))

(defn valid-datasource? [ds]
  (and (map? ds) (string? (:name ds))))

(defn valid-config? [{:keys [pipelines]}]
  (and (seq pipelines) (every? valid-pipeline? pipelines)))

(defn validate-config
  "Validate config, returns {:valid? bool :errors []}"
  [{:keys [pipelines] :as config}]
  (let [errors (cond-> []
                 (nil? config) (conj "Config is nil")
                 (not (seq pipelines)) (conj "No pipelines defined")
                 (some #(nil? (:name %)) pipelines) (conj "Pipeline missing name"))]
    {:valid? (empty? errors) :errors errors}))

;; =============================================================================
;; Utilities
;; =============================================================================

(defn sanitize-id
  "Sanitize string for use as Graphviz ID"
  [s]
  (str/replace (or s "") #"[^a-zA-Z0-9_]" "_"))

(defn escape-dot
  "Escape string for DOT format"
  [s]
  (-> s (str/replace "\\" "\\\\") (str/replace "\"" "\\\"")))

;; =============================================================================
;; Graph Traversal (generic, reusable)
;; =============================================================================

(defn bfs
  "Generic BFS traversal. Returns seq of {:id :depth} maps.
   - start: starting node id
   - neighbors-fn: (id) -> seq of neighbor ids"
  [start neighbors-fn]
  (loop [result []
         visited #{start}
         queue (mapv #(hash-map :id % :depth 1) (neighbors-fn start))]
    (if (empty? queue)
      result
      (let [{:keys [id depth]} (first queue)]
        (if (visited id)
          (recur result visited (subvec queue 1))
          (recur (conj result {:id id :depth depth})
                 (conj visited id)
                 (into (subvec queue 1)
                       (map #(hash-map :id % :depth (inc depth))
                            (remove visited (neighbors-fn id))))))))))

(defn reachable
  "All nodes reachable from start via neighbors-fn"
  [start neighbors-fn]
  (set (map :id (bfs start neighbors-fn))))

;; =============================================================================
;; Pipeline Graph Adjacency
;; =============================================================================

(defn build-pipeline-adjacency
  "Build upstream/downstream maps for pipeline-to-pipeline relationships"
  [{:keys [pipelines]}]
  (let [output->producer (into {} (for [p pipelines, out (:output_sources p)]
                                    [out (:name p)]))]
    (reduce
     (fn [acc {:keys [name input_sources upstream_pipelines]}]
       (let [implicit (keep output->producer input_sources)
             all-up (distinct (concat upstream_pipelines implicit))]
         (reduce (fn [a up]
                   (-> a
                       (update-in [:downstream up] (fnil conj #{}) name)
                       (update-in [:upstream name] (fnil conj #{}) up)))
                 acc all-up)))
     {:upstream {} :downstream {}}
     pipelines)))

(defn build-full-adjacency
  "Build adjacency including datasources as nodes"
  [{:keys [pipelines]}]
  (reduce
   (fn [acc {:keys [name input_sources output_sources upstream_pipelines]}]
     (as-> acc $
       (reduce #(-> %1
                    (update-in [:downstream %2] (fnil conj []) name)
                    (update-in [:upstream name] (fnil conj []) %2))
               $ (or input_sources []))
       (reduce #(-> %1
                    (update-in [:downstream name] (fnil conj []) %2)
                    (update-in [:upstream %2] (fnil conj []) name))
               $ (or output_sources []))
       (reduce #(-> %1
                    (update-in [:downstream %2] (fnil conj []) name)
                    (update-in [:upstream name] (fnil conj []) %2))
               $ (or upstream_pipelines []))))
   {:downstream {} :upstream {}}
   pipelines))

;; =============================================================================
;; Lineage Queries
;; =============================================================================

(defn upstream-of [config node]
  (let [{:keys [upstream]} (build-pipeline-adjacency config)]
    (reachable node #(get upstream % #{}))))

(defn downstream-of [config node]
  (let [{:keys [downstream]} (build-pipeline-adjacency config)]
    (reachable node #(get downstream % #{}))))

(defn lineage-with-depth
  "Get lineage as [{:name :depth}] using adjacency map"
  [adj-map start]
  (mapv #(assoc % :name (:id %))
        (bfs start #(get adj-map % #{}))))

(defn full-lineage
  "Get upstream and downstream lineage for a pipeline node"
  [config node]
  (let [{:keys [upstream downstream]} (build-pipeline-adjacency config)]
    {:upstream (lineage-with-depth upstream node)
     :downstream (lineage-with-depth downstream node)}))

(defn full-graph-lineage
  "Get lineage including datasources"
  [config node-name]
  (let [{:keys [upstream downstream]} (build-full-adjacency config)]
    {:upstream (mapv #(assoc % :name (:id %)) (bfs node-name #(get upstream % [])))
     :downstream (mapv #(assoc % :name (:id %)) (bfs node-name #(get downstream % [])))}))

;; =============================================================================
;; Node Provenance (for API/JSON export)
;; =============================================================================

(defn node-provenance
  "Generate provenance data for a node"
  [config node-name]
  (let [{:keys [pipelines datasources]} config
        node-data (or (first (filter #(= (:name %) node-name) pipelines))
                      (first (filter #(= (:name %) node-name) datasources)))
        node-type (cond
                    (some #(= (:name %) node-name) pipelines) "pipeline"
                    (some #(= (:name %) node-name) datasources) "datasource"
                    :else "unknown")
        {:keys [upstream downstream]} (build-full-adjacency config)
        up-chain (bfs node-name #(get upstream % []))
        down-chain (bfs node-name #(get downstream % []))
        all-nodes (into #{node-name} (map :id (concat up-chain down-chain)))
        edges (for [node all-nodes
                    neighbor (get downstream node [])
                    :when (all-nodes neighbor)]
                {:from node :to neighbor})]
    {:node (-> {:name node-name :type node-type}
               (merge (select-keys node-data
                                   [:description :cluster :tags :schedule
                                    :input_sources :output_sources])))
     :upstream (mapv #(select-keys % [:id :depth]) up-chain)
     :downstream (mapv #(select-keys % [:id :depth]) down-chain)
     :edges (vec edges)}))

;; =============================================================================
;; Pipeline Groups (collapsible)
;; =============================================================================

(defn build-group-data
  "Precompute group membership and edge data for collapse/expand.
   Returns {:groups {group-name {:members [pipelines] :inputs #{} :outputs #{}}}
            :pipeline->group {pipeline-name group-name}}"
  [{:keys [pipelines]}]
  (let [;; Build membership
        pipeline->group (into {} (for [p pipelines :when (:group p)]
                                   [(:name p) (:group p)]))
        groups (reduce (fn [acc p]
                         (if-let [g (:group p)]
                           (update-in acc [g :members] (fnil conj []) (:name p))
                           acc))
                       {} pipelines)

        ;; For each group, compute external inputs/outputs
        groups (reduce-kv
                (fn [acc gname {:keys [members]}]
                  (let [member-set (set members)
                        group-pipelines (filter #(member-set (:name %)) pipelines)
                        ;; Inputs: datasources feeding this group from outside
                        inputs (->> group-pipelines
                                    (mapcat :input_sources)
                                    (remove #(member-set %))
                                    set)
                        ;; Outputs: datasources this group produces
                        outputs (->> group-pipelines
                                     (mapcat :output_sources)
                                     set)
                        ;; Upstream pipelines from outside the group
                        upstream-ext (->> group-pipelines
                                          (mapcat :upstream_pipelines)
                                          (remove member-set)
                                          set)
                        ;; Downstream: pipelines outside group that depend on this group's outputs
                        downstream-ext (->> pipelines
                                            (remove #(member-set (:name %)))
                                            (filter #(some outputs (:input_sources %)))
                                            (map :name)
                                            set)]
                    (assoc acc gname {:members members
                                      :inputs inputs
                                      :outputs outputs
                                      :upstream-pipelines upstream-ext
                                      :downstream-pipelines downstream-ext})))
                groups groups)]
    {:groups groups
     :pipeline->group pipeline->group}))

(defn all-groups
  "Get all unique group names from config"
  [{:keys [pipelines]}]
  (->> pipelines (keep :group) distinct vec))

(defn group-provenance
  "Generate provenance data for a pipeline group"
  [config group-name]
  (let [{:keys [groups]} (build-group-data config)
        group-data (get groups group-name)
        members (:members group-data)
        member-set (set members)
        member-pipelines (filter #(member-set (:name %)) (:pipelines config))
        ;; Get cluster from first member
        cluster (:cluster (first member-pipelines))
        ;; Aggregate inputs/outputs
        inputs (:inputs group-data)
        outputs (:outputs group-data)
        ;; Compute lineage for the group as a whole
        {:keys [upstream downstream]} (build-full-adjacency config)
        ;; Upstream: anything feeding into the group (not a member)
        up-chain (distinct
                  (mapcat (fn [m]
                            (->> (bfs m #(get upstream % []))
                                 (remove #(member-set (:id %)))))
                          members))
        ;; Downstream: anything the group feeds (not a member)
        down-chain (distinct
                    (mapcat (fn [m]
                              (->> (bfs m #(get downstream % []))
                                   (remove #(member-set (:id %)))))
                            members))]
    {:node {:name group-name
            :type "group"
            :members members
            :cluster cluster
            :inputs (vec inputs)
            :outputs (vec outputs)}
     :upstream (mapv #(hash-map :name (:id %) :depth (:depth %)) up-chain)
     :downstream (mapv #(hash-map :name (:id %) :depth (:depth %)) down-chain)}))

;; =============================================================================
;; Search
;; =============================================================================

(defn fuzzy-match
  "Fuzzy match text against query. Returns {:match bool :score number}"
  [text query]
  (let [tl (str/lower-case text)
        ql (str/lower-case query)]
    (cond
      (str/includes? tl ql)
      {:match true :score (+ 0.5 (/ (count query) (count text)))}

      :else
      (loop [ti 0, qi 0, score 0, last-match -1]
        (cond
          (>= qi (count ql)) {:match true :score (/ score (count text))}
          (>= ti (count tl)) {:match false :score 0}
          (= (nth tl ti) (nth ql qi))
          (let [bonus (+ (if (= last-match (dec ti)) 0.5 0)
                         (if (or (zero? ti)
                                 (#{\_ \- \space} (nth tl (dec ti)))) 0.3 0))]
            (recur (inc ti) (inc qi) (+ score 1 bonus) ti))
          :else (recur (inc ti) qi score last-match))))))

(defn highlight-match
  "Return HTML with matching chars highlighted"
  [text query]
  (let [tl (str/lower-case text)
        ql (str/lower-case query)
        idx (str/index-of tl ql)]
    (if idx
      (str (subs text 0 idx)
           "<span class=\"result-match\">"
           (subs text idx (+ idx (count query)))
           "</span>"
           (subs text (+ idx (count query))))
      (loop [ti 0, qi 0, result ""]
        (cond
          (>= ti (count text)) result
          (>= qi (count ql)) (str result (subs text ti))
          (= (nth tl ti) (nth ql qi))
          (recur (inc ti) (inc qi)
                 (str result "<span class=\"result-match\">" (nth text ti) "</span>"))
          :else (recur (inc ti) qi (str result (nth text ti))))))))

(defn search-nodes
  "Search pipelines and datasources, returns sorted results"
  [{:keys [pipelines datasources]} query]
  (when (seq (str/trim (or query "")))
    (let [q (str/trim query)
          match-score (fn [item]
                        (let [nm (fuzzy-match (:name item) q)
                              dm (if (:description item)
                                   (fuzzy-match (:description item) q)
                                   {:match false :score 0})]
                          (when (or (:match nm) (:match dm))
                            (max (* (:score nm) 1.5) (:score dm)))))
          explicit-names (set (map :name datasources))
          auto-sources (->> pipelines
                            (mapcat #(concat (:input_sources %) (:output_sources %)))
                            (remove explicit-names)
                            distinct)]
      (->> (concat
            (for [p pipelines :let [s (match-score p)] :when s]
              {:name (:name p) :type :pipeline :score s})
            (for [ds datasources :let [s (match-score ds)] :when s]
              {:name (:name ds) :type :datasource :score s})
            (for [n auto-sources :let [m (fuzzy-match n q)] :when (:match m)]
              {:name n :type :datasource :score (* (:score m) 1.5)}))
           (sort-by :score >)
           (take 8)
           vec))))

;; =============================================================================
;; DOT Generation
;; =============================================================================

(def ^:private cluster-colors
  ["#1976d2" "#7b1fa2" "#388e3c" "#f57c00" "#d32f2f" "#616161" "#c2185b" "#303f9f"])

(defn- edge-color [dark?] (if dark? "#b0b0b0" "#555"))

(defn- dot-node [type {:keys [name schedule]} _opts]
  (let [n (escape-dot name)]
    (case type
      :pipeline
      (if-let [sched (format-schedule schedule)]
        (str "\"" n "\" [shape=box style=\"filled,rounded\" fillcolor=\"#e3f2fd\" "
             "color=\"#1976d2\" label=<" n "<BR/><FONT POINT-SIZE=\"9\" COLOR=\"#d63384\">"
             "<I>" sched "</I></FONT>>];")
        (str "\"" n "\" [shape=box style=\"filled,rounded\" fillcolor=\"#e3f2fd\" color=\"#1976d2\"];"))
      :datasource
      (str "\"" n "\" [shape=ellipse style=filled fillcolor=\"#f3e5f5\" color=\"#7b1fa2\"];"))))

(defn- dot-edges [{:keys [name input_sources output_sources]} {:keys [dark?]}]
  (let [n (escape-dot name)
        color (edge-color dark?)]
    (concat
     (for [src input_sources]
       (str "\"" (escape-dot src) "\" -> \"" n "\" [color=\"" color "\" arrowsize=0.8];"))
     (for [src output_sources]
       (str "\"" n "\" -> \"" (escape-dot src) "\" [color=\"" color "\" arrowsize=0.8];")))))

(defn- dependency-edges [pipelines]
  (let [valid (set (map :name pipelines))]
    (for [p pipelines
          up (:upstream_pipelines p)
          :when (valid up)]
      (str "\"" (escape-dot up) "\" -> \"" (escape-dot (:name p))
           "\" [color=\"#ff6b35\" style=solid arrowsize=0.8];"))))

(defn- dot-group-node
  "Render a collapsed group as a single node (3D orange box)"
  [group-name member-count]
  (str "\"group:" (escape-dot group-name) "\" [shape=box3d style=filled "
       "fillcolor=\"#fff3e0\" color=\"#ff6b35\" "
       "label=<" (escape-dot group-name)
       "<BR/><FONT POINT-SIZE=\"9\" COLOR=\"#666\">" member-count " pipelines</FONT>>];"))

(defn generate-dot
  "Generate Graphviz DOT from config.
   opts:
     :show-datasources? - include datasource nodes (default true)
     :dark? - dark mode colors
     :collapsed-groups - set of group names to render as single nodes"
  ([config] (generate-dot config {}))
  ([{:keys [pipelines datasources clusters] :as config} opts]
   (let [{:keys [show-datasources? collapsed-groups]
          :or {show-datasources? true collapsed-groups #{}}} opts

         ;; Group data for collapse logic
         {:keys [groups pipeline->group]} (build-group-data config)
         collapsed? (fn [p] (collapsed-groups (:group p)))

         ;; Visible pipelines (not in collapsed groups)
         visible-pipelines (remove collapsed? pipelines)

         ;; For edges: map pipeline name to its effective node name
         effective-node (fn [pname]
                          (if-let [g (pipeline->group pname)]
                            (if (collapsed-groups g)
                              (str "group:" g)
                              pname)
                            pname))

         ;; Determine cluster for each collapsed group (from first member)
         group->cluster (into {}
                              (for [g collapsed-groups
                                    :let [members (:members (groups g))]
                                    :when (seq members)
                                    :let [first-member (first (filter #(= (:name %) (first members)) pipelines))]]
                                [g (:cluster first-member)]))

         ;; Cluster setup
         cluster-defs (into {} (map #(vector (:name %) %) (or clusters [])))
         all-cluster-names (into #{} (concat (keep :cluster visible-pipelines)
                                             (keep val group->cluster)
                                             (when show-datasources? (keep :cluster datasources))))
         cluster-defs (reduce #(if (% %2) %1 (assoc %1 %2 {:name %2}))
                              cluster-defs (remove nil? all-cluster-names))

         hierarchy (into {} (for [c clusters :when (:parent c)] [(:name c) (:parent c)]))
         children (reduce (fn [m [c p]] (update m p (fnil conj []) c)) {} hierarchy)

         ;; Group nodes by cluster (including collapsed groups)
         nodes-by-cluster (as-> {} $
                            (reduce #(update %1 (or (:cluster %2) "_none")
                                             (fnil conj []) {:type :pipeline :node %2})
                                    $ visible-pipelines)
                            (reduce (fn [acc g]
                                      (let [cluster (or (group->cluster g) "_none")]
                                        (update acc cluster (fnil conj [])
                                                {:type :group :node {:name g :members (:members (groups g))}})))
                                    $ collapsed-groups)
                            (if show-datasources?
                              (reduce #(update %1 (or (:cluster %2) "_none")
                                               (fnil conj []) {:type :datasource :node %2})
                                      $ datasources)
                              $))

         ;; Render cluster recursively
         dark? (:dark? opts)
         cluster-fontcolor (if dark? "#aaa" "#555")
         render-node (fn [item indent]
                       (case (:type item)
                         :group (str indent (dot-group-node
                                             (:name (:node item))
                                             (count (:members (:node item)))))
                         (str indent (dot-node (:type item) (:node item) opts))))
         render-cluster (fn render-cluster [cname depth]
                          (let [nodes (get nodes-by-cluster cname [])
                                kids (get children cname [])
                                indent (apply str (repeat (inc depth) "  "))]
                            (when (or (seq nodes) (seq kids))
                              (str indent "subgraph cluster_" (sanitize-id cname) " {\n"
                                   indent "  label=\"" cname "\" style=dotted color=\"#666666\" fontcolor=\"" cluster-fontcolor "\" fontsize=11 fontname=Arial\n"
                                   (str/join "\n" (map #(render-node % (str indent "  ")) nodes))
                                   "\n"
                                   (str/join "\n" (map #(render-cluster % (inc depth)) kids))
                                   indent "}\n"))))

         roots (filter #(and (not (hierarchy %))
                             (or (seq (get nodes-by-cluster %))
                                 (some (comp seq nodes-by-cluster) (get children %))))
                       (keys cluster-defs))

         unclustered (get nodes-by-cluster "_none" [])

         ;; Generate edges with collapse-aware routing
         edge-clr (edge-color dark?)
         make-edges (fn []
                      (let [seen (atom #{})]
                        (concat
                         ;; Datasource edges
                         (when show-datasources?
                           (for [p pipelines
                                 :let [target (effective-node (:name p))]
                                 src (:input_sources p)
                                 :let [edge-key [src target]]
                                 :when (not (@seen edge-key))
                                 :let [_ (swap! seen conj edge-key)]]
                             (str "\"" (escape-dot src) "\" -> \"" (escape-dot target)
                                  "\" [color=\"" edge-clr "\" arrowsize=0.8];")))
                         (when show-datasources?
                           (for [p pipelines
                                 :let [source (effective-node (:name p))]
                                 out (:output_sources p)
                                 :let [edge-key [source out]]
                                 :when (not (@seen edge-key))
                                 :let [_ (swap! seen conj edge-key)]]
                             (str "\"" (escape-dot source) "\" -> \"" (escape-dot out)
                                  "\" [color=\"" edge-clr "\" arrowsize=0.8];")))
                         ;; Pipeline dependency edges
                         (let [valid (set (map :name pipelines))]
                           (for [p pipelines
                                 up (:upstream_pipelines p)
                                 :when (valid up)
                                 :let [source (effective-node up)
                                       target (effective-node (:name p))
                                       edge-key [source target]]
                                 :when (and (not= source target)
                                            (not (@seen edge-key)))
                                 :let [_ (swap! seen conj edge-key)]]
                             (str "\"" (escape-dot source) "\" -> \"" (escape-dot target)
                                  "\" [color=\"#ff6b35\" style=solid arrowsize=0.8];")))
                         ;; Implicit edges between groups via datasources
                         (when (seq collapsed-groups)
                           (for [g collapsed-groups
                                 :let [{:keys [outputs]} (groups g)]
                                 out outputs
                                 p pipelines
                                 :when (and (some #{out} (:input_sources p))
                                            (not (collapsed-groups (:group p))))
                                 :let [source (str "group:" g)
                                       target (:name p)
                                       edge-key [source target]]
                                 :when (not (@seen edge-key))
                                 :let [_ (swap! seen conj edge-key)]]
                             (str "\"" (escape-dot source) "\" -> \"" (escape-dot target)
                                  "\" [color=\"" edge-clr "\" arrowsize=0.8];"))))))]

     (str "digraph G {\n"
          "  rankdir=LR bgcolor=transparent\n"
          "  node [fontname=Arial fontsize=12]\n"
          "  edge [fontsize=10]\n\n"
          (str/join "\n" (map #(render-cluster % 0) roots))
          "\n"
          (str/join "\n" (map #(render-node % "  ") unclustered))
          "\n"
          (str/join "\n" (map #(str "  " %) (make-edges)))
          "\n}"))))

;; =============================================================================
;; Attribute Lineage
;; =============================================================================

(defn- parse-source-ref
  "Parse 'datasource::attr::nested' into {:ds :path :id}"
  [ref]
  (let [parts (str/split ref #"::")
        ds (first parts)
        path (str/join "__" (map sanitize-id (rest parts)))]
    {:ds (sanitize-id ds) :path path :id (str (sanitize-id ds) "__" path)}))

(defn- collect-source-refs
  "Collect all :from references from nested attributes"
  [attrs]
  (reduce (fn [refs attr]
            (let [froms (cond (vector? (:from attr)) (:from attr)
                              (:from attr) [(:from attr)]
                              :else [])
                  child-refs (if (:attributes attr)
                               (collect-source-refs (:attributes attr))
                               [])]
              (into refs (concat froms child-refs))))
          #{} attrs))

(defn- flatten-attributes
  "Flatten nested attributes into [{:id :name :ds :full-name :from}]"
  [attrs ds-name ds-id prefix]
  (mapcat (fn [attr]
            (let [path (if (seq prefix) (str prefix "__" (:name attr)) (:name attr))
                  id (str ds-id "__" (sanitize-id path))
                  full-name (str ds-name "::" (str/replace path "__" "::"))
                  froms (cond (vector? (:from attr)) (:from attr)
                              (:from attr) [(:from attr)]
                              :else [])]
              (cons {:id id :name (:name attr) :ds ds-name
                     :full-name full-name :from (mapv parse-source-ref froms)}
                    (when (:attributes attr)
                      (flatten-attributes (:attributes attr) ds-name ds-id path)))))
          attrs))

(defn build-attribute-lineage-map
  "Build attribute and datasource lineage maps"
  [config]
  (let [datasources (:datasources config)

        ;; Flatten all attributes
        all-attrs (mapcat #(when (:attributes %)
                             (flatten-attributes (:attributes %) (:name %)
                                                 (sanitize-id (:name %)) ""))
                          datasources)

        ;; Build base map
        attr-map (into {} (map #(vector (:id %)
                                        (assoc % :upstream [] :downstream []))
                               all-attrs))

        ;; Add edges
        attr-map (reduce (fn [m {:keys [id from]}]
                           (let [attr-id id]
                             (reduce (fn [m' src]
                                       (let [src-id (:id src)]
                                         (-> m'
                                             (update-in [attr-id :upstream] conj src-id)
                                             (update-in [src-id :downstream] conj attr-id))))
                                     m from)))
                         attr-map all-attrs)

        ;; Add full chains
        attr-map (reduce-kv (fn [m id attr]
                              (assoc m id
                                     (assoc attr
                                            :full-upstream (bfs id #(get-in attr-map [% :upstream] []))
                                            :full-downstream (bfs id #(get-in attr-map [% :downstream] [])))))
                            {} attr-map)

        ;; Build datasource-level lineage
        ds-ids (set (map #(sanitize-id (:name %)) datasources))

        ds-upstream (reduce-kv
                     (fn [m id {:keys [upstream]}]
                       (let [ds (first (str/split id #"__"))
                             ups (->> upstream
                                      (map #(first (str/split % #"__")))
                                      (filter #(and (not= % ds) (ds-ids %)))
                                      set)]
                         (update m ds (fnil into #{}) ups)))
                     {} attr-map)

        ds-downstream (reduce-kv
                       (fn [m id {:keys [downstream]}]
                         (let [ds (first (str/split id #"__"))
                               downs (->> downstream
                                          (map #(first (str/split % #"__")))
                                          (filter #(and (not= % ds) (ds-ids %)))
                                          set)]
                           (update m ds (fnil into #{}) downs)))
                       {} attr-map)

        ds-map (into {} (for [ds datasources
                              :let [id (sanitize-id (:name ds))]]
                          [id {:name (:name ds) :ds-id id
                               :upstream (bfs id #(get ds-upstream % #{}))
                               :downstream (bfs id #(get ds-downstream % #{}))}]))]

    {:attribute-map attr-map :datasource-map ds-map}))

(defn attribute-provenance
  "Get provenance for an attribute. Supports :max-depth option."
  ([lineage-map attr-id] (attribute-provenance lineage-map attr-id {}))
  ([lineage-map attr-id {:keys [max-depth]}]
   (when-let [attr (get lineage-map attr-id)]
     (let [filter-depth (if max-depth #(filter (fn [x] (<= (:depth x) max-depth)) %) identity)
           upstream (filter-depth (:full-upstream attr))
           downstream (filter-depth (:full-downstream attr))
           enrich (fn [items]
                    (mapv #(let [a (get lineage-map (:id %))]
                             (merge % (select-keys a [:name :full-name :datasource])))
                          items))]
       {:attribute attr-id
        :name (:name attr)
        :full-name (:full-name attr)
        :datasource (:ds attr)
        :upstream {:attributes (enrich upstream)}
        :downstream {:attributes (enrich downstream)}}))))

;; =============================================================================
;; Attribute DOT Generation
;; =============================================================================

(defn generate-attribute-dot
  "Generate DOT for attribute-level lineage"
  [config]
  (let [datasources (:datasources config)
        all-refs (reduce #(if (:attributes %2)
                            (into %1 (collect-source-refs (:attributes %2)))
                            %1)
                         #{} datasources)

        has-lineage? (fn [ds-name attr-path]
                       (contains? all-refs
                                  (str ds-name "::" (str/replace attr-path "__" "::"))))

        struct-attrs (atom #{})

        render-attrs (fn render-attrs [attrs ds-name ds-id prefix depth]
                       (str/join ""
                                 (for [attr attrs
                                       :let [path (if (seq prefix)
                                                    (str prefix "__" (:name attr))
                                                    (:name attr))
                                             id (str ds-id "__" (sanitize-id path))
                                             has-lin (or (:from attr) (has-lineage? ds-name path))
                                             kids (:attributes attr)]]
                                   (if kids
                                     (do (swap! struct-attrs conj id)
                                         (str "    subgraph cluster_" id " {\n"
                                              "      label=\"" (:name attr) "\" labelloc=t style=filled\n"
                                              "      fillcolor=\"" (if has-lin "#dde5ed" "#e8eef4") "\"\n"
                                              "      fontcolor=\"" (if has-lin "#7b1fa2" "#334155") "\"\n"
                                              "      color=\"" (if has-lin "#7b1fa2" "#94a3b8") "\"\n"
                                              "      fontname=Arial fontsize=9 margin=8\n"
                                              "      \"" id "\" [label=\"\" shape=point width=0 height=0 style=invis];\n"
                                              (render-attrs kids ds-name ds-id path (inc depth))
                                              "    }\n"))
                                     (str "      \"" id "\" [label=\"" (:name attr) "\" shape=box "
                                          "style=\"filled,rounded\" fillcolor=\""
                                          (if has-lin "#e2e8f0" "#ffffff") "\" "
                                          "color=\"#94a3b8\" fontcolor=\"#334155\" fontsize=9 height=0.3];\n")))))

        collect-edges (fn collect-edges [attrs ds-id prefix]
                        (mapcat (fn [attr]
                                  (let [path (if (seq prefix)
                                               (str prefix "__" (:name attr))
                                               (:name attr))
                                        tid (str ds-id "__" (sanitize-id path))
                                        froms (cond (vector? (:from attr)) (:from attr)
                                                    (:from attr) [(:from attr)]
                                                    :else [])]
                                    (concat
                                     (for [src froms
                                           :let [{:keys [id]} (parse-source-ref src)
                                                 attrs (cond-> ["color=\"#7b1fa2\""]
                                                         (@struct-attrs id) (conj (str "ltail=\"cluster_" id "\""))
                                                         (@struct-attrs tid) (conj (str "lhead=\"cluster_" tid "\"")))]]
                                       (str "  \"" id "\" -> \"" tid "\" [" (str/join " " attrs) "];"))
                                     (when (:attributes attr)
                                       (collect-edges (:attributes attr) ds-id path)))))
                                attrs))

        clusters (str/join ""
                           (for [ds datasources :when (:attributes ds)
                                 :let [id (sanitize-id (:name ds))]]
                             (str "  subgraph cluster_" id " {\n"
                                  "    label=\"" (:name ds) "\" style=filled fillcolor=\"#f1f5f9\"\n"
                                  "    fontcolor=\"#334155\" color=\"#94a3b8\" fontname=Arial fontsize=11\n"
                                  (render-attrs (:attributes ds) (:name ds) id "" 0)
                                  "  }\n")))

        edges (str/join "\n"
                        (mapcat #(when (:attributes %)
                                   (collect-edges (:attributes %) (sanitize-id (:name %)) ""))
                                datasources))]

    (str "digraph AttributeLineage {\n"
         "  rankdir=LR bgcolor=transparent compound=true nodesep=0.15\n"
         "  node [fontname=Arial fontsize=10]\n"
         "  edge [fontsize=9 color=\"#94a3b8\" arrowsize=0.6]\n\n"
         clusters "\n" edges "\n}")))

;; =============================================================================
;; Example Config
;; =============================================================================

(def example-config
  {:clusters
   [{:name "user-processing" :description "User data processing cluster"}
    {:name "order-management" :description "Order processing cluster"}
    {:name "real-time" :description "Real-time streaming" :parent "order-management"}
    {:name "analytics" :description "Analytics and reporting cluster"}]

   :pipelines
   [{:name "user-enrichment"
     :description "Enriches user data with behavioral signals"
     :input_sources ["raw_users" "user_events"]
     :output_sources ["enriched_users"]
     :schedule "Every 2 hours"
     :cluster "user-processing"
     :tags ["user-data" "ml"]
     :links {:airflow "https://airflow.company.com/dags/user_enrichment"}}
    {:name "order-processing"
     :description "Validates and processes incoming orders"
     :input_sources ["raw_orders" "inventory"]
     :output_sources ["processed_orders" "order_audit"]
     :schedule "Every 15 minutes"
     :cluster "real-time"
     :tags ["orders" "real-time"]}
    {:name "analytics-aggregation"
     :description "Daily aggregation of user metrics"
     :input_sources ["enriched_users" "processed_orders" "user_events"]
     :output_sources ["daily_metrics" "user_cohorts"]
     :schedule "Daily at 1:00 AM"
     :cluster "analytics"
     :upstream_pipelines ["user-enrichment" "order-processing"]
     :tags ["analytics" "daily"]}
    {:name "export-to-salesforce"
     :description "Sync user cohorts to Salesforce"
     :input_sources ["user_cohorts"]
     :output_sources ["salesforce_users"]
     :group "data-exports"
     :cluster "analytics"
     :upstream_pipelines ["analytics-aggregation"]}
    {:name "export-to-hubspot"
     :description "Sync user cohorts to HubSpot"
     :input_sources ["user_cohorts"]
     :output_sources ["hubspot_contacts"]
     :group "data-exports"
     :cluster "analytics"
     :upstream_pipelines ["analytics-aggregation"]}
    {:name "export-to-amplitude"
     :description "Sync daily metrics to Amplitude"
     :input_sources ["daily_metrics"]
     :output_sources ["amplitude_events"]
     :group "data-exports"
     :cluster "analytics"
     :upstream_pipelines ["analytics-aggregation"]}
    {:name "weekly-rollup"
     :description "Weekly executive summary"
     :input_sources ["daily_metrics" "enriched_users"]
     :output_sources ["executive_summary"]
     :schedule "0 6 * * MON"
     :cluster "analytics"
     :upstream_pipelines ["analytics-aggregation" "user-enrichment"]}]

   :datasources
   [{:name "raw_users"
     :type "snowflake"
     :description "Raw user registration data"
     :cluster "user-processing"
     :owner "data-platform@company.com"
     :tags ["pii" "users"]
     :attributes [{:name "id"}
                  {:name "first_name"}
                  {:name "last_name"}
                  {:name "email"}
                  {:name "signup_date"}
                  {:name "address"
                   :attributes [{:name "city"}
                                {:name "zip"}
                                {:name "geo" :attributes [{:name "lat"} {:name "lng"}]}]}]}
    {:name "user_events"
     :type "s3"
     :description "Clickstream events"
     :cluster "analytics"
     :tags ["events" "clickstream"]
     :attributes [{:name "event_id"}
                  {:name "user_id" :from "raw_users::id"}
                  {:name "event_type"}
                  {:name "timestamp"}]}
    {:name "raw_orders" :type "api" :description "Real-time orders" :cluster "real-time"}
    {:name "inventory" :type "snowflake" :description "Inventory levels" :cluster "order-management"}
    {:name "enriched_users"
     :type "delta"
     :description "Enriched user profiles"
     :cluster "user-processing"
     :attributes [{:name "user_id" :from "raw_users::id"}
                  {:name "full_name" :from ["raw_users::first_name" "raw_users::last_name"]}
                  {:name "email" :from "raw_users::email"}
                  {:name "event_count" :from "user_events::event_id"}
                  {:name "location" :from "raw_users::address"
                   :attributes [{:name "city" :from "raw_users::address::city"}
                                {:name "zip" :from "raw_users::address::zip"}]}]}
    {:name "processed_orders" :type "snowflake" :description "Validated orders"}
    {:name "order_audit" :type "s3" :description "Order audit trail"}
    {:name "daily_metrics"
     :type "snowflake"
     :description "Daily business metrics"
     :cluster "analytics"
     :attributes [{:name "date"}
                  {:name "active_users" :from "enriched_users::user_id"}
                  {:name "total_events" :from "user_events::event_id"}]}
    {:name "user_cohorts" :type "snowflake" :description "User segments" :cluster "analytics"}
    {:name "salesforce_users" :type "api" :description "Salesforce sync" :cluster "analytics"}
    {:name "hubspot_contacts" :type "api" :description "HubSpot sync" :cluster "analytics"}
    {:name "amplitude_events" :type "api" :description "Amplitude events" :cluster "analytics"}
    {:name "executive_summary"
     :type "snowflake"
     :description "Weekly executive metrics"
     :cluster "analytics"
     :attributes [{:name "week"}
                  {:name "weekly_active_users" :from "daily_metrics::active_users"}
                  {:name "weekly_events" :from "daily_metrics::total_events"}]}]})

;; =============================================================================
;; Cron Parsing
;; =============================================================================

(def ^:private days ["Sunday" "Monday" "Tuesday" "Wednesday" "Thursday" "Friday" "Saturday"])
(def ^:private days-short ["Sun" "Mon" "Tue" "Wed" "Thu" "Fri" "Sat"])
(def ^:private months ["January" "February" "March" "April" "May" "June"
                       "July" "August" "September" "October" "November" "December"])

(defn cron?
  "Returns true if string looks like a 5-field cron expression"
  [s]
  (when (string? s)
    (let [parts (str/split (str/trim s) #"\s+")]
      (and (= 5 (count parts))
           (every? #(re-matches #"^[\d,\-\*\/A-Za-z]+$" %) parts)))))

(defn- parse-int [s]
  #?(:clj  (Integer/parseInt s)
     :cljs (js/parseInt s 10)))

(defn- find-name-index [val names]
  (let [upper (str/upper-case val)]
    (or (->> names
             (keep-indexed (fn [i n] (when (str/starts-with? (str/upper-case n) upper) i)))
             first)
        (parse-int val))))

(defn parse-field
  "Parse a cron field into a map describing its value"
  [field max-val & [names]]
  (cond
    (= "*" field) nil
    (str/starts-with? field "*/") {:every (parse-int (subs field 2))}
    (and (str/includes? field "-") (not (str/includes? field ",")))
    (let [[start end] (str/split field #"-")]
      {:range [(if names (find-name-index start names) (parse-int start))
               (if names (find-name-index end names) (parse-int end))]})
    (str/includes? field ",")
    {:list (mapv #(if names (find-name-index % names) (parse-int %))
                 (str/split field #","))}
    :else
    {:value (if names (find-name-index field names) (parse-int field))}))

(defn- format-time [hour minute]
  (let [h (let [m (mod hour 12)] (if (zero? m) 12 m))
        ampm (if (< hour 12) "AM" "PM")
        m-str (if (< minute 10) (str "0" minute) (str minute))]
    (if (= "00" m-str) (str h " " ampm) (str h ":" m-str " " ampm))))

(defn- ordinal [n]
  (let [suffixes ["th" "st" "nd" "rd"]
        v (mod n 100)
        suffix (or (get suffixes (mod (- v 20) 10)) (get suffixes v) "th")]
    (str n suffix)))

(defn format-schedule
  "Convert cron expression to human-readable string"
  [schedule]
  (if-not (cron? schedule)
    schedule
    (let [parts (str/split (str/trim schedule) #"\s+")
          [minute hour dom month dow] parts
          min-p  (parse-field minute 59)
          hour-p (parse-field hour 23)
          dom-p  (parse-field dom 31)
          mon-p  (parse-field month 12 months)
          dow-p  (parse-field dow 6 days-short)]
      (cond
        (and (:every min-p) (nil? hour-p) (nil? dom-p) (nil? mon-p) (nil? dow-p))
        (str "Every " (:every min-p) " minutes")

        (and (:every hour-p) (or (= "0" minute) (= "*" minute))
             (nil? dom-p) (nil? mon-p) (nil? dow-p))
        (str "Every " (:every hour-p) " hours")

        :else
        (let [time-str (cond
                         (and (:value hour-p) (:value min-p))
                         (format-time (:value hour-p) (:value min-p))
                         (:value hour-p)
                         (format-time (:value hour-p) 0)
                         (and (:value min-p) (not= "0" minute))
                         (str ":" (if (< (:value min-p) 10) (str "0" (:value min-p)) (:value min-p)))
                         :else nil)]
          (cond
            (and dow-p (nil? dom-p) (nil? mon-p))
            (let [base (cond
                         (:range dow-p)
                         (let [[s e] (:range dow-p)]
                           (cond (and (= 1 s) (= 5 e)) "Weekdays"
                                 (and (= 0 s) (= 6 e)) "Daily"
                                 :else (str (days s) "-" (days e))))
                         (:list dow-p) (str/join ", " (map days-short (:list dow-p)))
                         (:value dow-p) (str (days (:value dow-p)) "s"))]
              (if time-str (str base " at " time-str) base))

            (and dom-p (nil? dow-p))
            (let [base (when (:value dom-p)
                         (str (ordinal (:value dom-p)) " of "
                              (if (:value mon-p) (months (dec (:value mon-p))) "each month")))]
              (if time-str (str base " at " time-str) base))

            (and (nil? dom-p) (nil? dow-p) (nil? mon-p) time-str)
            (str "Daily at " time-str)

            :else schedule))))))

;; =============================================================================
;; Splash Graph
;; =============================================================================

(defn generate-splash-dot
  "Generate the simple splash graph DOT for hero section"
  [_opts]
  (str "digraph {
    rankdir=LR
    bgcolor=\"transparent\"
    node [fontname=\"Arial\" fontsize=\"10\" fontcolor=\"#333333\"]
    edge [color=\"#999999\"]
    \"raw_events\" [shape=ellipse style=filled fillcolor=\"#f3e5f5\" color=\"#7b1fa2\"]
    \"etl-job\" [shape=box style=\"filled,rounded\" fillcolor=\"#e3f2fd\" color=\"#1976d2\"]
    \"cleaned_events\" [shape=ellipse style=filled fillcolor=\"#f3e5f5\" color=\"#7b1fa2\"]
    \"raw_events\" -> \"etl-job\"
    \"etl-job\" -> \"cleaned_events\"
}"))
