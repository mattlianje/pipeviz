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

(defn kahns-waves
  "Kahn's algorithm returning nodes grouped into execution waves.
   Nodes in same wave can execute in parallel."
  [nodes edges]
  (let [in-degree (reduce (fn [m [_ to]]
                            (if (nodes to) (update m to (fnil inc 0)) m))
                          (zipmap nodes (repeat 0))
                          edges)
        adj (reduce (fn [m [from to]]
                      (if (and (nodes from) (nodes to))
                        (update m from (fnil conj []) to)
                        m))
                    {} edges)]
    (loop [waves [] degrees in-degree]
      (let [ready (sort (keys (filter (fn [[_ d]] (zero? d)) degrees)))]
        (if (empty? ready)
          (if (empty? degrees) waves nil)
          (recur (conj waves (vec ready))
                 (reduce (fn [m node]
                           (reduce (fn [m2 neighbor]
                                     (if (contains? m2 neighbor)
                                       (update m2 neighbor dec) m2))
                                   (dissoc m node)
                                   (get adj node [])))
                         degrees ready)))))))

(defn build-downstream-edges
  "Build downstream edges from pipeline definitions."
  [pipelines]
  (let [output->producer (into {} (for [p pipelines
                                        out (:output_sources p)]
                                    [out (:name p)]))]
    (distinct
     (concat
      (for [p pipelines, up (:upstream_pipelines p)] [up (:name p)])
      (for [p pipelines
            input (:input_sources p)
            :let [producer (get output->producer input)]
            :when (and producer (not= producer (:name p)))]
        [producer (:name p)])))))

(defn build-edge-adjacency
  "Build upstream and downstream adjacency maps from edges."
  [edges]
  (reduce (fn [acc [from to]]
            (-> acc
                (update-in [:downstream from] (fnil conj #{}) to)
                (update-in [:upstream to] (fnil conj #{}) from)))
          {:upstream {} :downstream {}}
          edges))

(defn compute-execution-waves
  "Compute execution waves for selected pipelines using Kahn's algorithm."
  [pipelines selected-names]
  (when (seq selected-names)
    (let [edges (build-downstream-edges pipelines)
          {:keys [downstream]} (build-edge-adjacency edges)
          downstream-of (into {}
                              (for [node selected-names]
                                [node (reachable node #(get downstream % #{}))]))
          affected (reduce into (set selected-names) (vals downstream-of))
          relevant-edges (filter (fn [[from to]]
                                   (and (affected from) (affected to)))
                                 edges)
          waves (kahns-waves affected relevant-edges)]
      {:waves (or waves [])
       :edges (vec relevant-edges)
       :node-count (count affected)})))

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

         ;; Collect implicit datasources (referenced but not defined)
         explicit-ds-names (set (map :name datasources))
         implicit-datasources (->> pipelines
                                   (mapcat #(concat (:input_sources %) (:output_sources %)))
                                   distinct
                                   (remove explicit-ds-names)
                                   (map #(hash-map :name %)))

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
                              $)
                            ;; Add implicit datasources (no cluster, so goes to _none)
                            (if show-datasources?
                              (reduce #(update %1 "_none"
                                               (fnil conj []) {:type :datasource :node %2})
                                      $ implicit-datasources)
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
;; Blast Radius Analysis
;; =============================================================================

(defn build-blast-radius-graph
  "Build the downstream adjacency graph and node types map from config.
   This is precomputed once per config and reused for all blast radius queries.
   Returns {:downstream {node -> #{neighbors}}, :node-types {node -> :pipeline|:datasource},
            :pipelines [...], :groups {group-name -> [member-names]}}"
  [config]
  (when config
    (let [pipelines (or (:pipelines config) [])
          datasources (or (:datasources config) [])
          downstream (atom {})
          node-types (atom {})]

      ;; Build graph from pipelines
      (doseq [p pipelines]
        (swap! node-types assoc (:name p) :pipeline)
        (when-not (get @downstream (:name p))
          (swap! downstream assoc (:name p) #{}))

        ;; pipeline -> output sources
        (doseq [s (:output_sources p)]
          (swap! downstream update (:name p) (fnil conj #{}) s)
          (when-not (get @node-types s)
            (swap! node-types assoc s :datasource)))

        ;; input sources -> pipeline
        (doseq [s (:input_sources p)]
          (when-not (get @downstream s)
            (swap! downstream assoc s #{}))
          (swap! downstream update s (fnil conj #{}) (:name p))
          (when-not (get @node-types s)
            (swap! node-types assoc s :datasource)))

        ;; upstream pipelines -> this pipeline
        (doseq [u (:upstream_pipelines p)]
          (when-not (get @downstream u)
            (swap! downstream assoc u #{}))
          (swap! downstream update u (fnil conj #{}) (:name p))))

      ;; Add datasources not referenced by pipelines
      (doseq [ds datasources]
        (when-not (get @node-types (:name ds))
          (swap! node-types assoc (:name ds) :datasource)))

      ;; Build groups map
      (let [groups (->> pipelines
                        (filter :group)
                        (group-by :group)
                        (reduce-kv (fn [m k v] (assoc m k (mapv :name v))) {}))]
        {:downstream @downstream
         :node-types @node-types
         :pipelines pipelines
         :groups groups}))))

(defn blast-radius-for-node
  "Compute blast radius for a specific node using precomputed graph.
   Much faster than generate-blast-radius-analysis when called multiple times."
  [{:keys [downstream node-types pipelines groups]} node-name]
  (when node-name
    (let [;; Check if this is a pipeline group
          group-members (get groups node-name)
          is-group? (seq group-members)

          ;; BFS starting nodes
          start-nodes (if is-group? group-members [node-name])
          group-member-names (set start-nodes)

          ;; BFS to find all downstream nodes
          visited (atom (into {} (map #(vector % 0) start-nodes)))
          edges (atom [])
          queue (atom (vec (map #(vector % 0) start-nodes)))]

      (while (seq @queue)
        (let [[current depth] (first @queue)]
          (swap! queue #(vec (rest %)))
          (let [neighbors (get downstream current #{})]
            (doseq [neighbor neighbors]
              ;; For groups, don't add edges between group members
              (when-not (and is-group?
                             (contains? group-member-names current)
                             (contains? group-member-names neighbor))
                (swap! edges conj {:source (if is-group? node-name current)
                                   :target neighbor})
                (when-not (contains? @visited neighbor)
                  (swap! visited assoc neighbor (inc depth))
                  (swap! queue conj [neighbor (inc depth)])))))))

      ;; Build downstream nodes list (excluding source/group members)
      (let [downstream-nodes
            (->> @visited
                 (remove (fn [[node _]]
                           (if is-group?
                             (contains? group-member-names node)
                             (= node node-name))))
                 (map (fn [[node depth]]
                        (let [node-type (get node-types node :unknown)
                              pipeline (first (filter #(= (:name %) node) pipelines))
                              base {:name node :type node-type :depth depth}]
                          (cond-> base
                            (:schedule pipeline) (assoc :schedule (:schedule pipeline))
                            (:cluster pipeline) (assoc :cluster (:cluster pipeline))))))
                 (sort-by (juxt :depth :name))
                 vec)

            by-depth (->> downstream-nodes
                          (group-by :depth)
                          (into (sorted-map)))

            max-depth (if (seq downstream-nodes)
                        (apply max (map :depth downstream-nodes))
                        0)

            result {:source node-name
                    :source-type (if is-group? :group (get node-types node-name :unknown))
                    :total-affected (count downstream-nodes)
                    :max-depth max-depth
                    :downstream downstream-nodes
                    :by-depth by-depth
                    :edges (vec (distinct @edges))}]

        (if is-group?
          (assoc result
                 :group-members group-members
                 :group-size (count group-members))
          result)))))

(defn precompute-all-blast-radius
  "Precompute blast radius for all nodes and groups in the config.
   Returns {:graph <precomputed-graph>, :results {node-name -> analysis}}"
  [config]
  (when config
    (let [graph (build-blast-radius-graph config)
          all-nodes (keys (:node-types graph))
          all-groups (keys (:groups graph))
          all-targets (concat all-nodes all-groups)]
      {:graph graph
       :results (into {}
                      (for [node-name all-targets]
                        [node-name (blast-radius-for-node graph node-name)]))})))

(defn generate-blast-radius-analysis
  "Generate blast radius analysis for a node. Can use precomputed data for speed.
   If blast-data is provided (from precompute-all-blast-radius), uses cached result.
   Otherwise computes on-the-fly."
  ([config node-name]
   (generate-blast-radius-analysis config node-name nil))
  ([config node-name blast-data]
   (if-let [cached (get-in blast-data [:results node-name])]
     cached
     ;; Compute on-the-fly
     (when-let [graph (or (:graph blast-data) (build-blast-radius-graph config))]
       (blast-radius-for-node graph node-name)))))

(defn generate-blast-radius-dot
  "Generate DOT syntax for blast radius visualization"
  [analysis dark?]
  (when (and analysis (pos? (:total-affected analysis)))
    (let [bg-color (if dark? "#1a1a1a" "#ffffff")
          text-color (if dark? "#b0b0b0" "#666666")
          edge-color (if dark? "#666666" "#999999")

          depth-colors (if dark?
                         [{:fill "#4a2a2a" :border "#c98b8b" :text "#e0e0e0"}  ; Source
                          {:fill "#4a3a2a" :border "#d4a574" :text "#e0e0e0"}  ; Depth 1
                          {:fill "#4a4a2a" :border "#c4c474" :text "#e0e0e0"}  ; Depth 2
                          {:fill "#2a4a3a" :border "#7cb47c" :text "#e0e0e0"}  ; Depth 3
                          {:fill "#2a3a4a" :border "#6b9dc4" :text "#e0e0e0"}  ; Depth 4
                          {:fill "#3a2a4a" :border "#a88bc4" :text "#e0e0e0"}] ; Depth 5+
                         [{:fill "#fce4ec" :border "#c98b8b" :text "#495057"}
                          {:fill "#fff3e0" :border "#d4a574" :text "#495057"}
                          {:fill "#fffde7" :border "#c4c474" :text "#495057"}
                          {:fill "#e8f5e9" :border "#7cb47c" :text "#495057"}
                          {:fill "#e3f2fd" :border "#6b9dc4" :text "#495057"}
                          {:fill "#f3e5f5" :border "#a88bc4" :text "#495057"}])

          source-color (first depth-colors)
          is-group? (= :group (:source-type analysis))
          source-shape (if (= :datasource (:source-type analysis)) "ellipse" "box")
          source-label (if is-group?
                         (str (:source analysis) "\\n(" (:group-size analysis) " pipelines)")
                         (:source analysis))
          source-border (if is-group? "#00897b" (:border source-color))

          ;; Build source node
          source-node (str "    \"" (:source analysis) "\" [label=\"" source-label
                           "\" shape=\"" source-shape
                           "\" fillcolor=\"" (:fill source-color)
                           "\" color=\"" source-border
                           "\" fontcolor=\"" (:text source-color) "\" penwidth=\"2\"]\n\n")

          ;; Build depth clusters
          depth-clusters
          (str/join "\n"
                    (for [[depth nodes] (:by-depth analysis)]
                      (let [color-idx (min depth (dec (count depth-colors)))
                            colors (nth depth-colors color-idx)
                            node-strs (str/join "\n"
                                                (for [node nodes]
                                                  (let [shape (if (= :datasource (:type node)) "ellipse" "box")]
                                                    (str "        \"" (:name node) "\" [label=\"" (:name node)
                                                         "\" shape=\"" shape
                                                         "\" fillcolor=\"" (:fill colors)
                                                         "\" color=\"" (:border colors)
                                                         "\" fontcolor=\"" (:text colors) "\"]"))))]
                        (str "    subgraph cluster_depth" depth " {\n"
                             "        label=\"Depth " depth "\"\n"
                             "        fontname=\"Helvetica\"\n"
                             "        style=\"dashed\"\n"
                             "        color=\"" (:border colors) "\"\n"
                             "        fontcolor=\"" text-color "\"\n"
                             "        fontsize=\"9\"\n\n"
                             node-strs "\n"
                             "    }"))))

          ;; Build edges (deduplicated)
          edge-strs (->> (:edges analysis)
                         (map (fn [{:keys [source target]}]
                                (str "    \"" source "\" -> \"" target "\"")))
                         distinct
                         (str/join "\n"))]

      (str "digraph BlastRadius {\n"
           "    rankdir=LR\n"
           "    bgcolor=\"" bg-color "\"\n"
           "    fontname=\"Helvetica\"\n"
           "    node [fontname=\"Helvetica\" fontsize=\"9\" style=\"filled\"]\n"
           "    edge [color=\"" edge-color "\" arrowsize=\"0.6\"]\n\n"
           source-node
           depth-clusters "\n\n"
           edge-strs "\n"
           "}\n"))))

;; =============================================================================
;; Backfill/Planner Analysis (using pure core functions)
;; =============================================================================

(defn generate-backfill-analysis
  "Generate backfill analysis for selected pipelines using Kahn's topological sort.
   Returns execution waves (stages) where pipelines in same wave can run in parallel.

   Uses pure functions - no atoms, no side effects."
  [config selected-pipelines]
  (when (and config (seq selected-pipelines))
    (let [pipelines (or (:pipelines config) [])
          pipeline-map (into {} (map (juxt :name identity) pipelines))
          pipeline-names (set (map :name pipelines))

          ;; Validate: only allow pipelines
          invalid-nodes (remove pipeline-names selected-pipelines)
          _ (when (seq invalid-nodes)
              (throw (ex-info "Backfill only works for pipelines" {:invalid invalid-nodes})))

          ;; Compute execution waves using Kahn's algorithm
          {:keys [waves edges node-count]}
          (compute-execution-waves pipelines selected-pipelines)

          ;; Build result with pipeline metadata
          wave-data (map-indexed
                     (fn [idx wave-nodes]
                       {:wave idx
                        :parallel-count (count wave-nodes)
                        :pipelines (mapv (fn [n]
                                          (let [p (get pipeline-map n)]
                                            (cond-> {:name n}
                                              (:schedule p) (assoc :schedule (format-schedule (:schedule p)))
                                              (:cluster p) (assoc :cluster (:cluster p))
                                              (:owner p) (assoc :owner (:owner p)))))
                                        wave-nodes)})
                     waves)]

      {:nodes (vec selected-pipelines)
       :total-downstream-pipelines (max 0 (- node-count (count selected-pipelines)))
       :total-waves (count waves)
       :max-parallelism (if (seq waves) (apply max (map count waves)) 0)
       :waves (vec wave-data)
       :edges (mapv (fn [[from to]] {:source from :target to}) edges)})))

(def ^:private wave-colors
  [{:fill "#e8f5e9" :border "#81c784" :text "#495057"}   ; Green
   {:fill "#e3f2fd" :border "#64b5f6" :text "#495057"}   ; Blue
   {:fill "#f3e5f5" :border "#ba68c8" :text "#495057"}   ; Purple
   {:fill "#fff3e0" :border "#ffb74d" :text "#495057"}   ; Orange
   {:fill "#e0f7fa" :border "#4dd0e1" :text "#495057"}   ; Cyan
   {:fill "#fce4ec" :border "#f48fb1" :text "#495057"}]) ; Pink

(def ^:private wave-colors-dark
  [{:fill "#1b3d1b" :border "#81c784" :text "#e0e0e0"}
   {:fill "#1a2d3d" :border "#64b5f6" :text "#e0e0e0"}
   {:fill "#2d1a2d" :border "#ba68c8" :text "#e0e0e0"}
   {:fill "#3d2d1a" :border "#ffb74d" :text "#e0e0e0"}
   {:fill "#1a3d3d" :border "#4dd0e1" :text "#e0e0e0"}
   {:fill "#3d1a2d" :border "#f48fb1" :text "#e0e0e0"}])

(def ^:private wave0-color
  {:fill "#fef3e2" :border "#d4915c" :text "#495057"})

(def ^:private wave0-color-dark
  {:fill "#3d2d1a" :border "#d4915c" :text "#e0e0e0"})

(defn generate-backfill-dot
  "Generate DOT syntax for backfill execution plan visualization"
  [analysis dark?]
  (when (and analysis (pos? (:total-waves analysis)))
    (let [bg-color (if dark? "#1a1a1a" "#ffffff")
          text-color (if dark? "#b0b0b0" "#666666")
          edge-color (if dark? "#666666" "#999999")
          colors (if dark? wave-colors-dark wave-colors)
          w0-color (if dark? wave0-color-dark wave0-color)

          ;; Build wave clusters
          wave-clusters
          (str/join "\n"
                    (for [{:keys [wave pipelines]} (:waves analysis)]
                      (let [color (if (zero? wave)
                                    w0-color
                                    (nth colors (mod (dec wave) (count colors))))
                            node-strs (str/join "\n"
                                                (for [p pipelines]
                                                  (str "        \"" (:name p) "\" [label=\"" (:name p)
                                                       "\" fillcolor=\"" (:fill color)
                                                       "\" color=\"" (:border color)
                                                       "\" fontcolor=\"" (:text color) "\"]")))]
                        (str "    subgraph cluster_wave" wave " {\n"
                             "        label=\"Wave " wave "\"\n"
                             "        style=\"dashed\"\n"
                             "        color=\"" (:border color) "\"\n"
                             "        fontcolor=\"" text-color "\"\n"
                             "        fontsize=\"9\"\n\n"
                             node-strs "\n"
                             "    }"))))

          ;; Build edges
          edge-strs (->> (:edges analysis)
                         (map (fn [{:keys [source target]}]
                                (str "    \"" source "\" -> \"" target "\"")))
                         distinct
                         (str/join "\n"))]

      (str "digraph Backfill {\n"
           "    rankdir=LR\n"
           "    bgcolor=\"" bg-color "\"\n"
           "    fontname=\"Helvetica\"\n"
           "    node [fontname=\"Helvetica\" fontsize=\"9\" style=\"filled\" shape=\"box\"]\n"
           "    edge [color=\"" edge-color "\" arrowsize=\"0.6\"]\n\n"
           wave-clusters "\n\n"
           edge-strs "\n"
           "}\n"))))

(defn generate-airflow-analysis
  "Transform backfill analysis to Airflow DAG view"
  [config backfill-analysis]
  (when backfill-analysis
    (let [pipelines (or (:pipelines config) [])
          pipeline-map (into {} (map (juxt :name identity) pipelines))

          ;; Extract DAG name from airflow URL
          extract-dag (fn [p]
                        (when-let [url (get-in p [:links :airflow])]
                          (when-let [match (re-find #"/dags?/([^/?\s]+)" url)]
                            (second match))))

          ;; Build pipeline -> dag mapping
          pipeline-to-dag (into {}
                                (for [[name p] pipeline-map
                                      :let [dag (extract-dag p)]
                                      :when dag]
                                  [name dag]))

          ;; Transform waves to DAG waves
          dag-waves
          (for [{:keys [wave pipelines]} (:waves backfill-analysis)]
            (let [dag-groups (group-by #(get pipeline-to-dag (:name %) (:name %)) pipelines)]
              {:wave wave
               :parallel-count (count dag-groups)
               :dags (vec (for [[dag-name pips] dag-groups]
                            (let [p (first pips)
                                  url (get-in (get pipeline-map (:name p)) [:links :airflow])]
                              {:dag dag-name
                               :airflow-url url
                               :pipelines (mapv :name pips)
                               :missing (nil? url)})))}))

          ;; Transform edges to DAG edges
          dag-edges (->> (:edges backfill-analysis)
                         (map (fn [{:keys [source target]}]
                                {:source-dag (get pipeline-to-dag source source)
                                 :target-dag (get pipeline-to-dag target target)}))
                         (filter #(not= (:source-dag %) (:target-dag %)))
                         distinct
                         vec)]

      {:view :airflow
       :total-dags (count (distinct (map #(get pipeline-to-dag % %) (:nodes backfill-analysis))))
       :total-waves (count dag-waves)
       :waves (vec dag-waves)
       :edges dag-edges})))

(defn generate-airflow-dot
  "Generate DOT syntax for Airflow DAG visualization"
  [analysis dark?]
  (when (and analysis (pos? (:total-waves analysis)))
    (let [bg-color (if dark? "#1a1a1a" "#ffffff")
          text-color (if dark? "#b0b0b0" "#666666")
          edge-color (if dark? "#666666" "#999999")
          colors (if dark? wave-colors-dark wave-colors)
          w0-color (if dark? wave0-color-dark wave0-color)

          ;; Build wave clusters
          wave-clusters
          (str/join "\n"
                    (for [{:keys [wave dags]} (:waves analysis)]
                      (let [color (if (zero? wave)
                                    w0-color
                                    (nth colors (mod (dec wave) (count colors))))
                            node-strs (str/join "\n"
                                                (for [d dags]
                                                  (let [label (if (> (count (:pipelines d)) 1)
                                                                (str (:dag d) "\\n(" (count (:pipelines d)) " pipelines)")
                                                                (:dag d))
                                                        url (:airflow-url d)]
                                                    (str "        \"" (:dag d) "\" [label=\"" label
                                                         "\" fillcolor=\"" (:fill color)
                                                         "\" color=\"" (if (:missing d) "#cccccc" (:border color))
                                                         "\" fontcolor=\"" (:text color) "\""
                                                         (when (:missing d) " style=\"filled,dashed\"")
                                                         (when (and url (not (:missing d)))
                                                           (str " href=\"" url "\" target=\"_blank\" tooltip=\"Open in Airflow\""))
                                                         "]"))))]
                        (str "    subgraph cluster_wave" wave " {\n"
                             "        label=\"Wave " wave "\"\n"
                             "        style=\"dashed\"\n"
                             "        color=\"" (:border color) "\"\n"
                             "        fontcolor=\"" text-color "\"\n"
                             "        fontsize=\"9\"\n\n"
                             node-strs "\n"
                             "    }"))))

          ;; Build edges
          edge-strs (->> (:edges analysis)
                         (map (fn [{:keys [source-dag target-dag]}]
                                (str "    \"" source-dag "\" -> \"" target-dag "\"")))
                         distinct
                         (str/join "\n"))]

      (str "digraph Airflow {\n"
           "    rankdir=LR\n"
           "    bgcolor=\"" bg-color "\"\n"
           "    fontname=\"Helvetica\"\n"
           "    node [fontname=\"Helvetica\" fontsize=\"9\" style=\"filled\" shape=\"box\"]\n"
           "    edge [color=\"" edge-color "\" arrowsize=\"0.6\"]\n\n"
           wave-clusters "\n\n"
           edge-strs "\n"
           "}\n"))))

;; =============================================================================
;; Example Config
;; =============================================================================

(def example-config
  {:clusters
   [{:name "user-processing" :description "User data processing and enrichment cluster"}
    {:name "order-management" :description "Order processing and validation cluster"}
    {:name "real-time" :description "Real-time streaming data cluster" :parent "order-management"}
    {:name "analytics" :description "Analytics and reporting cluster"}]

   :pipelines
   [{:name "user-enrichment"
     :description "Enriches user data with behavioral signals and ML features"
     :input_sources ["raw_users" "user_events"]
     :output_sources ["enriched_users"]
     :schedule "Every 2 hours"
     :duration 45
     :cost 18.5
     :cluster "user-processing"
     :tags ["user-data" "ml" "enrichment"]
     :links {:airflow "https://airflow.company.com/dags/user_enrichment"
             :monitoring "https://grafana.company.com/d/user-enrichment"
             :docs "https://docs.company.com/pipelines/user-enrichment"}}
    {:name "order-processing"
     :description "Validates and processes incoming orders in real-time"
     :input_sources ["raw_orders" "inventory"]
     :output_sources ["processed_orders" "order_audit"]
     :schedule "Every 15 minutes"
     :duration 12
     :cost 4.2
     :cluster "real-time"
     :tags ["orders" "real-time" "validation"]
     :links {:airflow "https://airflow.company.com/dags/order_processing"
             :monitoring "https://grafana.company.com/d/orders"
             :alerts "https://pagerduty.company.com/services/orders"}}
    {:name "analytics-aggregation"
     :description "Daily aggregation of user metrics and business KPIs"
     :input_sources ["enriched_users" "processed_orders" "user_events"]
     :output_sources ["daily_metrics" "user_cohorts"]
     :schedule "Daily at 1:00 AM"
     :duration 90
     :cost 42.0
     :cluster "analytics"
     :upstream_pipelines ["user-enrichment" "order-processing"]
     :tags ["analytics" "aggregation" "daily"]
     :links {:airflow "https://airflow.company.com/dags/analytics_agg"
             :dashboard "https://tableau.company.com/analytics-dashboard"}}
    {:name "export-to-salesforce"
     :description "Sync user cohorts to Salesforce"
     :input_sources ["user_cohorts"]
     :output_sources ["salesforce_users"]
     :group "data-exports"
     :cluster "analytics"
     :duration 8
     :cost 2.1
     :upstream_pipelines ["analytics-aggregation"]
     :links {:airflow "https://airflow.company.com/dags/crm_exports"}}
    {:name "export-to-hubspot"
     :description "Sync user cohorts to HubSpot"
     :input_sources ["user_cohorts"]
     :output_sources ["hubspot_contacts"]
     :group "data-exports"
     :cluster "analytics"
     :duration 5
     :cost 1.8
     :upstream_pipelines ["analytics-aggregation"]
     :links {:airflow "https://airflow.company.com/dags/crm_exports"}}
    {:name "export-to-amplitude"
     :description "Sync daily metrics to Amplitude"
     :input_sources ["daily_metrics"]
     :output_sources ["amplitude_events"]
     :group "data-exports"
     :cluster "analytics"
     :duration 3
     :cost 0.9
     :upstream_pipelines ["analytics-aggregation"]
     :links {:airflow "https://airflow.company.com/dags/analytics_exports"}}
    {:name "weekly-rollup"
     :description "Aggregate daily metrics into weekly executive summary"
     :input_sources ["daily_metrics" "enriched_users"]
     :output_sources ["executive_summary"]
     :schedule "0 6 * * MON"
     :duration 120
     :cost 65.0
     :cluster "analytics"
     :upstream_pipelines ["analytics-aggregation" "user-enrichment"]
     :links {:airflow "https://airflow.company.com/dags/weekly_rollup"}}]

   :datasources
   [{:name "raw_users"
     :type "snowflake"
     :description "Raw user registration and profile data from production database"
     :cluster "user-processing"
     :owner "data-platform@company.com"
     :tags ["pii" "users" "core-data"]
     :attributes [{:name "id"}
                  {:name "first_name"}
                  {:name "last_name"}
                  {:name "email"}
                  {:name "signup_date"}
                  {:name "address"
                   :attributes [{:name "city"}
                                {:name "zip"}
                                {:name "geo" :attributes [{:name "lat"} {:name "lng"}]}]}]
     :metadata {:schema "RAW_DATA"
                :table "USERS"
                :size "2.1TB"
                :record_count "45M"
                :refresh_frequency "real-time"}
     :links {:snowflake "https://company.snowflakecomputing.com/console#/data/databases/PROD/schemas/RAW_DATA/table/USERS"
             :monitoring "https://grafana.company.com/d/raw-users"
             :docs "https://docs.company.com/schemas/raw_users"}}
    {:name "user_events"
     :type "s3"
     :description "Clickstream and interaction events from all digital touchpoints"
     :cluster "analytics"
     :owner "analytics-team@company.com"
     :tags ["events" "clickstream" "large-dataset"]
     :attributes [{:name "event_id"}
                  {:name "user_id" :from "raw_users::id"}
                  {:name "event_type"}
                  {:name "page_url"}
                  {:name "timestamp"}]
     :metadata {:bucket "company-events-prod"
                :size "15TB"
                :record_count "2.5B"
                :file_format "parquet"}
     :links {:s3 "https://s3.console.aws.amazon.com/s3/buckets/company-events-prod"
             :athena "https://console.aws.amazon.com/athena/home#query"}}
    {:name "raw_orders"
     :type "api"
     :description "Real-time order data from e-commerce platform"
     :cluster "real-time"
     :owner "platform-team@company.com"
     :tags ["orders" "real-time" "revenue"]
     :metadata {:endpoint "https://api.company.com/v2/orders"
                :rate_limit "1000 req/min"
                :record_count "120M"}
     :links {:api_docs "https://docs.company.com/api/orders"
             :monitoring "https://grafana.company.com/d/orders-api"}}
    {:name "inventory"
     :type "snowflake"
     :description "Product inventory levels across all warehouses"
     :cluster "order-management"
     :owner "supply-chain@company.com"
     :tags ["inventory" "warehouse" "operational"]
     :metadata {:schema "INVENTORY"
                :size "150GB"
                :refresh_frequency "every 15 minutes"}
     :links {:snowflake "https://company.snowflakecomputing.com/console#/data/databases/PROD/schemas/INVENTORY"
             :tableau "https://tableau.company.com/views/inventory-dashboard"}}
    {:name "enriched_users"
     :type "delta"
     :description "Enriched user profiles with behavioral features"
     :cluster "user-processing"
     :owner "data-platform@company.com"
     :tags ["users" "enriched" "ml-ready"]
     :attributes [{:name "user_id" :from "raw_users::id"}
                  {:name "full_name" :from ["raw_users::first_name" "raw_users::last_name"]}
                  {:name "email" :from "raw_users::email"}
                  {:name "event_count" :from "user_events::event_id"}
                  {:name "last_active" :from "user_events::timestamp"}
                  {:name "signup_date" :from "raw_users::signup_date"}
                  {:name "location" :from "raw_users::address"
                   :attributes [{:name "city" :from "raw_users::address::city"}
                                {:name "zip" :from "raw_users::address::zip"}
                                {:name "coords" :from "raw_users::address::geo"}]}]}
    {:name "daily_metrics"
     :type "snowflake"
     :description "Aggregated daily business metrics"
     :cluster "analytics"
     :owner "analytics-team@company.com"
     :tags ["metrics" "daily" "kpi"]
     :attributes [{:name "date"}
                  {:name "active_users" :from "enriched_users::user_id"}
                  {:name "total_events" :from "user_events::event_id"}]}
    {:name "user_cohorts"
     :type "snowflake"
     :description "User segmentation and cohort definitions"
     :cluster "analytics"}
    {:name "salesforce_users"
     :type "api"
     :description "User data synced to Salesforce CRM"
     :cluster "analytics"}
    {:name "hubspot_contacts"
     :type "api"
     :description "Contact data synced to HubSpot"
     :cluster "analytics"}
    {:name "amplitude_events"
     :type "api"
     :description "Product analytics events sent to Amplitude"
     :cluster "analytics"}
    {:name "executive_summary"
     :type "snowflake"
     :description "Weekly executive dashboard metrics"
     :cluster "analytics"
     :attributes [{:name "week"}
                  {:name "weekly_active_users" :from "daily_metrics::active_users"}
                  {:name "weekly_events" :from "daily_metrics::total_events"}
                  {:name "user_growth" :from ["daily_metrics::active_users" "enriched_users::signup_date"]}]}]})

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
