(ns pipeviz.core.graph
    "Pure domain logic - graph algorithms, validation, DOT generation"
    (:require [clojure.string :as str]
              [pipeviz.core.styles :as styles]))

(declare format-schedule)

;; Validation
(defn valid-pipeline? [p] (and (map? p) (string? (:name p))))

(defn validate-config [{:keys [pipelines] :as config}]
      (let [errors (cond-> []
                     (nil? config) (conj "Config is nil")
                     (not (seq pipelines)) (conj "No pipelines defined")
                     (some #(nil? (:name %)) pipelines) (conj "Pipeline missing name"))]
           {:valid? (empty? errors) :errors errors}))

;; Utilities
(defn sanitize-id [s] (str/replace (or s "") #"[^a-zA-Z0-9_]" "_"))
(defn escape-dot [s] (-> s (str/replace "\\" "\\\\") (str/replace "\"" "\\\"")))

;; Graph Traversal
(defn bfs
      "Generic BFS. Returns [{:id :depth}]"
      [start neighbors-fn]
      (loop [result [], visited #{start}
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

(defn reachable [start neighbors-fn]
      (set (map :id (bfs start neighbors-fn))))

(defn kahns-waves
      "Kahn's algorithm returning execution waves"
      [nodes edges]
      (let [in-degree (reduce (fn [m [_ to]] (if (nodes to) (update m to (fnil inc 0)) m))
                              (zipmap nodes (repeat 0)) edges)
            adj (reduce (fn [m [from to]]
                            (if (and (nodes from) (nodes to))
                                (update m from (fnil conj []) to) m))
                        {} edges)]
           (loop [waves [] degrees in-degree]
                 (let [ready (sort (keys (filter #(zero? (val %)) degrees)))]
                      (if (empty? ready)
                          (when (empty? degrees) waves)
                          (recur (conj waves (vec ready))
                                 (reduce (fn [m node]
                                             (reduce (fn [m2 n] (if (m2 n) (update m2 n dec) m2))
                                                     (dissoc m node) (get adj node [])))
                                         degrees ready)))))))

(defn build-downstream-edges [pipelines]
      (let [out->prod (into {} (for [p pipelines, out (:output_sources p)] [out (:name p)]))]
           (distinct
            (concat
             (for [p pipelines, up (:upstream_pipelines p)] [up (:name p)])
             (for [p pipelines, in (:input_sources p)
                   :let [prod (out->prod in)]
                   :when (and prod (not= prod (:name p)))]
                  [prod (:name p)])))))

(defn build-edge-adjacency [edges]
      (reduce (fn [acc [from to]]
                  (-> acc
                      (update-in [:downstream from] (fnil conj #{}) to)
                      (update-in [:upstream to] (fnil conj #{}) from)))
              {:upstream {} :downstream {}} edges))

(defn compute-execution-waves [pipelines selected-names]
      (when (seq selected-names)
            (let [edges (build-downstream-edges pipelines)
                  {:keys [downstream]} (build-edge-adjacency edges)
                  downstream-of (into {} (for [n selected-names]
                                              [n (reachable n #(get downstream % #{}))]))
                  affected (reduce into (set selected-names) (vals downstream-of))
                  relevant (filter (fn [[f t]] (and (affected f) (affected t))) edges)
                  waves (kahns-waves affected relevant)]
                 {:waves (or waves []) :edges (vec relevant) :node-count (count affected)})))

;; Pipeline Adjacency
(defn build-pipeline-adjacency [{:keys [pipelines]}]
      (let [out->prod (into {} (for [p pipelines, out (:output_sources p)] [out (:name p)]))]
           (reduce
            (fn [acc {:keys [name input_sources upstream_pipelines]}]
                (let [all-up (distinct (concat upstream_pipelines (keep out->prod input_sources)))]
                     (reduce (fn [a up]
                                 (-> a
                                     (update-in [:downstream up] (fnil conj #{}) name)
                                     (update-in [:upstream name] (fnil conj #{}) up)))
                             acc all-up)))
            {:upstream {} :downstream {}} pipelines)))

(defn upstream-of [config node]
      (let [{:keys [upstream]} (build-pipeline-adjacency config)]
           (reachable node #(get upstream % #{}))))

(defn downstream-of [config node]
      (let [{:keys [downstream]} (build-pipeline-adjacency config)]
           (reachable node #(get downstream % #{}))))

(defn build-full-adjacency [{:keys [pipelines]}]
      (reduce
       (fn [acc {:keys [name input_sources output_sources upstream_pipelines]}]
           (as-> acc $
                 (reduce #(-> %1 (update-in [:downstream %2] (fnil conj []) name)
                              (update-in [:upstream name] (fnil conj []) %2)) $ (or input_sources []))
                 (reduce #(-> %1 (update-in [:downstream name] (fnil conj []) %2)
                              (update-in [:upstream %2] (fnil conj []) name)) $ (or output_sources []))
                 (reduce #(-> %1 (update-in [:downstream %2] (fnil conj []) name)
                              (update-in [:upstream name] (fnil conj []) %2)) $ (or upstream_pipelines []))))
       {:downstream {} :upstream {}} pipelines))

;; Lineage
(defn lineage-with-depth [adj-map start]
      (mapv #(assoc % :name (:id %)) (bfs start #(get adj-map % #{}))))

(defn full-lineage [config node]
      (let [{:keys [upstream downstream]} (build-pipeline-adjacency config)]
           {:upstream (lineage-with-depth upstream node)
            :downstream (lineage-with-depth downstream node)}))

(defn full-graph-lineage [config node-name]
      (let [{:keys [upstream downstream]} (build-full-adjacency config)]
           {:upstream (mapv #(assoc % :name (:id %)) (bfs node-name #(get upstream % [])))
            :downstream (mapv #(assoc % :name (:id %)) (bfs node-name #(get downstream % [])))}))

(defn node-provenance [config node-name]
      (let [{:keys [pipelines datasources]} config
            find-by-name (fn [coll] (first (filter #(= (:name %) node-name) coll)))
            node-data (or (find-by-name pipelines) (find-by-name datasources))
            node-type (cond (find-by-name pipelines) "pipeline"
                            (find-by-name datasources) "datasource"
                            :else "unknown")
            {:keys [upstream downstream]} (build-full-adjacency config)
            up-chain (bfs node-name #(get upstream % []))
            down-chain (bfs node-name #(get downstream % []))
            all-nodes (into #{node-name} (map :id (concat up-chain down-chain)))]
           {:node (merge {:name node-name :type node-type}
                         (select-keys node-data [:description :cluster :tags :schedule :input_sources :output_sources]))
            :upstream (mapv #(select-keys % [:id :depth]) up-chain)
            :downstream (mapv #(select-keys % [:id :depth]) down-chain)
            :edges (vec (for [n all-nodes, nb (get downstream n []) :when (all-nodes nb)]
                             {:from n :to nb}))}))

;; Pipeline Groups
(defn build-group-data [{:keys [pipelines]}]
      (let [pipe->group (into {} (for [p pipelines :when (:group p)] [(:name p) (:group p)]))
            groups (reduce (fn [acc p]
                               (if-let [g (:group p)]
                                       (update-in acc [g :members] (fnil conj []) (:name p))
                                       acc))
                           {} pipelines)
            groups (reduce-kv
                    (fn [acc gname {:keys [members]}]
                        (let [mset (set members)
                              gp (filter #(mset (:name %)) pipelines)]
                             (assoc acc gname
                                    {:members members
                                     :inputs (set (remove mset (mapcat :input_sources gp)))
                                     :outputs (set (mapcat :output_sources gp))
                                     :upstream-pipelines (set (remove mset (mapcat :upstream_pipelines gp)))
                                     :downstream-pipelines (set (map :name (filter #(some (set (mapcat :output_sources gp))
                                                                                          (:input_sources %))
                                                                                   (remove #(mset (:name %)) pipelines))))})))
                    groups groups)]
           {:groups groups :pipeline->group pipe->group}))

(defn all-groups [{:keys [pipelines]}]
      (->> pipelines (keep :group) distinct vec))

(defn group-provenance [config group-name]
      (let [{:keys [groups]} (build-group-data config)
            {:keys [members inputs outputs]} (get groups group-name)
            mset (set members)
            cluster (:cluster (first (filter #(mset (:name %)) (:pipelines config))))
            {:keys [upstream downstream]} (build-full-adjacency config)
            up (distinct (mapcat #(->> (bfs % (fn [x] (get upstream x [])))
                                       (remove (fn [x] (mset (:id x))))) members))
            down (distinct (mapcat #(->> (bfs % (fn [x] (get downstream x [])))
                                         (remove (fn [x] (mset (:id x))))) members))]
           {:node {:name group-name :type "group" :members members :cluster cluster
                   :inputs (vec inputs) :outputs (vec outputs)}
            :upstream (mapv #(hash-map :name (:id %) :depth (:depth %)) up)
            :downstream (mapv #(hash-map :name (:id %) :depth (:depth %)) down)}))

;; Search
(defn fuzzy-match [text query]
      (let [tl (str/lower-case text), ql (str/lower-case query)]
           (if (str/includes? tl ql)
               {:match true :score (+ 0.5 (/ (count query) (count text)))}
               (loop [ti 0, qi 0, score 0, last-match -1]
                     (cond
                      (>= qi (count ql)) {:match true :score (/ score (count text))}
                      (>= ti (count tl)) {:match false :score 0}
                      (= (nth tl ti) (nth ql qi))
                      (recur (inc ti) (inc qi)
                             (+ score 1 (if (= last-match (dec ti)) 0.5 0)
                                (if (or (zero? ti) (#{\_ \- \space} (nth tl (dec ti)))) 0.3 0))
                             ti)
                      :else (recur (inc ti) qi score last-match))))))

(defn highlight-match [text query]
      (let [tl (str/lower-case text), ql (str/lower-case query), idx (str/index-of tl ql)]
           (if idx
               (str (subs text 0 idx) "<span class=\"result-match\">"
                    (subs text idx (+ idx (count query))) "</span>" (subs text (+ idx (count query))))
               (loop [ti 0, qi 0, result ""]
                     (cond
                      (>= ti (count text)) result
                      (>= qi (count ql)) (str result (subs text ti))
                      (= (nth tl ti) (nth ql qi))
                      (recur (inc ti) (inc qi) (str result "<span class=\"result-match\">" (nth text ti) "</span>"))
                      :else (recur (inc ti) qi (str result (nth text ti))))))))

(defn search-nodes [{:keys [pipelines datasources]} query]
      (when (seq (str/trim (or query "")))
            (let [q (str/trim query)
                  score (fn [item]
                            (let [nm (fuzzy-match (:name item) q)
                                  dm (if (:description item) (fuzzy-match (:description item) q) {:match false :score 0})]
                                 (when (or (:match nm) (:match dm)) (max (* (:score nm) 1.5) (:score dm)))))
                  explicit (set (map :name datasources))
                  auto (->> pipelines (mapcat #(concat (:input_sources %) (:output_sources %)))
                            (remove explicit) distinct)]
                 (->> (concat
                       (for [p pipelines :let [s (score p)] :when s] {:name (:name p) :type :pipeline :score s})
                       (for [d datasources :let [s (score d)] :when s] {:name (:name d) :type :datasource :score s})
                       (for [n auto :let [m (fuzzy-match n q)] :when (:match m)] {:name n :type :datasource :score (* (:score m) 1.5)}))
                      (sort-by :score >) (take 8) vec))))

;; DOT Generation Helpers
(defn- dot-node [type {:keys [name schedule]} _opts]
       (let [n (escape-dot name)
             {:keys [fill border]} (styles/dot-nodes type)]
            (case type
                  :pipeline (if-let [sched (format-schedule schedule)]
                                    (str "\"" n "\" [shape=box style=\"filled,rounded\" fillcolor=\"" fill "\" color=\"" border
                                         "\" label=<" n "<BR/><FONT POINT-SIZE=\"9\" COLOR=\"" (:pink styles/colors) "\"><I>" sched "</I></FONT>>];")
                                    (str "\"" n "\" [shape=box style=\"filled,rounded\" fillcolor=\"" fill "\" color=\"" border "\"];"))
                  :datasource (str "\"" n "\" [shape=ellipse style=filled fillcolor=\"" fill "\" color=\"" border "\"];"))))

(defn- dot-group-node [group-name member-count]
       (let [{:keys [fill border]} (:group styles/dot-nodes)]
            (str "\"group:" (escape-dot group-name) "\" [shape=box3d style=filled fillcolor=\"" fill "\" color=\"" border
                 "\" label=<" (escape-dot group-name) "<BR/><FONT POINT-SIZE=\"9\" COLOR=\"" (:gray styles/colors) "\">"
                 member-count " pipelines</FONT>>];")))

(defn generate-dot
      "Generate Graphviz DOT from config"
      ([config] (generate-dot config {}))
      ([{:keys [pipelines datasources clusters] :as config} opts]
       (let [{:keys [show-datasources? collapsed-groups] :or {show-datasources? true collapsed-groups #{}}} opts
             {:keys [groups pipeline->group]} (build-group-data config)
             collapsed? #(collapsed-groups (:group %))
             visible-pipelines (remove collapsed? pipelines)
             effective-node (fn [pn] (if-let [g (pipeline->group pn)]
                                             (if (collapsed-groups g) (str "group:" g) pn) pn))
             group->cluster (into {} (for [g collapsed-groups
                                           :let [ms (:members (groups g))]
                                           :when (seq ms)]
                                          [g (:cluster (first (filter #(= (:name %) (first ms)) pipelines)))]))
             cluster-defs (reduce #(if (% %2) %1 (assoc %1 %2 {:name %2}))
                                  (into {} (map #(vector (:name %) %) (or clusters [])))
                                  (remove nil? (into #{} (concat (keep :cluster visible-pipelines)
                                                                 (vals group->cluster)
                                                                 (when show-datasources? (keep :cluster datasources))))))
             hierarchy (into {} (for [c clusters :when (:parent c)] [(:name c) (:parent c)]))
             children (reduce (fn [m [c p]] (update m p (fnil conj []) c)) {} hierarchy)
             explicit-ds (set (map :name datasources))
             implicit-ds (->> pipelines (mapcat #(concat (:input_sources %) (:output_sources %)))
                              distinct (remove explicit-ds) (map #(hash-map :name %)))
             nodes-by-cluster (as-> {} $
                                    (reduce #(update %1 (or (:cluster %2) "_none") (fnil conj []) {:type :pipeline :node %2})
                                            $ visible-pipelines)
                                    (reduce (fn [a g] (update a (or (group->cluster g) "_none") (fnil conj [])
                                                              {:type :group :node {:name g :members (:members (groups g))}}))
                                            $ collapsed-groups)
                                    (if show-datasources?
                                        (reduce #(update %1 (or (:cluster %2) "_none") (fnil conj []) {:type :datasource :node %2})
                                                $ (concat datasources implicit-ds)) $))
             dark? (:dark? opts)
             render-node (fn [item indent]
                             (case (:type item)
                                   :group (str indent (dot-group-node (:name (:node item)) (count (:members (:node item)))))
                                   (str indent (dot-node (:type item) (:node item) opts))))
             render-cluster (fn rc [cn depth]
                                (let [ns (get nodes-by-cluster cn [])
                                      kids (get children cn [])
                                      ind (apply str (repeat (inc depth) "  "))]
                                     (when (or (seq ns) (seq kids))
                                           (str ind "subgraph cluster_" (sanitize-id cn) " {\n"
                                                ind "  label=\"" cn "\" style=dotted color=\"#666\" fontcolor=\""
                                                (if dark? "#aaa" "#555") "\" fontsize=11 fontname=Arial\n"
                                                (str/join "\n" (map #(render-node % (str ind "  ")) ns)) "\n"
                                                (str/join "\n" (map #(rc % (inc depth)) kids))
                                                ind "}\n"))))
             roots (filter #(and (not (hierarchy %))
                                 (or (seq (nodes-by-cluster %)) (some (comp seq nodes-by-cluster) (children %))))
                           (keys cluster-defs))
             edge-clr (styles/edge-color dark?)
         ;; Collect edges as data, dedupe by [src tgt], then format
             raw-edges (concat
                        (when show-datasources?
                              (concat
                               (for [p pipelines :let [tgt (effective-node (:name p))]
                                     src (:input_sources p)]
                                    {:src src :tgt tgt :color edge-clr})
                               (for [p pipelines :let [src (effective-node (:name p))]
                                     out (:output_sources p)]
                                    {:src src :tgt out :color edge-clr})))
                        (let [valid (set (map :name pipelines))]
                             (for [p pipelines
                                   up (:upstream_pipelines p)
                                   :when (valid up)
                                   :let [s (effective-node up)
                                         t (effective-node (:name p))]
                                   :when (not= s t)]
                                  {:src s :tgt t :color styles/dependency-edge-color :style "solid"}))
                        (when (seq collapsed-groups)
                              (for [g collapsed-groups
                                    :let [{:keys [outputs]} (groups g)]
                                    out outputs
                                    p pipelines
                                    :when (and (some #{out} (:input_sources p))
                                               (not (collapsed-groups (:group p))))]
                                   {:src (str "group:" g) :tgt (:name p) :color edge-clr})))
         ;; Dedupe by [src tgt] keeping first occurrence
             deduped-edges (->> raw-edges
                                (reduce (fn [{:keys [seen edges]} e]
                                            (let [k [(:src e) (:tgt e)]]
                                                 (if (seen k)
                                                     {:seen seen :edges edges}
                                                     {:seen (conj seen k) :edges (conj edges e)})))
                                        {:seen #{} :edges []})
                                :edges)
             make-edges (fn []
                            (map (fn [{:keys [src tgt color style]}]
                                     (str "\"" (escape-dot src) "\" -> \"" (escape-dot tgt)
                                          "\" [color=\"" color "\""
                                          (when style (str " style=" style))
                                          " arrowsize=0.8];"))
                                 deduped-edges))]
            (str "digraph G {\n  rankdir=LR bgcolor=transparent overlap=false splines=true\n  node [fontname=Arial fontsize=12]\n  edge [fontsize=10]\n\n"
                 (str/join "\n" (map #(render-cluster % 0) roots)) "\n"
                 (str/join "\n" (map #(render-node % "  ") (nodes-by-cluster "_none"))) "\n"
                 (str/join "\n" (map #(str "  " %) (make-edges))) "\n}"))))

;; Attribute Lineage
(defn- parse-source-ref [ref]
       (let [[ds & path] (str/split ref #"::")]
            {:ds (sanitize-id ds) :path (str/join "__" (map sanitize-id path))
             :id (str (sanitize-id ds) "__" (str/join "__" (map sanitize-id path)))}))

(defn- collect-source-refs [attrs]
       (reduce (fn [refs attr]
                   (into refs (concat (cond (vector? (:from attr)) (:from attr) (:from attr) [(:from attr)] :else [])
                                      (when (:attributes attr) (collect-source-refs (:attributes attr))))))
               #{} attrs))

(defn- flatten-attributes [attrs ds-name ds-id prefix]
       (mapcat (fn [attr]
                   (let [path (if (seq prefix) (str prefix "__" (:name attr)) (:name attr))
                         id (str ds-id "__" (sanitize-id path))
                         froms (cond (vector? (:from attr)) (:from attr) (:from attr) [(:from attr)] :else [])]
                        (cons {:id id :name (:name attr) :ds ds-name
                               :full-name (str ds-name "::" (str/replace path "__" "::"))
                               :from (mapv parse-source-ref froms)}
                              (when (:attributes attr) (flatten-attributes (:attributes attr) ds-name ds-id path)))))
               attrs))

(defn build-attribute-lineage-map [config]
      (let [datasources (:datasources config)
            all-attrs (mapcat #(when (:attributes %)
                                     (flatten-attributes (:attributes %) (:name %) (sanitize-id (:name %)) ""))
                              datasources)
            attr-map (reduce (fn [m {:keys [id from]}]
                                 (let [attr-id id]
                                      (reduce (fn [m' src]
                                                  (let [src-id (:id src)]
                                                       (-> m' (update-in [attr-id :upstream] conj src-id)
                                                           (update-in [src-id :downstream] conj attr-id))))
                                              m from)))
                             (into {} (map #(vector (:id %) (assoc % :upstream [] :downstream [])) all-attrs))
                             all-attrs)
            attr-map (reduce-kv (fn [m id attr]
                                    (assoc m id (assoc attr
                                                       :full-upstream (bfs id #(get-in attr-map [% :upstream] []))
                                                       :full-downstream (bfs id #(get-in attr-map [% :downstream] [])))))
                                {} attr-map)
            ds-ids (set (map #(sanitize-id (:name %)) datasources))
            ds-lineage (fn [dir]
                           (reduce-kv (fn [m id {:keys [upstream downstream]}]
                                          (let [ds (first (str/split id #"__"))
                                                deps (->> (if (= dir :up) upstream downstream)
                                                          (map #(first (str/split % #"__")))
                                                          (filter #(and (not= % ds) (ds-ids %))) set)]
                                               (update m ds (fnil into #{}) deps)))
                                      {} attr-map))]
           {:attribute-map attr-map
            :datasource-map (into {} (for [ds datasources :let [id (sanitize-id (:name ds))]]
                                          [id {:name (:name ds) :ds-id id
                                               :upstream (bfs id #(get (ds-lineage :up) % #{}))
                                               :downstream (bfs id #(get (ds-lineage :down) % #{}))}]))}))

(defn attribute-provenance
      ([lineage-map attr-id] (attribute-provenance lineage-map attr-id {}))
      ([lineage-map attr-id {:keys [max-depth]}]
       (when-let [attr (lineage-map attr-id)]
                 (let [filt (if max-depth #(filter (fn [x] (<= (:depth x) max-depth)) %) identity)]
                      {:attribute attr-id :name (:name attr) :full-name (:full-name attr) :datasource (:ds attr)
                       :upstream {:attributes (mapv #(merge % (select-keys (lineage-map (:id %)) [:name :full-name])) (filt (:full-upstream attr)))}
                       :downstream {:attributes (mapv #(merge % (select-keys (lineage-map (:id %)) [:name :full-name])) (filt (:full-downstream attr)))}}))))

(defn- collect-struct-ids
       "Collect IDs of all struct (nested) attributes"
       [attrs ds-id prefix]
       (reduce (fn [ids a]
                   (let [path (if (seq prefix) (str prefix "__" (:name a)) (:name a))
                         id (str ds-id "__" (sanitize-id path))]
                        (if (:attributes a)
                            (into (conj ids id) (collect-struct-ids (:attributes a) ds-id path))
                            ids)))
               #{} attrs))

(defn generate-attribute-dot [config]
      (let [datasources (:datasources config)
            all-refs (reduce #(if (:attributes %2) (into %1 (collect-source-refs (:attributes %2))) %1) #{} datasources)
            has-lin? (fn [ds path] (contains? all-refs (str ds "::" (str/replace path "__" "::"))))
        ;; Pre-collect all struct IDs
            structs (reduce (fn [ids ds]
                                (if (:attributes ds)
                                    (into ids (collect-struct-ids (:attributes ds) (sanitize-id (:name ds)) ""))
                                    ids))
                            #{} datasources)
            render-attrs (fn ra [attrs ds-name ds-id prefix]
                             (str/join "" (for [a attrs
                                                :let [path (if (seq prefix) (str prefix "__" (:name a)) (:name a))
                                                      id (str ds-id "__" (sanitize-id path))
                                                      lin? (or (:from a) (has-lin? ds-name path))
                                                      st (styles/attr-struct-style lin?)]]
                                               (if (:attributes a)
                                                   (str "    subgraph cluster_" id " {\n      label=\"" (:name a)
                                                        "\" labelloc=t style=filled fillcolor=\"" (:fill st)
                                                        "\" fontcolor=\"" (:text st) "\" color=\"" (:border st)
                                                        "\" fontname=Arial fontsize=9 margin=8\n      \"" id
                                                        "\" [label=\"\" shape=point width=0 height=0 style=invis];\n"
                                                        (ra (:attributes a) ds-name ds-id path) "    }\n")
                                                   (str "      \"" id "\" [label=\"" (:name a) "\" shape=box style=\"filled,rounded\" fillcolor=\""
                                                        (styles/attr-node-fill lin?) "\" color=\"" (:gray-border styles/colors)
                                                        "\" fontcolor=\"#334155\" fontsize=9 height=0.3];\n")))))
            collect-edges (fn ce [attrs ds-id prefix]
                              (mapcat (fn [a]
                                          (let [path (if (seq prefix) (str prefix "__" (:name a)) (:name a))
                                                tid (str ds-id "__" (sanitize-id path))
                                                froms (cond (vector? (:from a)) (:from a) (:from a) [(:from a)] :else [])]
                                               (concat
                                                (for [src froms
                                                      :let [{:keys [id]} (parse-source-ref src)
                                                            as (cond-> [(str "color=\"" (:edge styles/attr-graph) "\"")]
                                                                 (structs id) (conj (str "ltail=\"cluster_" id "\""))
                                                                 (structs tid) (conj (str "lhead=\"cluster_" tid "\"")))]]
                                                     (str "  \"" id "\" -> \"" tid "\" [" (str/join " " as) "];"))
                                                (when (:attributes a) (ce (:attributes a) ds-id path)))))
                                      attrs))]
           (str "digraph AttributeLineage {\n  rankdir=LR bgcolor=transparent compound=true nodesep=0.15\n"
                "  node [fontname=Arial fontsize=10]\n  edge [fontsize=9 color=\"#94a3b8\" arrowsize=0.6]\n\n"
                (str/join "" (for [ds datasources :when (:attributes ds) :let [id (sanitize-id (:name ds))]]
                                  (str "  subgraph cluster_" id " {\n    label=\"" (:name ds)
                                       "\" style=filled fillcolor=\"#f1f5f9\" fontcolor=\"#334155\" color=\"#94a3b8\" fontname=Arial fontsize=11\n"
                                       (render-attrs (:attributes ds) (:name ds) id "") "  }\n")))
                "\n" (str/join "\n" (mapcat #(when (:attributes %) (collect-edges (:attributes %) (sanitize-id (:name %)) "")) datasources))
                "\n}")))

;; Blast Radius
(defn- add-downstream-edge [downstream from to]
       (update downstream from (fnil conj #{}) to))

(defn- add-node-type [node-types node t]
       (update node-types node #(or % t)))

(defn build-blast-radius-graph [config]
      (when config
            (let [pipelines (or (:pipelines config) [])
                  init {:downstream {} :node-types {}}
                  with-pipelines
                  (reduce
                   (fn [acc p]
                       (let [pname (:name p)]
                            (as-> acc $
                                  (update $ :node-types assoc pname :pipeline)
                                  (update $ :downstream #(update % pname (fnil identity #{})))
                 ;; output sources
                                  (reduce (fn [a s]
                                              (-> a
                                                  (update :downstream add-downstream-edge pname s)
                                                  (update :node-types add-node-type s :datasource)))
                                          $ (:output_sources p))
                 ;; input sources
                                  (reduce (fn [a s]
                                              (-> a
                                                  (update :downstream #(update % s (fnil identity #{})))
                                                  (update :downstream add-downstream-edge s pname)
                                                  (update :node-types add-node-type s :datasource)))
                                          $ (:input_sources p))
                 ;; upstream pipelines
                                  (reduce (fn [a u]
                                              (-> a
                                                  (update :downstream #(update % u (fnil identity #{})))
                                                  (update :downstream add-downstream-edge u pname)))
                                          $ (:upstream_pipelines p)))))
                   init pipelines)
                  with-datasources
                  (reduce (fn [acc ds]
                              (update acc :node-types add-node-type (:name ds) :datasource))
                          with-pipelines (:datasources config))]
                 (assoc with-datasources
                        :pipelines pipelines
                        :groups (reduce-kv (fn [m k v] (assoc m k (mapv :name v))) {}
                                           (group-by :group (filter :group pipelines)))))))

(defn blast-radius-for-node [{:keys [downstream node-types pipelines groups]} node-name]
      (when node-name
            (let [members (get groups node-name)
                  is-group? (seq members)
                  starts (if is-group? members [node-name])
                  mset (set starts)
                  init-visited (into {} (map #(vector % 0) starts))
                  init-queue (vec (map #(vector % 0) starts))
                  {:keys [visited edges]}
                  (loop [visited init-visited
                         edges []
                         queue init-queue]
                        (if (empty? queue)
                            {:visited visited :edges edges}
                            (let [[curr depth] (first queue)
                                  neighbors (get downstream curr #{})
                                  {:keys [visited' edges' queue']}
                                  (reduce
                                   (fn [{:keys [visited' edges' queue']} nb]
                                       (if (and is-group? (mset curr) (mset nb))
                                           {:visited' visited' :edges' edges' :queue' queue'}
                                           (let [edge {:source (if is-group? node-name curr) :target nb}]
                                                (if (visited' nb)
                                                    {:visited' visited' :edges' (conj edges' edge) :queue' queue'}
                                                    {:visited' (assoc visited' nb (inc depth))
                                                     :edges' (conj edges' edge)
                                                     :queue' (conj queue' [nb (inc depth)])}))))
                                   {:visited' visited :edges' edges :queue' (subvec queue 1)}
                                   neighbors)]
                                 (recur visited' edges' queue'))))
                  nodes (->> visited
                             (remove (fn [[n _]] (if is-group? (mset n) (= n node-name))))
                             (map (fn [[n d]]
                                      (let [p (first (filter #(= (:name %) n) pipelines))]
                                           (cond-> {:name n :type (node-types n :unknown) :depth d}
                                             (:schedule p) (assoc :schedule (:schedule p))
                                             (:cluster p) (assoc :cluster (:cluster p))))))
                             (sort-by (juxt :depth :name))
                             vec)]
                 (cond-> {:source node-name
                          :source-type (if is-group? :group (node-types node-name :unknown))
                          :total-affected (count nodes)
                          :max-depth (if (seq nodes) (apply max (map :depth nodes)) 0)
                          :downstream nodes
                          :by-depth (into (sorted-map) (group-by :depth nodes))
                          :edges (vec (distinct edges))}
                   is-group? (assoc :group-members members :group-size (count members))))))

(defn precompute-all-blast-radius [config]
      (when config
            (let [g (build-blast-radius-graph config)]
                 {:graph g :results (into {} (for [n (concat (keys (:node-types g)) (keys (:groups g)))]
                                                  [n (blast-radius-for-node g n)]))})))

(defn generate-blast-radius-analysis
      ([config node-name] (generate-blast-radius-analysis config node-name nil))
      ([config node-name blast-data]
       (or (get-in blast-data [:results node-name])
           (some-> (or (:graph blast-data) (build-blast-radius-graph config))
             (blast-radius-for-node node-name)))))

(defn generate-blast-radius-dot [analysis dark?]
      (when (and analysis (pos? (:total-affected analysis)))
            (let [colors (if dark? (:dark styles/blast-radius-colors) (:light styles/blast-radius-colors))
                  src-color (first colors)
                  src-shape (if (= :datasource (:source-type analysis)) "ellipse" "box")
                  src-label (if (= :group (:source-type analysis))
                                (str (:source analysis) "\\n(" (:group-size analysis) " pipelines)")
                                (:source analysis))]
                 (str "digraph BlastRadius {\n    rankdir=LR\n    bgcolor=\"" (if dark? "#1a1a1a" "#fff") "\"\n"
                      "    fontname=\"Helvetica\"\n    node [fontname=\"Helvetica\" fontsize=\"9\" style=\"filled\"]\n"
                      "    edge [color=\"" (if dark? "#666" "#999") "\" arrowsize=\"0.6\"]\n\n"
                      "    \"" (:source analysis) "\" [label=\"" src-label "\" shape=\"" src-shape
                      "\" fillcolor=\"" (:fill src-color) "\" color=\""
                      (if (= :group (:source-type analysis)) "#00897b" (:border src-color))
                      "\" fontcolor=\"" (:text src-color) "\" penwidth=\"2\"]\n\n"
                      (str/join "\n" (for [[d ns] (:by-depth analysis)
                                           :let [c (nth colors (min d (dec (count colors))))]]
                                          (str "    subgraph cluster_depth" d " {\n        label=\"Depth " d
                                               "\" fontname=\"Helvetica\" style=\"dashed\" color=\"" (:border c)
                                               "\" fontcolor=\"" (if dark? "#b0b0b0" "#666") "\" fontsize=\"9\"\n\n"
                                               (str/join "\n" (for [n ns] (str "        \"" (:name n) "\" [label=\"" (:name n)
                                                                               "\" shape=\"" (if (= :datasource (:type n)) "ellipse" "box")
                                                                               "\" fillcolor=\"" (:fill c) "\" color=\"" (:border c)
                                                                               "\" fontcolor=\"" (:text c) "\"]")))
                                               "\n    }")))
                      "\n\n" (str/join "\n" (distinct (map #(str "    \"" (:source %) "\" -> \"" (:target %) "\"") (:edges analysis))))
                      "\n}\n"))))

;; Backfill Analysis
(defn generate-backfill-analysis [config selected]
      (when (and config (seq selected))
            (let [pipelines (:pipelines config)
                  pmap (into {} (map (juxt :name identity) pipelines))
                  invalid (remove (set (map :name pipelines)) selected)]
                 (when (seq invalid) (throw (ex-info "Backfill only works for pipelines" {:invalid invalid})))
                 (let [{:keys [waves edges node-count]} (compute-execution-waves pipelines selected)]
                      {:nodes (vec selected)
                       :total-downstream-pipelines (max 0 (- node-count (count selected)))
                       :total-waves (count waves)
                       :max-parallelism (if (seq waves) (apply max (map count waves)) 0)
                       :waves (vec (map-indexed (fn [i ns]
                                                    {:wave i :parallel-count (count ns)
                                                     :pipelines (mapv (fn [n] (let [p (pmap n)]
                                                                                   (cond-> {:name n}
                                                                                     (:schedule p) (assoc :schedule (format-schedule (:schedule p)))
                                                                                     (:cluster p) (assoc :cluster (:cluster p)))))
                                                                      ns)})
                                                waves))
                       :edges (mapv (fn [[f t]] {:source f :target t}) edges)}))))

(defn generate-backfill-dot [analysis dark?]
      (when (and analysis (pos? (:total-waves analysis)))
            (let [colors (if dark? (:dark styles/wave-colors) (:light styles/wave-colors))
                  w0 (styles/get-wave-color 0 dark?)]
                 (str "digraph Backfill {\n    rankdir=LR\n    bgcolor=\"" (if dark? "#1a1a1a" "#fff") "\"\n"
                      "    fontname=\"Helvetica\"\n    node [fontname=\"Helvetica\" fontsize=\"9\" style=\"filled\" shape=\"box\"]\n"
                      "    edge [color=\"" (if dark? "#666" "#999") "\" arrowsize=\"0.6\"]\n\n"
                      (str/join "\n" (for [{:keys [wave pipelines]} (:waves analysis)
                                           :let [c (if (zero? wave) w0 (nth colors (mod (dec wave) (count colors))))]]
                                          (str "    subgraph cluster_wave" wave " {\n        label=\"Wave " wave
                                               "\" style=\"dashed\" color=\"" (:border c) "\" fontcolor=\""
                                               (if dark? "#b0b0b0" "#666") "\" fontsize=\"9\"\n\n"
                                               (str/join "\n" (for [p pipelines]
                                                                   (str "        \"" (:name p) "\" [label=\"" (:name p)
                                                                        "\" fillcolor=\"" (:fill c) "\" color=\"" (:border c)
                                                                        "\" fontcolor=\"" (:text c) "\"]")))
                                               "\n    }")))
                      "\n\n" (str/join "\n" (distinct (map #(str "    \"" (:source %) "\" -> \"" (:target %) "\"") (:edges analysis))))
                      "\n}\n"))))

(defn generate-airflow-analysis [config backfill]
      (when backfill
            (let [pmap (into {} (map (juxt :name identity) (:pipelines config)))
                  extract-dag (fn [p] (some->> (get-in p [:links :airflow]) (re-find #"/dags?/([^/?\s]+)") second))
                  p->dag (into {} (for [[n p] pmap :let [d (extract-dag p)] :when d] [n d]))]
                 {:view :airflow
                  :total-dags (count (distinct (map #(get p->dag % %) (:nodes backfill))))
                  :total-waves (count (:waves backfill))
                  :waves (vec (for [{:keys [wave pipelines]} (:waves backfill)]
                                   (let [gs (group-by #(get p->dag (:name %) (:name %)) pipelines)]
                                        {:wave wave :parallel-count (count gs)
                                         :dags (vec (for [[dn ps] gs :let [url (get-in (pmap (:name (first ps))) [:links :airflow])]]
                                                         {:dag dn :airflow-url url :pipelines (mapv :name ps) :missing (nil? url)}))})))
                  :edges (vec (distinct (for [{:keys [source target]} (:edges backfill)
                                              :let [s (get p->dag source source) t (get p->dag target target)]
                                              :when (not= s t)]
                                             {:source-dag s :target-dag t})))})))

(defn generate-airflow-dot [analysis dark?]
      (when (and analysis (pos? (:total-waves analysis)))
            (let [colors (if dark? (:dark styles/wave-colors) (:light styles/wave-colors))
                  w0 (styles/get-wave-color 0 dark?)]
                 (str "digraph Airflow {\n    rankdir=LR\n    bgcolor=\"" (if dark? "#1a1a1a" "#fff") "\"\n"
                      "    fontname=\"Helvetica\"\n    node [fontname=\"Helvetica\" fontsize=\"9\" style=\"filled\" shape=\"box\"]\n"
                      "    edge [color=\"" (if dark? "#666" "#999") "\" arrowsize=\"0.6\"]\n\n"
                      (str/join "\n" (for [{:keys [wave dags]} (:waves analysis)
                                           :let [c (if (zero? wave) w0 (nth colors (mod (dec wave) (count colors))))]]
                                          (str "    subgraph cluster_wave" wave " {\n        label=\"Wave " wave
                                               "\" style=\"dashed\" color=\"" (:border c) "\" fontcolor=\""
                                               (if dark? "#b0b0b0" "#666") "\" fontsize=\"9\"\n\n"
                                               (str/join "\n" (for [d dags :let [lbl (if (> (count (:pipelines d)) 1)
                                                                                         (str (:dag d) "\\n(" (count (:pipelines d)) " pipelines)")
                                                                                         (:dag d))
                                                                                 missing? (:missing d)]]
                                                                   (str "        \"" (:dag d) "\" [label=\"" lbl "\" fillcolor=\""
                                                                        (if missing? (if dark? "#2a2a2a" "#e8e8e8") (:fill c))
                                                                        "\" color=\"" (if missing? "#ccc" (:border c))
                                                                        "\" fontcolor=\"" (if missing? "#999" (:text c)) "\""
                                                                        (when missing? " style=\"filled,dashed\"")
                                                                        (when (and (:airflow-url d) (not missing?))
                                                                              (str " href=\"" (:airflow-url d) "\" target=\"_blank\" tooltip=\"Open in Airflow\""))
                                                                        "]")))
                                               "\n    }")))
                      "\n\n" (str/join "\n" (distinct (map #(str "    \"" (:source-dag %) "\" -> \"" (:target-dag %) "\"") (:edges analysis))))
                      "\n}\n"))))

;; Cron Parsing
(def ^:private days ["Sunday" "Monday" "Tuesday" "Wednesday" "Thursday" "Friday" "Saturday"])
(def ^:private days-short ["Sun" "Mon" "Tue" "Wed" "Thu" "Fri" "Sat"])
(def ^:private months ["January" "February" "March" "April" "May" "June" "July" "August" "September" "October" "November" "December"])

(defn cron? [s]
      (when (string? s)
            (let [parts (str/split (str/trim s) #"\s+")]
                 (and (= 5 (count parts)) (every? #(re-matches #"^[\d,\-\*\/A-Za-z]+$" %) parts)))))

(defn- parse-int [s] #?(:clj (Integer/parseInt s) :cljs (js/parseInt s 10)))

(defn- find-name-idx [v names]
       (or (->> names (keep-indexed (fn [i n] (when (str/starts-with? (str/upper-case n) (str/upper-case v)) i))) first)
           (parse-int v)))

(defn parse-field [field _ & [names]]
      (cond
       (= "*" field) nil
       (str/starts-with? field "*/") {:every (parse-int (subs field 2))}
       (and (str/includes? field "-") (not (str/includes? field ",")))
       (let [[s e] (str/split field #"-")]
            {:range [(if names (find-name-idx s names) (parse-int s))
                     (if names (find-name-idx e names) (parse-int e))]})
       (str/includes? field ",")
       {:list (mapv #(if names (find-name-idx % names) (parse-int %)) (str/split field #","))}
       :else {:value (if names (find-name-idx field names) (parse-int field))}))

(defn- fmt-time [h m]
       (let [h12 (let [x (mod h 12)] (if (zero? x) 12 x))
             ms (if (< m 10) (str "0" m) (str m))]
            (str h12 (when (not= "00" ms) (str ":" ms)) " " (if (< h 12) "AM" "PM"))))

(defn- ordinal [n]
       (str n (["th" "st" "nd" "rd"] (if (<= 11 (mod n 100) 13) 0 (min (mod n 10) 4)))))

(defn format-schedule [sched]
      (if-not (cron? sched) sched
              (let [[min hr dom mon dow] (str/split (str/trim sched) #"\s+")
                    mp (parse-field min 59), hp (parse-field hr 23)
                    dp (parse-field dom 31), mop (parse-field mon 12 months), dwp (parse-field dow 6 days-short)]
                   (cond
                    (and (:every mp) (not hp) (not dp) (not mop) (not dwp)) (str "Every " (:every mp) " minutes")
                    (and (:every hp) (#{"0" "*"} min) (not dp) (not mop) (not dwp)) (str "Every " (:every hp) " hours")
                    :else
                    (let [ts (cond (and (:value hp) (:value mp)) (fmt-time (:value hp) (:value mp))
                                   (:value hp) (fmt-time (:value hp) 0)
                                   (and (:value mp) (not= "0" min)) (str ":" (if (< (:value mp) 10) (str "0" (:value mp)) (:value mp))))]
                         (cond
                          (and dwp (not dp) (not mop))
                          (let [base (cond (:range dwp) (let [[s e] (:range dwp)]
                                                             (cond (and (= 1 s) (= 5 e)) "Weekdays" (and (= 0 s) (= 6 e)) "Daily"
                                                                   :else (str (days s) "-" (days e))))
                                           (:list dwp) (str/join ", " (map days-short (:list dwp)))
                                           (:value dwp) (str (days (:value dwp)) "s"))]
                               (if ts (str base " at " ts) base))
                          (and dp (not dwp))
                          (let [base (when (:value dp) (str (ordinal (:value dp)) " of "
                                                            (if (:value mop) (months (dec (:value mop))) "each month")))]
                               (if ts (str base " at " ts) base))
                          (and (not dp) (not dwp) (not mop) ts) (str "Daily at " ts)
                          :else sched))))))

;; Splash Graph
(defn generate-splash-dot [_]
      (let [{df :fill db :border} (:datasource styles/dot-nodes)
            {pf :fill pb :border} (:pipeline styles/dot-nodes)]
           (str "digraph {\n    rankdir=LR\n    bgcolor=\"transparent\"\n"
                "    node [fontname=\"Arial\" fontsize=\"10\" fontcolor=\"#333\"]\n"
                "    edge [color=\"" (:gray-muted styles/colors) "\"]\n"
                "    \"raw_events\" [shape=ellipse style=filled fillcolor=\"" df "\" color=\"" db "\"]\n"
                "    \"etl-job\" [shape=box style=\"filled,rounded\" fillcolor=\"" pf "\" color=\"" pb "\"]\n"
                "    \"cleaned_events\" [shape=ellipse style=filled fillcolor=\"" df "\" color=\"" db "\"]\n"
                "    \"raw_events\" -> \"etl-job\"\n    \"etl-job\" -> \"cleaned_events\"\n}")))

;; Export Functions
(defn generate-graph-export
      "Generate JSON-exportable graph structure with nodes and edges"
      [config]
      (when config
            (let [pipelines (or (:pipelines config) [])
                  datasources (or (:datasources config) [])
                  clusters (or (:clusters config) [])
                  node-ids (atom #{})
                  nodes (atom [])
                  edges (atom [])
                  ;; Add pipelines
                  _ (doseq [p pipelines]
                          (swap! nodes conj
                                 (cond-> {:id (:name p) :type "pipeline"}
                                   (:description p) (assoc :description (:description p))
                                   (:schedule p) (assoc :schedule (:schedule p))
                                   (:cluster p) (assoc :cluster (:cluster p))
                                   (:group p) (assoc :group (:group p))
                                   (:owner p) (assoc :owner (:owner p))
                                   (seq (:tags p)) (assoc :tags (:tags p))
                                   (seq (:links p)) (assoc :links (:links p))
                                   (seq (:metadata p)) (assoc :metadata (:metadata p))))
                          (swap! node-ids conj (:name p))
                          ;; Input edges
                          (doseq [src (:input_sources p)]
                                 (swap! edges conj {:source src :target (:name p) :type "data_flow"}))
                          ;; Output edges
                          (doseq [out (:output_sources p)]
                                 (swap! edges conj {:source (:name p) :target out :type "data_flow"}))
                          ;; Upstream pipeline edges
                          (doseq [up (:upstream_pipelines p)]
                                 (swap! edges conj {:source up :target (:name p) :type "pipeline_dependency"})))
                  ;; Add explicit datasources
                  _ (doseq [ds datasources]
                          (swap! nodes conj
                                 (cond-> {:id (:name ds) :type "datasource"}
                                   (:description ds) (assoc :description (:description ds))
                                   (:type ds) (assoc :source_type (:type ds))
                                   (:cluster ds) (assoc :cluster (:cluster ds))
                                   (:owner ds) (assoc :owner (:owner ds))
                                   (seq (:tags ds)) (assoc :tags (:tags ds))
                                   (seq (:links ds)) (assoc :links (:links ds))
                                   (seq (:metadata ds)) (assoc :metadata (:metadata ds))))
                          (swap! node-ids conj (:name ds)))
                  ;; Add implicit datasources (referenced but not defined)
                  referenced (->> pipelines
                                  (mapcat #(concat (:input_sources %) (:output_sources %)))
                                  set)
                  _ (doseq [src referenced]
                          (when-not (@node-ids src)
                                    (swap! nodes conj {:id src :type "datasource" :implicit true})
                                    (swap! node-ids conj src)))]
                 {:nodes @nodes
                  :edges @edges
                  :clusters (mapv (fn [c] {:id (:name c) :description (:description c) :parent (:parent c)}) clusters)
                  :meta {:node_count (count @nodes)
                         :edge_count (count @edges)
                         :pipeline_count (count pipelines)
                         :datasource_count (count (filter #(= "datasource" (:type %)) @nodes))}})))

(defn generate-mermaid-export
      "Generate Mermaid flowchart format"
      [config]
      (when config
            (let [pipelines (or (:pipelines config) [])
                  datasources (or (:datasources config) [])
                  node-ids (atom #{})
                  pipeline-ids (set (map :name pipelines))
                  sanitize #(str/replace (or % "") #"[^a-zA-Z0-9]" "_")
                  ;; Collect all node IDs
                  _ (doseq [p pipelines]
                          (swap! node-ids conj (:name p))
                          (doseq [s (:input_sources p)] (swap! node-ids conj s))
                          (doseq [s (:output_sources p)] (swap! node-ids conj s)))
                  _ (doseq [ds datasources] (swap! node-ids conj (:name ds)))
                  ;; Build Mermaid output
                  lines (atom ["flowchart LR" "    %% Nodes"])]
                 ;; Add node definitions
                 (doseq [id @node-ids]
                        (let [sid (sanitize id)]
                             (if (pipeline-ids id)
                                 (swap! lines conj (str "    " sid "[[\"" id "\"]]"))  ;; Stadium shape for pipelines
                                 (swap! lines conj (str "    " sid "[(\"" id "\")]"))))) ;; Cylinder for datasources
                 ;; Add edges section
                 (swap! lines conj "" "    %% Edges")
                 (let [edge-set (atom #{})]
                      (doseq [p pipelines]
                             (let [pid (sanitize (:name p))]
                                  ;; Input edges
                                  (doseq [src (:input_sources p)]
                                         (let [edge (str "    " (sanitize src) " --> " pid)]
                                              (when-not (@edge-set edge)
                                                        (swap! edge-set conj edge)
                                                        (swap! lines conj edge))))
                                  ;; Output edges
                                  (doseq [out (:output_sources p)]
                                         (let [edge (str "    " pid " --> " (sanitize out))]
                                              (when-not (@edge-set edge)
                                                        (swap! edge-set conj edge)
                                                        (swap! lines conj edge))))
                                  ;; Upstream pipeline edges (dashed)
                                  (doseq [up (:upstream_pipelines p)]
                                         (let [edge (str "    " (sanitize up) " -.-> " pid)]
                                              (when-not (@edge-set edge)
                                                        (swap! edge-set conj edge)
                                                        (swap! lines conj edge)))))))
                 ;; Add styling
                 (swap! lines conj "" "    %% Styling")
                 (swap! lines conj "    classDef pipeline fill:#fff3e0,stroke:#e65100,stroke-width:2px")
                 (swap! lines conj "    classDef datasource fill:#e3f2fd,stroke:#1565c0,stroke-width:2px")
                 (let [pipeline-list (str/join "," (map sanitize pipeline-ids))
                       datasource-list (str/join "," (map sanitize (remove pipeline-ids @node-ids)))]
                      (when (seq pipeline-list)
                            (swap! lines conj (str "    class " pipeline-list " pipeline")))
                      (when (seq datasource-list)
                            (swap! lines conj (str "    class " datasource-list " datasource"))))
                 (str/join "\n" @lines))))

;; Stats Functions
(defn detect-cycles
      "Detect cycles in the pipeline dependency graph"
      [pipelines]
      (let [graph (atom {})
            pipeline-names (atom #{})
            ;; Build graph with groups collapsed
            _ (doseq [p pipelines]
                     (let [node-name (or (:group p) (:name p))]
                          (swap! pipeline-names conj node-name)
                          (when-not (get @graph node-name)
                                    (swap! graph assoc node-name #{}))))
            _ (doseq [p pipelines]
                     (let [node-name (or (:group p) (:name p))]
                          (doseq [up (:upstream_pipelines p)]
                                 (when (@pipeline-names up)
                                       (when-not (get @graph up)
                                                 (swap! graph assoc up #{}))
                                       (swap! graph update up conj node-name)))))
            visited (atom #{})
            rec-stack (atom #{})
            cycles (atom [])]
           ;; DFS to find cycles
           (letfn [(dfs [node path]
                        (swap! visited conj node)
                        (swap! rec-stack conj node)
                        (let [path (conj path node)
                              result (atom nil)]
                             (doseq [neighbor (get @graph node #{})
                                     :when (nil? @result)]
                                    (cond
                                     (not (@visited neighbor))
                                     (when-let [r (dfs neighbor path)]
                                               (reset! result r))
                                     (@rec-stack neighbor)
                                     (let [cycle-start (.indexOf path neighbor)
                                           cycle (conj (vec (drop cycle-start path)) neighbor)]
                                          (reset! result cycle))))
                             (swap! rec-stack disj node)
                             @result))]
                  (doseq [node @pipeline-names
                          :when (not (@visited node))]
                         (when-let [cycle (dfs node [])]
                                   (swap! cycles conj cycle)
                                   (reset! visited #{})
                                   (reset! rec-stack #{})
                                   (doseq [n cycle] (swap! visited conj n)))))
           @cycles))

(defn compute-stats
      "Compute comprehensive statistics about the config"
      [config]
      (when config
            (let [pipelines (or (:pipelines config) [])
                  datasources (or (:datasources config) [])
                  cycles (detect-cycles pipelines)
                  ;; Hub detection - count upstream/downstream connections
                  upstream-counts (atom {})
                  downstream-counts (atom {})
                  groups (atom #{})
                  _ (doseq [p pipelines]
                          (when (:group p) (swap! groups conj (:group p))))
                  _ (doseq [p pipelines]
                          (let [node-name (or (:group p) (:name p))]
                               ;; Input sources contribute to node's upstream count
                               (doseq [s (:input_sources p)]
                                      (swap! downstream-counts update s (fnil inc 0))
                                      (swap! upstream-counts update node-name (fnil inc 0)))
                               ;; Output sources
                               (doseq [s (:output_sources p)]
                                      (swap! downstream-counts update node-name (fnil inc 0))
                                      (swap! upstream-counts update s (fnil inc 0)))
                               ;; Upstream pipelines
                               (doseq [u (:upstream_pipelines p)]
                                      (swap! downstream-counts update u (fnil inc 0))
                                      (swap! upstream-counts update node-name (fnil inc 0)))))
                  pipeline-names (set (map :name pipelines))
                  all-nodes (distinct (concat (keys @upstream-counts) (keys @downstream-counts)))
                  hubs (->> all-nodes
                            (map (fn [name]
                                     (let [type (cond
                                                 (@groups name) "group"
                                                 (pipeline-names name) "pipeline"
                                                 :else "datasource")]
                                          {:name name
                                           :type type
                                           :upstream (get @upstream-counts name 0)
                                           :downstream (get @downstream-counts name 0)
                                           :total (+ (get @upstream-counts name 0) (get @downstream-counts name 0))})))
                            (sort-by :total >)
                            (take 8)
                            vec)
                  ;; Orphaned datasources (defined but never referenced)
                  referenced-sources (->> pipelines
                                          (mapcat #(concat (:input_sources %) (:output_sources %)))
                                          set)
                  orphaned (->> datasources
                                (remove #(referenced-sources (:name %)))
                                (map :name)
                                vec)
                  ;; Cluster distribution
                  cluster-counts (reduce (fn [m p]
                                             (update m (or (:cluster p) "unclustered") (fnil inc 0)))
                                         {} pipelines)
                  ;; Datasource type distribution
                  type-counts (reduce (fn [m ds]
                                          (update m (or (:type ds) "unknown") (fnil inc 0)))
                                      {} datasources)
                  ;; Coverage
                  schedules-covered (count (filter :schedule pipelines))
                  airflow-covered (count (filter #(get-in % [:links :airflow]) pipelines))]
                 {:counts {:pipelines (count pipelines)
                           :datasources (count datasources)
                           :clusters (count (remove #(= "unclustered" %) (keys cluster-counts)))}
                  :cycles cycles
                  :hubs hubs
                  :orphaned orphaned
                  :coverage {:schedules {:covered schedules-covered
                                         :total (count pipelines)
                                         :missing (mapv :name (remove :schedule pipelines))}
                             :airflow {:covered airflow-covered
                                       :total (count pipelines)
                                       :missing (mapv :name (remove #(get-in % [:links :airflow]) pipelines))}}
                  :distributions {:clusters cluster-counts
                                  :types type-counts}})))

;; Example Config (used for demo)
(def example-config
     {:clusters [{:name "user-processing"} {:name "order-management"} {:name "real-time" :parent "order-management"} {:name "analytics"}]
      :pipelines
      [{:name "user-enrichment" :description "Enriches user data" :input_sources ["raw_users" "user_events"]
        :output_sources ["enriched_users"] :schedule "Every 2 hours" :cluster "user-processing" :tags ["user-data" "ml"]
        :links {:airflow "https://airflow.company.com/dags/user_enrichment"}}
       {:name "order-processing" :description "Processes orders" :input_sources ["raw_orders" "inventory"]
        :output_sources ["processed_orders" "order_audit"] :schedule "Every 15 minutes" :cluster "real-time" :tags ["orders"]
        :links {:airflow "https://airflow.company.com/dags/order_processing"}}
       {:name "analytics-aggregation" :description "Daily aggregation" :input_sources ["enriched_users" "processed_orders" "user_events"]
        :output_sources ["daily_metrics" "user_cohorts"] :schedule "Daily at 1:00 AM" :cluster "analytics"
        :upstream_pipelines ["user-enrichment" "order-processing"] :tags ["analytics"]
        :links {:airflow "https://airflow.company.com/dags/analytics_agg"}}
       {:name "export-to-salesforce" :input_sources ["user_cohorts"] :output_sources ["salesforce_users"]
        :group "data-exports" :cluster "analytics" :upstream_pipelines ["analytics-aggregation"]
        :links {:airflow "https://airflow.company.com/dags/crm_exports"}}
       {:name "export-to-hubspot" :input_sources ["user_cohorts"] :output_sources ["hubspot_contacts"]
        :group "data-exports" :cluster "analytics" :upstream_pipelines ["analytics-aggregation"]
        :links {:airflow "https://airflow.company.com/dags/crm_exports"}}
       {:name "export-to-amplitude" :input_sources ["daily_metrics"] :output_sources ["amplitude_events"]
        :group "data-exports" :cluster "analytics" :upstream_pipelines ["analytics-aggregation"]
        :links {:airflow "https://airflow.company.com/dags/analytics_exports"}}
       {:name "weekly-rollup" :input_sources ["daily_metrics" "enriched_users"] :output_sources ["executive_summary"]
        :schedule "0 6 * * MON" :cluster "analytics" :upstream_pipelines ["analytics-aggregation" "user-enrichment"]
        :links {:airflow "https://airflow.company.com/dags/weekly_rollup"}}]
      :datasources
      [{:name "raw_users" :type "snowflake" :cluster "user-processing"
        :attributes [{:name "id"} {:name "first_name"} {:name "last_name"} {:name "email"} {:name "signup_date"}
                     {:name "address" :attributes [{:name "city"} {:name "zip"} {:name "geo" :attributes [{:name "lat"} {:name "lng"}]}]}]}
       {:name "user_events" :type "s3" :cluster "analytics"
        :attributes [{:name "event_id"} {:name "user_id" :from "raw_users::id"} {:name "event_type"} {:name "timestamp"}]}
       {:name "raw_orders" :type "api" :cluster "real-time"}
       {:name "inventory" :type "snowflake" :cluster "order-management"}
       {:name "enriched_users" :type "delta" :cluster "user-processing"
        :attributes [{:name "user_id" :from "raw_users::id"}
                     {:name "full_name" :from ["raw_users::first_name" "raw_users::last_name"]}
                     {:name "email" :from "raw_users::email"} {:name "event_count" :from "user_events::event_id"}
                     {:name "last_active" :from "user_events::timestamp"} {:name "signup_date" :from "raw_users::signup_date"}
                     {:name "location" :from "raw_users::address"
                      :attributes [{:name "city" :from "raw_users::address::city"} {:name "zip" :from "raw_users::address::zip"}
                                   {:name "coords" :from "raw_users::address::geo"}]}]}
       {:name "daily_metrics" :type "snowflake" :cluster "analytics"
        :attributes [{:name "date"} {:name "active_users" :from "enriched_users::user_id"}
                     {:name "total_events" :from "user_events::event_id"}]}
       {:name "user_cohorts" :type "snowflake" :cluster "analytics"}
       {:name "salesforce_users" :type "api" :cluster "analytics"}
       {:name "hubspot_contacts" :type "api" :cluster "analytics"}
       {:name "amplitude_events" :type "api" :cluster "analytics"}
       {:name "executive_summary" :type "snowflake" :cluster "analytics"
        :attributes [{:name "week"} {:name "weekly_active_users" :from "daily_metrics::active_users"}
                     {:name "weekly_events" :from "daily_metrics::total_events"}
                     {:name "user_growth" :from ["daily_metrics::active_users" "enriched_users::signup_date"]}]}]})
