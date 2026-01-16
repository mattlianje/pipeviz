(ns pipeviz.graph
  "Pure business logic for graph operations - no DOM, no JS interop"
  (:require [clojure.string :as str]))

;; Example config
(def example-config
  {:pipelines
   [{:name "ingest"
     :schedule "0 */2 * * *"
     :output_sources ["raw_events"]}
    {:name "validate"
     :input_sources ["raw_events"]
     :output_sources ["valid_events" "quarantine"]}
    {:name "enrich"
     :input_sources ["valid_events"]
     :output_sources ["enriched_events"]}
    {:name "aggregate"
     :schedule "0 0 * * *"
     :input_sources ["enriched_events"]
     :output_sources ["daily_stats"]}
    {:name "export"
     :input_sources ["daily_stats"]
     :output_sources ["data_warehouse"]}]
   :datasources
   [{:name "raw_events" :type "kafka"}
    {:name "valid_events" :type "kafka"}
    {:name "quarantine" :type "s3"}
    {:name "enriched_events" :type "kafka"}
    {:name "daily_stats" :type "postgres"}
    {:name "data_warehouse" :type "snowflake"}]})

;; DOT generation
(defn escape-dot [s]
  (-> s
      (str/replace "\\" "\\\\")
      (str/replace "\"" "\\\"")))

(defn pipeline-node [{:keys [name schedule]}]
  (let [safe-name (escape-dot name)]
    (if schedule
      (str "  \"" safe-name "\" [shape=box, style=\"filled,rounded\", fillcolor=\"#e3f2fd\", "
           "label=<" safe-name "<BR/><FONT POINT-SIZE=\"9\" COLOR=\"#d63384\"><I>" schedule "</I></FONT>>];")
      (str "  \"" safe-name "\" [shape=box, style=\"filled,rounded\", fillcolor=\"#e3f2fd\"];"))))

(defn datasource-node [{:keys [name]}]
  (str "  \"" (escape-dot name) "\" [shape=ellipse, style=filled, fillcolor=\"#f3e5f5\"];"))

(defn edges-for-pipeline [{:keys [name input_sources output_sources]}]
  (let [safe-name (escape-dot name)]
    (concat
     (for [src input_sources]
       (str "  \"" (escape-dot src) "\" -> \"" safe-name "\";"))
     (for [src output_sources]
       (str "  \"" safe-name "\" -> \"" (escape-dot src) "\";")))))

(defn generate-dot
  "Generate Graphviz DOT string from config"
  [{:keys [pipelines datasources]}]
  (let [pipeline-nodes (map pipeline-node pipelines)
        datasource-nodes (map datasource-node datasources)
        all-edges (mapcat edges-for-pipeline pipelines)]
    (str "digraph G {\n"
         "  rankdir=LR;\n"
         "  bgcolor=\"transparent\";\n"
         "  node [fontname=\"Arial\", fontsize=12];\n"
         (str/join "\n" (concat pipeline-nodes datasource-nodes))
         "\n"
         (str/join "\n" all-edges)
         "\n}")))

;; Graph analysis
(defn build-adjacency
  "Build upstream/downstream adjacency maps from config"
  [{:keys [pipelines]}]
  (let [output->producer (into {}
                               (for [p pipelines
                                     out (:output_sources p)]
                                 [out (:name p)]))]
    (reduce
     (fn [acc {:keys [name input_sources upstream_pipelines]}]
       (let [implicit-upstream (keep output->producer input_sources)
             all-upstream (distinct (concat upstream_pipelines implicit-upstream))]
         (reduce
          (fn [a up]
            (-> a
                (update-in [:downstream up] (fnil conj #{}) name)
                (update-in [:upstream name] (fnil conj #{}) up)))
          acc
          all-upstream)))
     {:upstream {} :downstream {}}
     pipelines)))

(defn reachable-from
  "Find all nodes reachable from start using adjacency map"
  [adj-map start]
  (loop [visited #{}
         queue [start]]
    (if (empty? queue)
      (disj visited start)
      (let [node (first queue)
            neighbors (get adj-map node #{})]
        (recur
         (conj visited node)
         (into (rest queue) (remove visited neighbors)))))))

(defn downstream-of
  "Get all downstream nodes from a starting node"
  [config node]
  (let [{:keys [downstream]} (build-adjacency config)]
    (reachable-from downstream node)))

(defn upstream-of
  "Get all upstream nodes from a starting node"
  [config node]
  (let [{:keys [upstream]} (build-adjacency config)]
    (reachable-from upstream node)))

;; Validation
(defn validate-config
  "Validate a pipeviz config, returns {:valid? bool :errors []}"
  [{:keys [pipelines datasources] :as config}]
  (let [errors (cond-> []
                 (nil? config)
                 (conj "Config is nil")

                 (and config (not (seq pipelines)))
                 (conj "No pipelines defined")

                 (some #(nil? (:name %)) pipelines)
                 (conj "Some pipelines missing name"))]
    {:valid? (empty? errors)
     :errors errors}))
