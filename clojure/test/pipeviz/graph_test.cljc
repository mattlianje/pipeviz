(ns pipeviz.graph-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.string :as str]
            [pipeviz.graph :as graph]))

;; =============================================================================
;; Cron Tests
;; =============================================================================

(deftest cron?-test
  (testing "valid cron expressions"
    (is (graph/cron? "* * * * *"))
    (is (graph/cron? "0 0 * * *"))
    (is (graph/cron? "*/15 * * * *"))
    (is (graph/cron? "0 9 * * MON-FRI")))
  (testing "invalid cron expressions"
    (is (not (graph/cron? nil)))
    (is (not (graph/cron? "")))
    (is (not (graph/cron? "daily")))
    (is (not (graph/cron? "0 0 * *")))
    (is (not (graph/cron? "0 0 * * * *")))))

(deftest format-schedule-test
  (testing "non-cron passthrough"
    (is (= "daily" (graph/format-schedule "daily")))
    (is (nil? (graph/format-schedule nil))))
  (testing "every N minutes"
    (is (= "Every 15 minutes" (graph/format-schedule "*/15 * * * *"))))
  (testing "every N hours"
    (is (= "Every 2 hours" (graph/format-schedule "0 */2 * * *"))))
  (testing "daily at specific time"
    (is (= "Daily at 9 AM" (graph/format-schedule "0 9 * * *")))
    (is (= "Daily at 9:30 AM" (graph/format-schedule "30 9 * * *"))))
  (testing "weekday patterns"
    (is (= "Weekdays at 9 AM" (graph/format-schedule "0 9 * * 1-5")))
    (is (= "Mondays at 8 AM" (graph/format-schedule "0 8 * * 1"))))
  (testing "day of month"
    (is (= "1st of each month at 12 AM" (graph/format-schedule "0 0 1 * *")))))

;; =============================================================================
;; Graph Tests
;; =============================================================================

(def test-config
  {:pipelines
   [{:name "ingest" :output_sources ["raw"]}
    {:name "transform" :input_sources ["raw"] :output_sources ["clean"]}
    {:name "load" :input_sources ["clean"]}]
   :datasources
   [{:name "raw" :type "kafka"}
    {:name "clean" :type "postgres"}]})

(deftest escape-dot-test
  (testing "escapes special characters"
    (is (= "foo" (graph/escape-dot "foo")))
    (is (= "foo\\\"bar" (graph/escape-dot "foo\"bar")))
    (is (= "foo\\\\bar" (graph/escape-dot "foo\\bar")))))

(deftest generate-dot-test
  (testing "generates valid DOT"
    (let [dot (graph/generate-dot test-config)]
      (is (str/starts-with? dot "digraph G {"))
      (is (str/ends-with? (str/trim dot) "}"))
      (is (str/includes? dot "\"ingest\""))
      (is (str/includes? dot "\"transform\""))
      (is (str/includes? dot "\"raw\""))
      (is (str/includes? dot "->"))))

  (testing "respects show-datasources? option"
    (let [dot (graph/generate-dot test-config {:show-datasources? false})]
      (is (str/includes? dot "\"ingest\""))
      (is (not (str/includes? dot "shape=ellipse"))))))

(deftest build-pipeline-adjacency-test
  (testing "builds correct adjacency maps"
    (let [{:keys [upstream downstream]} (graph/build-pipeline-adjacency test-config)]
      (is (= #{"ingest"} (upstream "transform")))
      (is (= #{"transform"} (upstream "load")))
      (is (= #{"transform"} (downstream "ingest")))
      (is (= #{"load"} (downstream "transform"))))))

(deftest reachable-test
  (testing "finds all reachable nodes"
    (let [{:keys [downstream]} (graph/build-pipeline-adjacency test-config)]
      (is (= #{"transform" "load"} (graph/reachable "ingest" #(get downstream % #{}))))
      (is (= #{"load"} (graph/reachable "transform" #(get downstream % #{}))))
      (is (= #{} (graph/reachable "load" #(get downstream % #{})))))))

(deftest upstream-downstream-test
  (testing "upstream-of"
    (is (= #{"ingest" "transform"} (graph/upstream-of test-config "load")))
    (is (= #{"ingest"} (graph/upstream-of test-config "transform")))
    (is (= #{} (graph/upstream-of test-config "ingest"))))

  (testing "downstream-of"
    (is (= #{"transform" "load"} (graph/downstream-of test-config "ingest")))
    (is (= #{"load"} (graph/downstream-of test-config "transform")))
    (is (= #{} (graph/downstream-of test-config "load")))))

(deftest lineage-with-depth-test
  (testing "returns nodes with depth"
    (let [{:keys [downstream]} (graph/build-pipeline-adjacency test-config)
          lineage (graph/lineage-with-depth downstream "ingest")]
      (is (= 2 (count lineage)))
      (is (some #(and (= "transform" (:name %)) (= 1 (:depth %))) lineage))
      (is (some #(and (= "load" (:name %)) (= 2 (:depth %))) lineage)))))

(deftest full-lineage-test
  (testing "returns complete lineage"
    (let [{:keys [upstream downstream]} (graph/full-lineage test-config "transform")]
      (is (= 1 (count upstream)))
      (is (= "ingest" (:name (first upstream))))
      (is (= 1 (count downstream)))
      (is (= "load" (:name (first downstream)))))))

(deftest build-full-adjacency-test
  (testing "builds adjacency including datasources"
    (let [{:keys [upstream downstream]} (graph/build-full-adjacency test-config)]
      ;; raw -> transform (datasource feeds pipeline)
      (is (= ["transform"] (downstream "raw")))
      ;; transform -> clean (pipeline produces datasource)
      (is (= ["clean"] (downstream "transform")))
      ;; clean -> load (datasource feeds pipeline)
      (is (= ["load"] (downstream "clean")))
      ;; Upstream: transform <- raw
      (is (= ["raw"] (upstream "transform")))
      ;; Upstream: clean <- transform
      (is (= ["transform"] (upstream "clean"))))))

(deftest full-graph-lineage-test
  (testing "lineage includes datasources"
    (let [{:keys [upstream downstream]} (graph/full-graph-lineage test-config "transform")]
      ;; Upstream of transform: raw -> ingest
      (is (= 2 (count upstream)))
      (is (some #(= "raw" (:name %)) upstream))
      (is (some #(= "ingest" (:name %)) upstream))
      ;; Downstream of transform: clean, load
      (is (= 2 (count downstream)))
      (is (some #(= "clean" (:name %)) downstream))
      (is (some #(= "load" (:name %)) downstream))))

  (testing "lineage from datasource"
    (let [{:keys [upstream downstream]} (graph/full-graph-lineage test-config "raw")]
      ;; raw is produced by ingest
      (is (= 1 (count upstream)))
      (is (= "ingest" (:name (first upstream))))
      ;; Downstream: transform, clean, load
      (is (= 3 (count downstream)))))

  (testing "full chain from ingest"
    (let [{:keys [downstream]} (graph/full-graph-lineage test-config "ingest")]
      ;; ingest -> raw -> transform -> clean -> load
      (is (= 4 (count downstream)))
      (is (some #(= "raw" (:name %)) downstream))
      (is (some #(= "transform" (:name %)) downstream))
      (is (some #(= "clean" (:name %)) downstream))
      (is (some #(= "load" (:name %)) downstream)))))

(deftest validate-config-test
  (testing "valid config"
    (let [{:keys [valid? errors]} (graph/validate-config test-config)]
      (is valid?)
      (is (empty? errors))))

  (testing "nil config"
    (let [{:keys [valid? errors]} (graph/validate-config nil)]
      (is (not valid?))
      (is (some #(str/includes? % "nil") errors))))

  (testing "empty pipelines"
    (let [{:keys [valid? errors]} (graph/validate-config {:pipelines []})]
      (is (not valid?))
      (is (some #(str/includes? % "No pipelines") errors))))

  (testing "pipeline missing name"
    (let [{:keys [valid? errors]} (graph/validate-config {:pipelines [{:schedule "daily"}]})]
      (is (not valid?))
      (is (some #(str/includes? % "missing name") errors)))))

(deftest fuzzy-match-test
  (testing "exact substring match"
    (let [{:keys [match score]} (graph/fuzzy-match "transform" "trans")]
      (is match)
      (is (> score 0.5))))

  (testing "case insensitive"
    (let [{:keys [match]} (graph/fuzzy-match "Transform" "trans")]
      (is match)))

  (testing "fuzzy character match"
    (let [{:keys [match]} (graph/fuzzy-match "user-enrichment" "ue")]
      (is match)))

  (testing "no match"
    (let [{:keys [match score]} (graph/fuzzy-match "transform" "xyz")]
      (is (not match))
      (is (zero? score))))

  (testing "word boundary bonus"
    (let [boundary-match (graph/fuzzy-match "user_events" "ue")
          no-boundary-match (graph/fuzzy-match "superenrich" "ue")]
      (is (:match boundary-match))
      (is (:match no-boundary-match))
      ;; Boundary match should score higher
      (is (> (:score boundary-match) (:score no-boundary-match))))))

(deftest highlight-match-test
  (testing "substring match"
    (is (= "trans<span class=\"result-match\">form</span>ation"
           (graph/highlight-match "transformation" "form"))))

  (testing "case insensitive highlight"
    (is (= "<span class=\"result-match\">Trans</span>form"
           (graph/highlight-match "Transform" "trans"))))

  (testing "fuzzy match highlight"
    (let [result (graph/highlight-match "user" "ur")]
      (is (str/includes? result "<span class=\"result-match\">u</span>"))
      (is (str/includes? result "<span class=\"result-match\">r</span>")))))

(deftest search-nodes-test
  (testing "finds pipelines by name"
    (let [results (graph/search-nodes test-config "trans")]
      (is (= 1 (count results)))
      (is (= "transform" (:name (first results))))
      (is (= :pipeline (:type (first results))))))

  (testing "finds datasources"
    (let [results (graph/search-nodes test-config "raw")]
      (is (some #(= "raw" (:name %)) results))))

  (testing "empty query returns nil"
    (is (nil? (graph/search-nodes test-config "")))
    (is (nil? (graph/search-nodes test-config "   "))))

  (testing "results sorted by score"
    (let [config {:pipelines [{:name "abc" :description "xyz"}
                              {:name "abcdef"}]
                  :datasources []}
          results (graph/search-nodes config "abc")]
      ;; Shorter name match should score higher
      (is (= "abc" (:name (first results)))))))

;; Attribute lineage tests
(def attr-test-config
  {:pipelines []
   :datasources
   [{:name "source"
     :type "snowflake"
     :attributes [{:name "id"}
                  {:name "name"}
                  {:name "nested"
                   :attributes [{:name "field1"}
                                {:name "field2"}]}]}
    {:name "target"
     :type "delta"
     :attributes [{:name "source_id" :from "source::id"}
                  {:name "source_name" :from "source::name"}
                  {:name "combined" :from ["source::id" "source::name"]}]}]})

(deftest build-attribute-lineage-map-test
  (testing "builds attribute map"
    (let [{:keys [attribute-map]} (graph/build-attribute-lineage-map attr-test-config)]
      ;; Has source attributes
      (is (contains? attribute-map "source__id"))
      (is (contains? attribute-map "source__name"))
      (is (contains? attribute-map "source__nested__field1"))
      ;; Has target attributes
      (is (contains? attribute-map "target__source_id"))
      (is (contains? attribute-map "target__source_name"))))

  (testing "tracks upstream/downstream"
    (let [{:keys [attribute-map]} (graph/build-attribute-lineage-map attr-test-config)
          source-id (get attribute-map "source__id")
          target-id (get attribute-map "target__source_id")]
      ;; source::id has downstream
      (is (seq (:downstream source-id)))
      ;; target::source_id has upstream
      (is (seq (:upstream target-id)))))

  (testing "computes full chains"
    (let [{:keys [attribute-map]} (graph/build-attribute-lineage-map attr-test-config)
          target-id (get attribute-map "target__source_id")]
      (is (seq (:full-upstream target-id)))))

  (testing "builds datasource map"
    (let [{:keys [datasource-map]} (graph/build-attribute-lineage-map attr-test-config)]
      (is (contains? datasource-map "source"))
      (is (contains? datasource-map "target"))
      ;; target has source as upstream
      (let [target-ds (get datasource-map "target")]
        (is (some #(= "source" (:id %)) (:upstream target-ds)))))))

(deftest attribute-provenance-test
  (testing "returns provenance for attribute"
    (let [{:keys [attribute-map]} (graph/build-attribute-lineage-map attr-test-config)
          prov (graph/attribute-provenance attribute-map "target__source_id")]
      (is (= "target__source_id" (:attribute prov)))
      (is (= "source_id" (:name prov)))
      (is (= "target" (:datasource prov)))
      (is (seq (get-in prov [:upstream :attributes])))))

  (testing "respects max-depth"
    (let [{:keys [attribute-map]} (graph/build-attribute-lineage-map attr-test-config)
          prov-full (graph/attribute-provenance attribute-map "target__source_id")
          prov-depth1 (graph/attribute-provenance attribute-map "target__source_id" {:max-depth 1})]
      ;; Should have same or fewer upstream with depth limit
      (is (<= (count (get-in prov-depth1 [:upstream :attributes]))
              (count (get-in prov-full [:upstream :attributes]))))))

  (testing "returns nil for unknown attribute"
    (is (nil? (graph/attribute-provenance {} "unknown__attr")))))

;; Pipeline groups tests
(def group-test-config
  {:pipelines
   [{:name "ingest" :output_sources ["raw"]}
    {:name "etl-1" :input_sources ["raw"] :output_sources ["clean"] :group "etl"}
    {:name "etl-2" :input_sources ["raw"] :output_sources ["enriched"] :group "etl"}
    {:name "report" :input_sources ["clean" "enriched"]}]
   :datasources
   [{:name "raw" :type "kafka"}
    {:name "clean" :type "postgres"}
    {:name "enriched" :type "postgres"}]})

(deftest build-group-data-test
  (testing "builds group membership"
    (let [{:keys [groups pipeline->group]} (graph/build-group-data group-test-config)]
      (is (= "etl" (pipeline->group "etl-1")))
      (is (= "etl" (pipeline->group "etl-2")))
      (is (nil? (pipeline->group "ingest")))
      (is (= #{"etl-1" "etl-2"} (set (:members (groups "etl")))))))

  (testing "computes group inputs/outputs"
    (let [{:keys [groups]} (graph/build-group-data group-test-config)
          etl-group (groups "etl")]
      ;; ETL group reads from raw
      (is (contains? (:inputs etl-group) "raw"))
      ;; ETL group outputs clean and enriched
      (is (= #{"clean" "enriched"} (:outputs etl-group))))))

(deftest all-groups-test
  (testing "returns unique groups"
    (is (= ["etl"] (graph/all-groups group-test-config)))
    (is (empty? (graph/all-groups test-config)))))

(deftest generate-dot-with-collapsed-groups-test
  (testing "collapsed group renders as single node"
    (let [dot (graph/generate-dot group-test-config {:collapsed-groups #{"etl"}})]
      (is (str/includes? dot "group:etl"))
      (is (str/includes? dot "2 pipelines"))
      ;; Individual pipelines should not appear
      (is (not (str/includes? dot "\"etl-1\"")))))

  (testing "expanded group renders individual pipelines"
    (let [dot (graph/generate-dot group-test-config {:collapsed-groups #{}})]
      (is (str/includes? dot "\"etl-1\""))
      (is (str/includes? dot "\"etl-2\""))
      (is (not (str/includes? dot "group:etl"))))))

;; Topological sort / Execution waves tests
(deftest kahns-waves-test
  (testing "simple linear chain"
    (let [nodes #{"a" "b" "c"}
          edges [["a" "b"] ["b" "c"]]
          waves (graph/kahns-waves nodes edges)]
      (is (= [["a"] ["b"] ["c"]] waves))))

  (testing "parallel nodes"
    (let [nodes #{"a" "b" "c" "d"}
          edges [["a" "b"] ["a" "c"] ["b" "d"] ["c" "d"]]
          waves (graph/kahns-waves nodes edges)]
      (is (= 3 (count waves)))
      (is (= ["a"] (first waves)))
      (is (= #{"b" "c"} (set (second waves))))
      (is (= ["d"] (last waves)))))

  (testing "returns nil for cyclic graph"
    (let [nodes #{"a" "b" "c"}
          edges [["a" "b"] ["b" "c"] ["c" "a"]]]
      (is (nil? (graph/kahns-waves nodes edges))))))

(deftest compute-execution-waves-test
  (testing "computes waves for selected pipelines"
    (let [result (graph/compute-execution-waves (:pipelines test-config) ["ingest"])]
      (is (some? result))
      (is (vector? (:waves result)))
      (is (>= (:node-count result) 1))))

  (testing "includes downstream pipelines"
    (let [result (graph/compute-execution-waves (:pipelines test-config) ["ingest"])]
      (is (= 3 (:node-count result)))
      (is (some #(some (fn [n] (= "transform" n)) %) (:waves result)))
      (is (some #(some (fn [n] (= "load" n)) %) (:waves result)))))

  (testing "returns nil for empty selection"
    (is (nil? (graph/compute-execution-waves (:pipelines test-config) [])))))

;; Blast radius tests
(deftest build-blast-radius-graph-test
  (testing "builds graph structure"
    (let [g (graph/build-blast-radius-graph test-config)]
      (is (map? (:downstream g)))
      (is (map? (:node-types g)))
      (is (= :pipeline ((:node-types g) "ingest")))
      (is (= :datasource ((:node-types g) "raw")))))

  (testing "downstream includes data flow"
    (let [g (graph/build-blast-radius-graph test-config)]
      (is (contains? ((:downstream g) "ingest") "raw"))
      (is (contains? ((:downstream g) "raw") "transform")))))

(deftest blast-radius-for-node-test
  (testing "computes blast radius from pipeline"
    (let [g (graph/build-blast-radius-graph test-config)
          result (graph/blast-radius-for-node g "ingest")]
      (is (= "ingest" (:source result)))
      (is (= :pipeline (:source-type result)))
      (is (pos? (:total-affected result)))
      (is (vector? (:downstream result)))
      (is (map? (:by-depth result)))))

  (testing "blast from datasource"
    (let [g (graph/build-blast-radius-graph test-config)
          result (graph/blast-radius-for-node g "raw")]
      (is (= "raw" (:source result)))
      (is (= :datasource (:source-type result)))
      (is (some #(= "transform" (:name %)) (:downstream result)))))

  (testing "returns nil for nil node"
    (let [g (graph/build-blast-radius-graph test-config)]
      (is (nil? (graph/blast-radius-for-node g nil))))))

;; Backfill analysis tests
(deftest generate-backfill-analysis-test
  (testing "generates backfill plan"
    (let [result (graph/generate-backfill-analysis test-config ["ingest"])]
      (is (= ["ingest"] (:nodes result)))
      (is (pos? (:total-waves result)))
      (is (vector? (:waves result)))
      (is (vector? (:edges result)))))

  (testing "includes downstream in waves"
    (let [result (graph/generate-backfill-analysis test-config ["ingest"])]
      (is (>= (:total-downstream-pipelines result) 2))
      (is (some #(some (fn [p] (= "transform" (:name p))) (:pipelines %)) (:waves result)))))

  (testing "returns nil for empty selection"
    (is (nil? (graph/generate-backfill-analysis test-config []))))

  (testing "throws for non-pipeline nodes"
    (is (thrown? #?(:clj Exception :cljs js/Error)
                 (graph/generate-backfill-analysis test-config ["raw"])))))

