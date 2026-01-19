(ns pipeviz.styles-test
  (:require [clojure.test :refer [deftest is testing]]
            [pipeviz.styles :as styles]))

(deftest colors-test
  (testing "core colors exist"
    (is (= "#1976d2" (:blue styles/colors)))
    (is (= "#7b1fa2" (:purple styles/colors)))
    (is (= "#ff6b35" (:orange styles/colors))))
  (testing "cluster colors are a vector of 8"
    (is (= 8 (count styles/cluster-colors)))
    (is (every? string? styles/cluster-colors))))

(deftest dot-nodes-test
  (testing "all node types defined"
    (is (contains? styles/dot-nodes :pipeline))
    (is (contains? styles/dot-nodes :datasource))
    (is (contains? styles/dot-nodes :group)))
  (testing "dot-node-attrs generates valid DOT"
    (is (re-find #"shape=box" (styles/dot-node-attrs :pipeline)))
    (is (re-find #"shape=ellipse" (styles/dot-node-attrs :datasource)))
    (is (nil? (styles/dot-node-attrs :nonexistent)))))

(deftest edge-color-test
  (is (= "#b0b0b0" (styles/edge-color true)))
  (is (= "#555" (styles/edge-color false)))
  (is (= "#ff6b35" styles/dependency-edge-color)))

(deftest badge-style-test
  (testing "generates valid CSS"
    (is (= "background:#fff3e0;color:#e65100" (styles/badge-style :group)))
    (is (= "background:#fff3cd;color:#856404" (styles/badge-style :tag)))
    (is (nil? (styles/badge-style :nonexistent)))))

(deftest lineage-style-test
  (testing "depth 1 has no indent, full opacity"
    (is (re-find #"padding-left:0px;opacity:1" (styles/lineage-style 1))))
  (testing "deeper depths have more indent, lower opacity"
    (is (re-find #"padding-left:12px" (styles/lineage-style 2)))
    (is (re-find #"padding-left:24px" (styles/lineage-style 3))))
  (testing "opacity floors at 0.5"
    (is (re-find #"opacity:0\.5" (styles/lineage-style 10)))))

(deftest attr-graph-test
  (testing "attr-struct-style returns different colors based on lineage"
    (let [with-lin (styles/attr-struct-style true)
          without  (styles/attr-struct-style false)]
      (is (not= (:fill with-lin) (:fill without)))
      (is (every? #(contains? % :fill) [with-lin without]))
      (is (every? #(contains? % :text) [with-lin without]))
      (is (every? #(contains? % :border) [with-lin without]))))
  (testing "attr-node-fill"
    (is (= "#e2e8f0" (styles/attr-node-fill true)))
    (is (= "#ffffff" (styles/attr-node-fill false)))))

(deftest visualization-colors-test
  (testing "blast radius palettes have 6 entries each"
    (is (= 6 (count (:light styles/blast-radius-colors))))
    (is (= 6 (count (:dark styles/blast-radius-colors)))))
  (testing "wave palettes have 6 entries each"
    (is (= 6 (count (:light styles/wave-colors))))
    (is (= 6 (count (:dark styles/wave-colors)))))
  (testing "get-depth-color returns valid maps"
    (let [c (styles/get-depth-color 0 false)]
      (is (contains? c :fill))
      (is (contains? c :border))
      (is (contains? c :text))))
  (testing "get-depth-color clamps to max depth"
    (is (= (styles/get-depth-color 5 false) (styles/get-depth-color 100 false))))
  (testing "get-wave-color handles wave 0 specially"
    (is (= (:light styles/wave0-color) (styles/get-wave-color 0 false)))
    (is (= (:dark styles/wave0-color) (styles/get-wave-color 0 true))))
  (testing "get-wave-color cycles through palette"
    (is (= (styles/get-wave-color 1 false) (styles/get-wave-color 7 false)))))
