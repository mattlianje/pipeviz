(ns pipeviz.ui.state
    "Application state atoms")

;; Main application state
(defonce app (atom {:config nil
                    :selected-node nil
                    :graphviz nil
                    :dark? false
                    :pipelines-only? false
                    :lineage-cache nil
                    :blast-radius-cache nil
                    :blast-radius-graphviz nil
                    :collapsed-groups #{}
                    :dot-cache {}
                    :last-rendered-key nil}))

;; Filter state for tables
(defonce filters (atom {:pipeline-tags #{}
                        :pipeline-clusters #{}
                        :datasource-types #{}
                        :datasource-tags #{}
                        :datasource-clusters #{}}))

;; Attribute graph state
(defonce attributes (atom {:graphviz nil
                           :lineage-map nil
                           :datasource-lineage-map nil
                           :selected-attribute nil}))

;; Planner view state
(defonce planner (atom {:view :pipeline
                        :selected []
                        :graphviz nil
                        :picker-open? false}))
