(ns pipeviz.ui.export
  "Export view for generating JSON, Mermaid, and DOT exports"
  (:require [clojure.string :as str]
            [pipeviz.core.graph :as core]
            [pipeviz.ui.dom :refer [$id on! set-html! add-class! remove-class!
                                    icon-copy icon-check]]
            [pipeviz.ui.state :as state]))

(defonce ^:private current-format (atom :json))

(defn- generate-dot-export []
  (when-let [config (:config @state/app)]
    (core/generate-dot config {:dark? false
                               :show-datasources? true
                               :collapsed-groups #{}})))

(defn update-export! []
  (when-let [output ($id "export-output")]
    (let [config (:config @state/app)
          content (case @current-format
                    :mermaid (or (core/generate-mermaid-export config)
                                 "Load a configuration to see the Mermaid export...")
                    :dot (or (generate-dot-export)
                             "Load a configuration to see the DOT export...")
                    (if-let [export (core/generate-graph-export config)]
                      (js/JSON.stringify (clj->js export) nil 2)
                      "Load a configuration to see the graph export..."))]
      (set! (.-textContent output) content))))

(defn set-format! [format-name]
  (let [fmt (keyword format-name)]
    (reset! current-format fmt)
    ;; Update button states
    (doseq [btn (array-seq (.querySelectorAll js/document ".export-format-btn"))]
      (if (= format-name (.getAttribute btn "data-format"))
        (add-class! btn "active")
        (remove-class! btn "active")))
    (update-export!)))

(defn copy-export! []
  (when-let [output ($id "export-output")]
    (let [text (.-textContent output)]
      (when-not (str/blank? text)
        (when-let [btn (.querySelector js/document ".export-copy-btn")]
          (-> (js/navigator.clipboard.writeText text)
              (.then (fn []
                       (add-class! btn "copied")
                       (set! (.-innerHTML btn) icon-check)
                       (js/setTimeout #(do (remove-class! btn "copied")
                                           (set! (.-innerHTML btn) icon-copy))
                                      1500)))
              (.catch #(js/console.error "Failed to copy:" %))))))))

(defn init! []
  (update-export!))
