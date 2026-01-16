(ns pipeviz.core
  "Browser entry point - DOM and d3 interop only"
  (:require [pipeviz.graph :as graph]
            ["d3" :as d3]
            ["d3-graphviz" :as d3-graphviz]))

;; State
(defonce state (atom {:config nil
                      :selected-node nil
                      :graphviz nil}))

;; DOM helpers
(defn $id [id] (.getElementById js/document id))
(defn on! [el event f] (.addEventListener el event f))

;; Graph rendering (d3 interop)
(defn render-graph! []
  (when-let [config (:config @state)]
    (when-let [container ($id "graph")]
      (let [dot (graph/generate-dot config)
            gv (-> (d3/select "#graph")
                   (.graphviz)
                   (.width (.-clientWidth container))
                   (.height 500)
                   (.fit true))]
        (swap! state assoc :graphviz gv)
        (.renderDot gv dot)))))

;; File handling
(defn load-config! [json-str]
  (try
    (let [config (js->clj (js/JSON.parse json-str) :keywordize-keys true)
          {:keys [valid? errors]} (graph/validate-config config)]
      (if valid?
        (do
          (swap! state assoc :config config)
          (render-graph!))
        (js/console.error "Invalid config:" (clj->js errors))))
    (catch :default e
      (js/console.error "Failed to parse config:" e))))

(defn handle-file-drop [e]
  (.preventDefault e)
  (when-let [file (-> e .-dataTransfer .-files (aget 0))]
    (let [reader (js/FileReader.)]
      (set! (.-onload reader) #(load-config! (-> % .-target .-result)))
      (.readAsText reader file))))

(defn handle-file-select [e]
  (when-let [file (-> e .-target .-files (aget 0))]
    (let [reader (js/FileReader.)]
      (set! (.-onload reader) #(load-config! (-> % .-target .-result)))
      (.readAsText reader file))))

;; Setup
(defn setup-drop-zone! []
  (when-let [drop-zone ($id "drop-zone")]
    (on! drop-zone "dragover" #(.preventDefault %))
    (on! drop-zone "drop" handle-file-drop))
  (when-let [input ($id "file-input")]
    (on! input "change" handle-file-select)))

;; Init
(defn init []
  (js/console.log "Pipeviz initialized")
  (setup-drop-zone!)
  ;; Load example on start
  (swap! state assoc :config graph/example-config)
  (render-graph!))

(defn reload []
  (when (:config @state)
    (render-graph!)))
