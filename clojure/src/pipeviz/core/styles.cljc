(ns pipeviz.core.styles
    "Centralized color palette and style definitions for Pipeviz.")

;; Core palette
;; blue = pipelines, purple = datasources, orange = groups/dependencies
(def colors
     {:blue       "#1976d2"  :blue-bg    "#e3f2fd"
      :purple     "#7b1fa2"  :purple-bg  "#f3e5f5"
      :orange     "#ff6b35"  :orange-bg  "#fff3e0"  :orange-dark "#e65100"
      :green      "#388e3c"  :green-bg   "#e8f5e9"
      :pink       "#d63384"  :red        "#d32f2f"
      :tag-bg     "#fff3cd"  :tag-text   "#856404"
      :gray       "#616161"  :gray-muted "#999"     :gray-border "#94a3b8"
      :edge-dark  "#b0b0b0"  :edge-light "#555"})

;; Cluster subgraph colors (cycles through for multiple clusters)
(def cluster-colors
     ["#1976d2" "#7b1fa2" "#388e3c" "#f57c00" "#d32f2f" "#616161" "#c2185b" "#303f9f"])

;; DOT node shapes and fills by type
(def dot-nodes
     {:pipeline   {:shape "box"     :style "filled,rounded" :fill (:blue-bg colors)   :border (:blue colors)}
      :datasource {:shape "ellipse" :style "filled"         :fill (:purple-bg colors) :border (:purple colors)}
      :group      {:shape "box3d"   :style "filled"         :fill (:orange-bg colors) :border (:orange colors)}})

(defn dot-node-attrs [node-type]
      (when-let [{:keys [shape style fill border]} (dot-nodes node-type)]
                (str "[shape=" shape " style=\"" style "\" fillcolor=\"" fill "\" color=\"" border "\"]")))

;; Edge colors
(defn edge-color [dark?] (if dark? (:edge-dark colors) (:edge-light colors)))
(def dependency-edge-color (:orange colors))

;; Badge styles (group and tag badges in side panel)
(def badges
     {:group {:bg (:orange-bg colors) :text (:orange-dark colors)}
      :tag   {:bg (:tag-bg colors)    :text (:tag-text colors)}})

(defn badge-style [badge-type]
      (when-let [{:keys [bg text]} (badges badge-type)]
                (str "background:" bg ";color:" text)))

;; Lineage tree indentation and fade
(defn lineage-style [depth]
      (str "padding-left:" (* (dec depth) 12) "px;opacity:" (max 0.5 (- 1 (* (dec depth) 0.15)))))

;; Attribute lineage graph colors
(def attr-graph
     {:node-fill     "#e2e8f0"  :node-default   "#ffffff"
      :struct-fill   "#dde5ed"  :struct-default "#e8eef4"
      :struct-text   (:purple colors) :text-default   "#334155"
      :struct-border (:purple colors) :border-default "#94a3b8"
      :edge (:purple colors)})

(defn attr-struct-style [has-lineage?]
      (let [suffix (if has-lineage? "" "-default")]
           {:fill   ((keyword (str "struct-fill" suffix)) attr-graph)
            :text   ((keyword (str "struct-text" suffix)) attr-graph)
            :border ((keyword (str "struct-border" suffix)) attr-graph)}))

(defn attr-node-fill [has-lineage?]
      (if has-lineage? (:node-fill attr-graph) (:node-default attr-graph)))

;; Blast radius / backfill wave palettes (6 depth levels, light/dark)
(defn- make-palette [fills borders text]
       (mapv #(hash-map :fill %1 :border %2 :text text) fills borders))

(def blast-radius-colors
     (let [borders ["#c98b8b" "#d4a574" "#c4c474" "#7cb47c" "#6b9dc4" "#a88bc4"]]
          {:light (make-palette ["#fce4ec" "#fff3e0" "#fffde7" "#e8f5e9" "#e3f2fd" "#f3e5f5"] borders "#495057")
           :dark  (make-palette ["#4a2a2a" "#4a3a2a" "#4a4a2a" "#2a4a3a" "#2a3a4a" "#3a2a4a"] borders "#e0e0e0")}))

(def wave-colors
     (let [borders ["#81c784" "#64b5f6" "#ba68c8" "#ffb74d" "#4dd0e1" "#f48fb1"]]
          {:light (make-palette ["#e8f5e9" "#e3f2fd" "#f3e5f5" "#fff3e0" "#e0f7fa" "#fce4ec"] borders "#495057")
           :dark  (make-palette ["#1b3d1b" "#1a2d3d" "#2d1a2d" "#3d2d1a" "#1a3d3d" "#3d1a2d"] borders "#e0e0e0")}))

(def wave0-color
     {:light {:fill "#fef3e2" :border "#d4915c" :text "#495057"}
      :dark  {:fill "#3d2d1a" :border "#d4915c" :text "#e0e0e0"}})

(defn get-depth-color [depth dark?]
      (let [palette ((if dark? :dark :light) blast-radius-colors)]
           (nth palette (min depth (dec (count palette))))))

(defn get-wave-color [wave dark?]
      (if (zero? wave)
          ((if dark? :dark :light) wave0-color)
          (let [palette ((if dark? :dark :light) wave-colors)]
               (nth palette (mod (dec wave) (count palette))))))
