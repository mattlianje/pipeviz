(ns pipeviz.ui.planner
  "Planner view for backfill and Airflow analysis"
  (:require ["d3" :as d3]
            [clojure.string :as str]
            [pipeviz.core.graph :as core]
            [pipeviz.ui.dom :refer [$id on! set-html! add-class! remove-class!
                                    icon-copy icon-check]]
            [pipeviz.ui.hash :as hash]
            [pipeviz.ui.state :as state]))

(defn- dark? []
  (= "dark" (.getAttribute js/document.documentElement "data-theme")))

(def ^:private hints
  {:pipeline "Select pipelines to plan backfill execution order"
   :airflow "Maps pipelines to Airflow DAGs based on links"
   :blast "Select a node to see all downstream dependencies affected by changes"})

(defn- update-picker-label! []
  (when-let [label ($id "planner-picker-label")]
    (let [selected (:selected @state/planner)
          view (:view @state/planner)
          text (cond
                 (empty? selected) (if (= view :blast) "Select a node..." "Select pipelines...")
                 (= view :blast) (first selected)
                 :else (str (count selected) " selected"))]
      (set! (.-textContent label) text))))

(defn- update-count! []
  (let [{:keys [view selected]} @state/planner
        n (count selected)]
    ;; Update footer count
    (when-let [count-el ($id "planner-picker-count")]
      (set! (.-textContent count-el) (if (pos? n) (str n " selected") "")))
    ;; Update button badge
    (doseq [v ["pipeline" "airflow" "blast"]]
      (when-let [badge ($id (str "planner-count-" v))]
        (set! (.-textContent badge)
              (if (and (= (name view) v) (pos? n)) (str n) ""))))))

(defn toggle-picker! []
  (when-let [dropdown ($id "planner-picker-dropdown")]
    (let [is-open? (.contains (.-classList dropdown) "show")]
      (if is-open?
        (remove-class! dropdown "show")
        (add-class! dropdown "show"))
      (swap! state/planner assoc :picker-open? (not is-open?)))))

(defn close-picker! []
  (when-let [dropdown ($id "planner-picker-dropdown")]
    (remove-class! dropdown "show")
    (swap! state/planner assoc :picker-open? false)))

(declare render-graph! populate-picker!)

(defn render-graph! []
  (when-let [config (:config @state/app)]
    (let [container ($id "planner-graph")
          output ($id "planner-output")
          {:keys [view selected]} @state/planner
          blast-cache (:blast-radius-cache @state/app)]
      (when container
        ;; Update URL hash with planner state
        (when (seq selected)
          (hash/update-planner! view selected))
        (if (empty? selected)
          ;; No selection - show placeholder and reset graphviz instance
          (do
            (swap! state/planner assoc :graphviz nil)
            (set! (.-innerHTML container) "<div class='planner-placeholder'>Select pipelines to see the execution plan</div>")
            (when output (set! (.-textContent output) "")))
          ;; Generate analysis and render
          (let [analysis (case view
                           :blast (core/generate-blast-radius-analysis config (first selected) blast-cache)
                           :airflow (let [backfill (core/generate-backfill-analysis config selected)]
                                      (core/generate-airflow-analysis config backfill))
                           (core/generate-backfill-analysis config selected))
                dot (case view
                      :blast (core/generate-blast-radius-dot analysis (dark?))
                      :airflow (core/generate-airflow-dot analysis (dark?))
                      (core/generate-backfill-dot analysis (dark?)))]
            ;; Update output JSON
            (when output
              (set! (.-textContent output) (js/JSON.stringify (clj->js analysis) nil 2)))
            ;; Render graph - reuse graphviz instance for smooth morphing
            (if dot
              (try
                (let [existing-gv (:graphviz @state/planner)
                      width (or (.-clientWidth container) 500)]
                  (if existing-gv
                    ;; Reuse existing instance for smooth morph transition
                    (.renderDot existing-gv dot)
                    ;; Create new instance (first render)
                    (do
                      (set! (.-innerHTML container) "")
                      (let [gv (-> (.select d3 "#planner-graph")
                                   (.graphviz)
                                   (.width width)
                                   (.height 450)
                                   (.fit true)
                                   (.zoom false)
                                   (.transition (fn [] (-> (d3/transition "planner")
                                                           (.duration 300)
                                                           (.ease (.-easeCubicInOut d3)))))
                                   (.on "end"
                                        (fn []
                                          (let [svg (.select d3 "#planner-graph svg")
                                                g (.select svg "g")
                                                zoom (-> (.zoom d3)
                                                         (.scaleExtent #js [0.1 4])
                                                         (.on "zoom" (fn [event]
                                                                       (.attr g "transform" (.-transform event)))))]
                                            (.call svg zoom)))))]
                        (swap! state/planner assoc :graphviz gv)
                        (.renderDot gv dot)))))
                (catch :default e
                  (js/console.error "Planner graph error:" e)
                  (swap! state/planner assoc :graphviz nil)
                  (set! (.-innerHTML container) "<div class='planner-placeholder'>Error rendering graph</div>")))
              (do
                ;; Reset graphviz instance since we're replacing the container content
                (swap! state/planner assoc :graphviz nil)
                (set! (.-innerHTML container) "<div class='planner-placeholder'>No dependencies to visualize</div>")))))))))

(defn- handle-item-change! [e]
  (let [input (.-target e)
        name (.-value input)
        checked? (.-checked input)
        view (:view @state/planner)]
    (if (= view :blast)
      ;; Blast mode - single selection (radio)
      (swap! state/planner assoc :selected (if checked? [name] []))
      ;; Pipeline/Airflow mode - multiple selection (checkbox)
      (swap! state/planner update :selected
             (fn [sel]
               (if checked?
                 (conj (vec sel) name)
                 (vec (remove #(= % name) sel))))))
    (update-picker-label!)
    (update-count!)
    (render-graph!)))

(defn populate-picker! []
  (when-let [config (:config @state/app)]
    (when-let [list-el ($id "planner-picker-list")]
      (let [{:keys [view selected]} @state/planner
            pipelines (or (:pipelines config) [])
            datasources (or (:datasources config) [])
            selected-set (set selected)
            is-blast? (= view :blast)
            input-type (if is-blast? "radio" "checkbox")

            ;; Group pipelines by cluster or group
            grouped (group-by #(or (:cluster %) (:group %) "Other") pipelines)

            ;; Build HTML
            html (str
                  ;; Pipelines grouped
                  (str/join ""
                            (for [[group-name group-pipelines] (sort-by first grouped)]
                              (str "<div class='planner-picker-group'>" group-name "</div>"
                                   (str/join ""
                                             (for [p (sort-by :name group-pipelines)]
                                               (str "<label class='planner-picker-item' data-name='" (str/lower-case (:name p)) "'>"
                                                    "<input type='" input-type "' name='planner-item' value='" (:name p) "'"
                                                    (when (contains? selected-set (:name p)) " checked") ">"
                                                    "<span>" (:name p) "</span>"
                                                    "</label>"))))))
                  ;; Datasources (only in blast mode)
                  (when is-blast?
                    (str "<div class='planner-picker-group'>Datasources</div>"
                         (str/join ""
                                   (for [ds (sort-by :name datasources)]
                                     (str "<label class='planner-picker-item' data-name='" (str/lower-case (:name ds)) "'>"
                                          "<input type='radio' name='planner-item' value='" (:name ds) "'"
                                          (when (contains? selected-set (:name ds)) " checked") ">"
                                          "<span>" (:name ds) "</span>"
                                          "<span class='ds-tag'>ds</span>"
                                          "</label>"))))))]
        (set! (.-innerHTML list-el) html)
        ;; Add event listeners
        (doseq [input (array-seq (.querySelectorAll list-el "input"))]
          (on! input "change" handle-item-change!))))))

(defn filter-items! [query]
  (when-let [list-el ($id "planner-picker-list")]
    (let [q (str/lower-case (str/trim query))]
      (doseq [item (array-seq (.querySelectorAll list-el ".planner-picker-item"))]
        (let [name (.getAttribute item "data-name")]
          (if (or (str/blank? q) (str/includes? name q))
            (remove-class! item "hidden")
            (add-class! item "hidden")))))))

(defn clear-selection! []
  (swap! state/planner assoc :selected [])
  (when-let [list-el ($id "planner-picker-list")]
    (doseq [input (array-seq (.querySelectorAll list-el "input"))]
      (set! (.-checked input) false)))
  (update-picker-label!)
  (update-count!)
  (render-graph!))

(defn- position-dropdown! [view-name]
  (when-let [dropdown ($id "planner-picker-dropdown")]
    (when-let [btn (.querySelector js/document (str ".planner-view-btn[data-view='" view-name "']"))]
      (let [btn-rect (.getBoundingClientRect btn)
            viewport-width js/window.innerWidth
            dropdown-width 280
            ;; Check if dropdown would overflow viewport on the right
            would-overflow? (> (+ (.-left btn-rect) dropdown-width) (- viewport-width 20))]
        ;; Reset both left and right first
        (set! (.-left (.-style dropdown)) "")
        (set! (.-right (.-style dropdown)) "")
        (if would-overflow?
          ;; Align to right edge of container
          (set! (.-right (.-style dropdown)) "0")
          ;; Align to left edge of button (relative to container)
          (let [container (.-parentElement btn)
                container-rect (.getBoundingClientRect container)
                left-pos (- (.-left btn-rect) (.-left container-rect))]
            (set! (.-left (.-style dropdown)) (str left-pos "px"))))))))

(defn set-view! [view-name]
  (let [view (keyword view-name)
        current-view (:view @state/planner)
        same-view? (= view current-view)
        picker-open? (:picker-open? @state/planner)]
    ;; If clicking same button and picker is open, close it
    (if (and same-view? picker-open?)
      (close-picker!)
      (do
        ;; Set view if different
        (when-not same-view?
          (swap! state/planner assoc :view view :selected [] :graphviz nil)
          ;; Update button states
          (doseq [btn (array-seq (.querySelectorAll js/document ".planner-view-btn"))]
            (if (= view-name (.getAttribute btn "data-view"))
              (add-class! btn "active")
              (remove-class! btn "active")))
          (update-count!)
          (render-graph!))
        ;; Always populate and show picker
        (populate-picker!)
        (position-dropdown! view-name)
        (when-let [dropdown ($id "planner-picker-dropdown")]
          (add-class! dropdown "show")
          (swap! state/planner assoc :picker-open? true)
          (when-let [filter-input ($id "planner-picker-filter")]
            (js/setTimeout #(.focus filter-input) 0)))))))

(defn copy-output! []
  (when-let [output ($id "planner-output")]
    (let [text (.-textContent output)]
      (when-not (str/blank? text)
        (when-let [btn (.querySelector js/document ".planner-copy-btn")]
          (-> (js/navigator.clipboard.writeText text)
              (.then (fn []
                       (add-class! btn "copied")
                       (set! (.-innerHTML btn) icon-check)
                       (js/setTimeout #(do (remove-class! btn "copied")
                                           (set! (.-innerHTML btn) icon-copy))
                                      1500)))
              (.catch #(js/console.error "Failed to copy:" %))))))))

(defn restore-from-hash! []
  (let [{:keys [view pipelines]} (hash/get-planner-state)]
    (when (or view pipelines)
      (when view
        (swap! state/planner assoc :view view)
        ;; Update button states
        (doseq [btn (array-seq (.querySelectorAll js/document ".planner-view-btn"))]
          (if (= (name view) (.getAttribute btn "data-view"))
            (add-class! btn "active")
            (remove-class! btn "active")))
        ;; Update hint
        (when-let [hint ($id "planner-hint")]
          (set! (.-textContent hint) (get hints view))))
      (when pipelines
        (swap! state/planner assoc :selected (vec pipelines)))
      (populate-picker!)
      (update-picker-label!)
      (update-count!)
      (render-graph!))))

(defn init! []
  (populate-picker!)
  (update-picker-label!)
  ;; Close picker when clicking outside
  (.addEventListener js/document "click"
                     (fn [e]
                       (when (:picker-open? @state/planner)
                         (let [picker (.closest (.-target e) ".planner-view-btns")]
                           (when-not picker
                             (close-picker!)))))))
