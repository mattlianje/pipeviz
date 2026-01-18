(ns pipeviz.server
  "Optional API server for serving graph data"
  (:require [pipeviz.graph :as graph]
            [clojure.data.json :as json]
            [org.httpkit.server :as http]
            [ring.util.response :as resp]
            [ring.middleware.params :refer [wrap-params]]
            [ring.middleware.keyword-params :refer [wrap-keyword-params]]
            [clojure.java.io :as io])
  (:gen-class))

;; Config storage - can be set via API or loaded from file
(defonce config (atom nil))

(defn json-response [data]
  (-> (resp/response (json/write-str data))
      (resp/content-type "application/json")))

(defn dot-response [dot-string]
  (-> (resp/response dot-string)
      (resp/content-type "text/vnd.graphviz")))

(defn load-config-file [path]
  (when (.exists (io/file path))
    (-> (slurp path)
        (json/read-str :key-fn keyword))))

;; API handlers
(defn handle-get-config [_req]
  (if-let [cfg @config]
    (json-response cfg)
    (-> (resp/response "No config loaded")
        (resp/status 404))))

(defn handle-set-config [req]
  (try
    (let [body (slurp (:body req))
          cfg (json/read-str body :key-fn keyword)
          {:keys [valid? errors]} (graph/validate-config cfg)]
      (if valid?
        (do
          (reset! config cfg)
          (json-response {:status "ok" :pipelines (count (:pipelines cfg))}))
        (-> (json-response {:status "error" :errors errors})
            (resp/status 400))))
    (catch Exception e
      (-> (json-response {:status "error" :message (.getMessage e)})
          (resp/status 400)))))

(defn handle-get-dot [_req]
  (if-let [cfg @config]
    (dot-response (graph/generate-dot cfg))
    (-> (resp/response "No config loaded")
        (resp/status 404))))

(defn handle-get-lineage [req]
  (if-let [cfg @config]
    (let [node (get-in req [:params :node])]
      (if node
        (json-response
         {:node node
          :upstream (vec (graph/upstream-of cfg node))
          :downstream (vec (graph/downstream-of cfg node))})
        (-> (json-response {:error "Missing 'node' parameter"})
            (resp/status 400))))
    (-> (resp/response "No config loaded")
        (resp/status 404))))

(defn handle-health [_req]
  (json-response {:status "ok"
                  :config-loaded (some? @config)}))

(defn handle-get-provenance [req]
  (if-let [cfg @config]
    (let [node (get-in req [:params :node])
          max-depth (when-let [d (get-in req [:params :depth])]
                      (try (Integer/parseInt d) (catch Exception _ nil)))]
      (if node
        (let [lineage-map (:attribute-map (graph/build-attribute-lineage-map cfg))
              provenance (graph/attribute-provenance lineage-map node
                                                     (when max-depth {:max-depth max-depth}))]
          (if provenance
            (json-response provenance)
            (-> (json-response {:error "Attribute not found" :attribute node})
                (resp/status 404))))
        (-> (json-response {:error "Missing 'node' parameter"
                            :usage "/api/provenance?node=datasource__attribute&depth=2"})
            (resp/status 400))))
    (-> (resp/response "No config loaded")
        (resp/status 404))))

(def api-docs
  {:name "Pipeviz API"
   :version "1.0"
   :description "REST API for pipeline lineage visualization"
   :endpoints
   [{:path "/health"
     :method "GET"
     :description "Health check"
     :response {:status "ok" :config-loaded "boolean"}}
    {:path "/api/config"
     :method "GET"
     :description "Get current loaded configuration"}
    {:path "/api/config"
     :method "POST"
     :description "Load a new configuration"
     :body "JSON pipeviz config"
     :response {:status "ok" :pipelines "count"}}
    {:path "/api/dot"
     :method "GET"
     :description "Get DOT graph representation"
     :content-type "text/vnd.graphviz"}
    {:path "/api/lineage"
     :method "GET"
     :description "Get pipeline/datasource lineage"
     :parameters {:node "Name of pipeline or datasource"
                  :depth "Optional. Max depth (1=direct, 2=two hops, etc.)"}
     :example "/api/lineage?node=my_pipeline&depth=2"}
    {:path "/api/provenance"
     :method "GET"
     :description "Get attribute-level provenance"
     :parameters {:node "Attribute ID (format: datasource__attribute)"
                  :depth "Optional. Max depth (1=direct, 2=two hops, etc.)"}
     :example "/api/provenance?node=users__email&depth=1"}
    {:path "/api"
     :method "GET"
     :description "This documentation"}]})

(defn handle-api-docs [_req]
  (json-response api-docs))

(defn handle-get-lineage [req]
  (if-let [cfg @config]
    (let [node (get-in req [:params :node])
          max-depth (when-let [d (get-in req [:params :depth])]
                      (try (Integer/parseInt d) (catch Exception _ nil)))]
      (if node
        (let [upstream (graph/upstream-of cfg node)
              downstream (graph/downstream-of cfg node)
              ;; Filter by depth if specified
              filter-by-depth (fn [items]
                                (if max-depth
                                  (take max-depth items)
                                  items))]
          (json-response
           {:node node
            :depth-applied max-depth
            :upstream (vec (filter-by-depth upstream))
            :downstream (vec (filter-by-depth downstream))}))
        (-> (json-response {:error "Missing 'node' parameter"
                            :usage "/api/lineage?node=pipeline_name&depth=2"})
            (resp/status 400))))
    (-> (resp/response "No config loaded")
        (resp/status 404))))

;; Routing
(defn router [req]
  (let [method (:request-method req)
        path (:uri req)]
    (cond
      (= path "/health")
      (handle-health req)

      (and (= method :get) (= path "/api"))
      (handle-api-docs req)

      (and (= method :get) (= path "/api/config"))
      (handle-get-config req)

      (and (= method :post) (= path "/api/config"))
      (handle-set-config req)

      (and (= method :get) (= path "/api/dot"))
      (handle-get-dot req)

      (and (= method :get) (= path "/api/lineage"))
      (handle-get-lineage req)

      (and (= method :get) (= path "/api/provenance"))
      (handle-get-provenance req)

      :else
      (-> (resp/response "Not found")
          (resp/status 404)))))

(def app
  (-> router
      wrap-keyword-params
      wrap-params))

(defn -main [& args]
  (let [port (Integer/parseInt (or (first args) "3000"))
        config-file (second args)]
    (when config-file
      (if-let [cfg (load-config-file config-file)]
        (do
          (reset! config cfg)
          (println "Loaded config from" config-file))
        (println "Warning: could not load" config-file)))
    (println (str "Starting server on http://localhost:" port))
    (println "Endpoints:")
    (println "  GET  /api              - API documentation (self-documenting)")
    (println "  GET  /health           - Health check")
    (println "  GET  /api/config       - Get current config")
    (println "  POST /api/config       - Set config (JSON body)")
    (println "  GET  /api/dot          - Get DOT graph string")
    (println "  GET  /api/lineage      - Get lineage (?node=NAME&depth=N)")
    (println "  GET  /api/provenance   - Get attribute provenance (?node=ds__attr&depth=N)")
    (http/run-server app {:port port})
    @(promise)))
