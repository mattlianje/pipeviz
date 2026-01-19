(ns pipeviz.server.main
    "Ring server for pipeviz API"
    (:require [clojure.data.json :as json]
              [clojure.java.io :as io]
              [org.httpkit.server :as http]
              [pipeviz.core.graph :as graph]
              [ring.middleware.keyword-params :refer [wrap-keyword-params]]
              [ring.middleware.params :refer [wrap-params]]
              [ring.util.response :as resp])
    (:gen-class))

(defonce config (atom nil))

;; Response helpers
(defn- json-response [data]
       (-> (resp/response (json/write-str data))
           (resp/content-type "application/json")))

(defn- dot-response [s]
       (-> (resp/response s)
           (resp/content-type "text/vnd.graphviz")))

(defn- error [status msg]
       (-> (json-response {:error msg}) (resp/status status)))

(defn- require-config [f]
       (if @config (f @config) (error 404 "No config loaded")))

(defn- parse-int [s]
       (try (Integer/parseInt s) (catch Exception _ nil)))

;; Config loading
(defn load-config-file [path]
      (when (.exists (io/file path))
            (-> (slurp path) (json/read-str :key-fn keyword))))

;; Handlers
(defn- handle-health [_]
       (json-response {:status "ok" :config-loaded (some? @config)}))

(defn- handle-get-config [_]
       (require-config json-response))

(defn- handle-set-config [req]
       (try
        (let [cfg (-> req :body slurp (json/read-str :key-fn keyword))
              {:keys [valid? errors]} (graph/validate-config cfg)]
             (if valid?
                 (do (reset! config cfg)
                     (json-response {:status "ok" :pipelines (count (:pipelines cfg))}))
                 (error 400 (first errors))))
        (catch Exception e
               (error 400 (.getMessage e)))))

(defn- handle-get-dot [_]
       (require-config #(dot-response (graph/generate-dot %))))

(defn- handle-get-lineage [req]
       (require-config
        (fn [cfg]
            (if-let [node (get-in req [:params :node])]
                    (let [depth (parse-int (get-in req [:params :depth]))]
                         (json-response
                          {:node node
                           :upstream (cond->> (graph/upstream-of cfg node) depth (take depth) true vec)
                           :downstream (cond->> (graph/downstream-of cfg node) depth (take depth) true vec)}))
                    (error 400 "Missing 'node' parameter")))))

(defn- handle-get-attribute-lineage [req]
       (require-config
        (fn [cfg]
            (if-let [node (get-in req [:params :node])]
                    (let [depth (parse-int (get-in req [:params :depth]))
                          lineage-map (:attribute-map (graph/build-attribute-lineage-map cfg))
                          opts (when depth {:max-depth depth})]
                         (if-let [prov (graph/attribute-provenance lineage-map node opts)]
                                 (json-response prov)
                                 (error 404 (str "Attribute not found: " node))))
                    (error 400 "Missing 'node' parameter")))))

(defn- handle-get-blast-radius [req]
       (require-config
        (fn [cfg]
            (if-let [node (get-in req [:params :node])]
                    (if-let [result (graph/generate-blast-radius-analysis cfg node)]
                            (json-response result)
                            (error 404 (str "Node not found: " node)))
                    (error 400 "Missing 'node' parameter")))))

(defn- handle-get-backfill [req]
       (require-config
        (fn [cfg]
            (if-let [nodes-param (get-in req [:params :nodes])]
                    (let [nodes (clojure.string/split nodes-param #",")]
                         (try
                          (json-response (graph/generate-backfill-analysis cfg nodes))
                          (catch Exception e
                                 (error 400 (.getMessage e)))))
                    (error 400 "Missing 'nodes' parameter (comma-separated pipeline names)")))))

(defn- handle-get-stats [_]
       (require-config #(json-response (graph/compute-stats %))))

(defn- handle-search [req]
       (require-config
        (fn [cfg]
            (if-let [q (get-in req [:params :q])]
                    (json-response {:query q :results (or (graph/search-nodes cfg q) [])})
                    (error 400 "Missing 'q' parameter")))))

(defn- handle-export-json [_]
       (require-config #(json-response (graph/generate-graph-export %))))

(defn- text-response [s content-type]
       (-> (resp/response s)
           (resp/content-type content-type)))

(defn- handle-export-mermaid [_]
       (require-config #(text-response (graph/generate-mermaid-export %) "text/plain")))

(defn- handle-get-attribute-dot [_]
       (require-config #(dot-response (graph/generate-attribute-dot %))))

(defn- html-response [s]
       (-> (resp/response s)
           (resp/content-type "text/html")))

(def ^:private swagger-ui-html
     "<!DOCTYPE html>
<html><head>
  <title>Pipeviz API</title>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui.css\">
</head><body>
  <div id=\"swagger-ui\"></div>
  <script src=\"https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js\"></script>
  <script>SwaggerUIBundle({url:'/api/openapi.json',dom_id:'#swagger-ui',deepLinking:true});</script>
</body></html>")

(defn- handle-swagger-ui [_]
       (html-response swagger-ui-html))

(defn- handle-openapi-spec [_]
       (if-let [resource (io/resource "openapi.json")]
               (json-response (json/read-str (slurp resource)))
               (error 404 "OpenAPI spec not found")))

;; Router
(defn router [{:keys [request-method uri] :as req}]
      (case [request-method uri]
            [:get "/health"]                (handle-health req)
            [:get "/api"]                   (handle-swagger-ui req)
            [:get "/api/openapi.json"]      (handle-openapi-spec req)
            [:get "/api/config"]            (handle-get-config req)
            [:post "/api/config"]           (handle-set-config req)
            [:get "/api/dot"]               (handle-get-dot req)
            [:get "/api/attribute-dot"]     (handle-get-attribute-dot req)
            [:get "/api/lineage"]           (handle-get-lineage req)
            [:get "/api/attribute-lineage"] (handle-get-attribute-lineage req)
            [:get "/api/blast-radius"]      (handle-get-blast-radius req)
            [:get "/api/backfill"]          (handle-get-backfill req)
            [:get "/api/stats"]             (handle-get-stats req)
            [:get "/api/search"]            (handle-search req)
            [:get "/api/export/json"]       (handle-export-json req)
            [:get "/api/export/mermaid"]    (handle-export-mermaid req)
            (error 404 "Not found")))

(def app
     (-> router
         wrap-keyword-params
         wrap-params))

(defn -main [& args]
      (let [port (or (some-> (first args) parse-int) 3000)
            config-file (second args)]
           (when config-file
                 (if-let [cfg (load-config-file config-file)]
                         (do (reset! config cfg)
                             (println "Loaded config from" config-file))
                         (println "Warning: could not load" config-file)))
           (println (str "Pipeviz server running on http://localhost:" port))
           (println (str "Swagger UI: http://localhost:" port "/api"))
           (http/run-server app {:port port})
           @(promise)))
