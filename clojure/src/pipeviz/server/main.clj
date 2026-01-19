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

(defn- handle-get-provenance [req]
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

(defn- handle-api-docs [_]
       (json-response
        {:endpoints
         [{:path "/health" :method "GET" :desc "Health check"}
          {:path "/api/config" :method "GET" :desc "Get current config"}
          {:path "/api/config" :method "POST" :desc "Set config (JSON body)"}
          {:path "/api/dot" :method "GET" :desc "Get DOT graph"}
          {:path "/api/lineage" :method "GET" :desc "Get lineage"
           :params "node (required), depth (optional)"}
          {:path "/api/provenance" :method "GET" :desc "Get attribute provenance"
           :params "node=datasource__attr, depth (optional)"}]}))

;; Router
(defn router [{:keys [request-method uri] :as req}]
      (case [request-method uri]
            [:get "/health"]         (handle-health req)
            [:get "/api"]            (handle-api-docs req)
            [:get "/api/config"]     (handle-get-config req)
            [:post "/api/config"]    (handle-set-config req)
            [:get "/api/dot"]        (handle-get-dot req)
            [:get "/api/lineage"]    (handle-get-lineage req)
            [:get "/api/provenance"] (handle-get-provenance req)
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
           (println "GET /api for endpoint docs")
           (http/run-server app {:port port})
           @(promise)))
