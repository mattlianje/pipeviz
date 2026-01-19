#!/usr/bin/env bb

;; Bundles the compiled JS into a single HTML file

(require '[clojure.string :as str]
         '[babashka.fs :as fs])

(def html-template (slurp "resources/public/index.html"))
(def js-content (slurp "dist/js/main.js"))

(def bundled-html
  (str/replace html-template
               #"<script src=\"js/main.js\"></script>"
               (str "<script>\n" js-content "\n</script>")))

(spit "dist/pipeviz.html" bundled-html)

(println "Bundled to dist/pipeviz.html")
(println (str "Size: " (Math/round (/ (count bundled-html) 1024.0)) " KB"))
